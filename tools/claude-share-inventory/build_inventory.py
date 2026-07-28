#!/usr/bin/env python3
"""Build a metadata-only inventory of publicly discoverable Claude share URLs.

Outputs contain full canonical URLs and aggregate metadata, never conversation bodies.
The collector does not brute-force UUIDs, bypass access controls, or submit URLs to
public scanning services.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import re
import statistics
import time
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pyarrow as pa
import pyarrow.parquet as pq
import requests

OUT = Path(os.environ.get("INVENTORY_OUT", "inventory-output"))
OUT.mkdir(parents=True, exist_ok=True)
RUN_AT = datetime.now(timezone.utc).isoformat()
UA = "ChimeraWerks-ClaudeShareInventory/1.0 (metadata-only public-index research)"
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": UA, "Accept": "application/json,text/plain,*/*"})

UUID_RE = re.compile(
    r"https?://(?:www\.)?claude\.ai/share/"
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)
WORD_RE = re.compile(r"[^\W_]+(?:['’\-][^\W_]+)*", re.UNICODE)
CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]")
CODE_RE = re.compile(r"```(?:[^\n]*)\n(.*?)```", re.DOTALL)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def canonical(value: str | None) -> tuple[str, str] | None:
    if not value:
        return None
    match = UUID_RE.search(str(value).strip())
    if not match:
        return None
    share_id = match.group(1).lower()
    return share_id, f"https://claude.ai/share/{share_id}"


def get(url: str, *, params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None, timeout: int = 120,
        attempts: int = 4) -> requests.Response:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            response = HTTP.get(url, params=params, headers=headers, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                delay = as_int(response.headers.get("Retry-After")) or 2 ** attempt
                time.sleep(min(delay, 30))
                continue
            return response
        except requests.RequestException as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"GET failed after {attempts} attempts: {url}: {last!r}")


def body_from_export(markdown: str) -> str:
    marker = "\n---\n"
    pos = markdown.find(marker)
    body = markdown[pos + len(marker):] if pos >= 0 else markdown
    body = re.sub(r"(?m)^---\s*$", "", body)
    body = re.sub(r"(?m)^## (?:Human|Assistant)\s*$", "", body)
    return body.strip()


def metrics(text: str) -> dict[str, Any]:
    raw = text.encode("utf-8")
    code_blocks = CODE_RE.findall(text)
    tokens = WORD_RE.findall(text)
    cjk_chars = len(CJK_RE.findall(text))
    chars = len(text)
    cjk_ratio = cjk_chars / max(chars, 1)
    if cjk_ratio >= 0.10:
        non_cjk_tokens = sum(1 for token in tokens if not CJK_RE.search(token))
        word_estimate = non_cjk_tokens + cjk_chars
        word_confidence = "low_cjk_character_equivalent"
    else:
        word_estimate = len(tokens)
        word_confidence = "high_unicode_token_sequence"
    token_estimate = round((chars - cjk_chars) / 4 + cjk_chars / 1.5)
    return {
        "text_bytes_utf8": len(raw),
        "character_count": chars,
        "nonspace_character_count": len(re.sub(r"\s+", "", text)),
        "unicode_word_token_count": len(tokens),
        "word_count_estimate": word_estimate,
        "word_count_method": "unicode_tokens_plus_cjk_character_equivalents",
        "word_count_confidence": word_confidence,
        "estimated_tokens_heuristic": token_estimate,
        "estimated_tokens_method": "non_cjk_chars_div_4_plus_cjk_chars_div_1_5",
        "cjk_character_count": cjk_chars,
        "code_block_count": len(code_blocks),
        "code_character_count": sum(len(block) for block in code_blocks),
        "content_sha256": hashlib.sha256(raw).hexdigest(),
    }


@dataclass
class Entry:
    share_id: str
    url: str
    sources: set[str] = field(default_factory=set)
    values: dict[str, Any] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)


entries: dict[str, Entry] = {}
status: dict[str, dict[str, Any]] = {}


def item_for(value: str | None) -> Entry | None:
    parsed = canonical(value)
    if not parsed:
        return None
    share_id, url = parsed
    if share_id not in entries:
        entries[share_id] = Entry(share_id=share_id, url=url)
    return entries[share_id]


def source_status(name: str, ok: bool, count: int = 0, error: str | None = None,
                  **details: Any) -> None:
    status[name] = {
        "ok": ok,
        "count": count,
        "error": error,
        "checked_at": now(),
        "details": details,
    }


def ingest_public_archive() -> None:
    name = "github_shared_claude_chats"
    url = "https://codeload.github.com/fuadmefleh/Shared-Claude-Chats/zip/refs/heads/main"
    try:
        response = get(url, timeout=240)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            names = zf.namelist()
            links_name = next(n for n in names if n.endswith("/link_lists/claudeaisharelinks.txt"))
            manifest_name = next(n for n in names if n.endswith("/claude_chats/.claude-share-export.json"))
            links = zf.read(links_name).decode("utf-8", errors="replace").splitlines()
            manifest = json.loads(zf.read(manifest_name).decode("utf-8"))
            exports = manifest.get("exports", {})

            md_by_basename: dict[str, list[str]] = defaultdict(list)
            for member in names:
                if "/claude_chats/" in member and member.endswith(".md"):
                    md_by_basename[Path(member).name].append(member)

            listed = 0
            for line in links:
                item = item_for(line)
                if item:
                    listed += 1
                    item.sources.add(name)
                    item.values["github_link_listed"] = True

            measured = 0
            missing_md = 0
            for share_id, meta in exports.items():
                item = item_for(f"https://claude.ai/share/{share_id}")
                if not item:
                    continue
                item.sources.add(name)
                item.values.update({
                    "title": meta.get("title"),
                    "archive_exported_at": meta.get("exported_at"),
                    "archive_message_count": as_int(meta.get("messages")),
                    "archive_filename": meta.get("file"),
                    "historically_public_export_succeeded": True,
                })
                candidates = md_by_basename.get(Path(str(meta.get("file", ""))).name, [])
                if not candidates:
                    missing_md += 1
                    continue
                member = sorted(candidates, key=len)[0]
                raw = zf.read(member)
                markdown = raw.decode("utf-8", errors="replace")
                text = body_from_export(markdown)
                item.values.update({f"archive_{k}": v for k, v in metrics(text).items()})
                item.values["archive_markdown_bytes"] = len(raw)
                created = re.search(r"(?m)^- Created:\s*(.+?)\s*$", markdown)
                if created:
                    item.values["conversation_created"] = created.group(1).strip()
                measured += 1

        source_status(name, True, len({canonical(x)[0] for x in links if canonical(x)}),
                      archive_zip_bytes=len(response.content), listed_lines=listed,
                      manifest_exports=len(exports), measured_markdown=measured,
                      missing_markdown=missing_md,
                      repository="https://github.com/fuadmefleh/Shared-Claude-Chats")
    except Exception as exc:
        source_status(name, False, error=repr(exc))


def ingest_sharechat() -> None:
    name = "huggingface_sharechat_derivative"
    endpoint = "https://datasets-server.huggingface.co/filter"
    offset = 0
    count = 0
    try:
        while True:
            params = {
                "dataset": "qdwqwd/sharechat-conversations",
                "config": "default",
                "split": "train",
                "where": '"platform"=\'claude\'',
                "offset": offset,
                "length": 100,
            }
            response = get(endpoint, params=params, timeout=240)
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("rows", [])
            if not rows:
                break
            for wrapper in rows:
                row = wrapper.get("row", wrapper)
                item = item_for(row.get("url"))
                if not item:
                    continue
                item.sources.add(name)
                messages = row.get("messages") if isinstance(row.get("messages"), list) else []
                roles = Counter()
                parts: list[str] = []
                for message in messages:
                    if not isinstance(message, dict):
                        continue
                    roles[str(message.get("role", "unknown"))] += 1
                    content = message.get("content", "")
                    parts.append(content if isinstance(content, str)
                                 else json.dumps(content, ensure_ascii=False, sort_keys=True))
                item.values.update({f"sharechat_{k}": v for k, v in metrics("\n\n".join(parts)).items()})
                item.values.update({
                    "sharechat_message_count": len(messages),
                    "sharechat_user_message_count": roles.get("user", 0),
                    "sharechat_assistant_message_count": roles.get("assistant", 0),
                    "sharechat_language": row.get("language"),
                    "sharechat_model": row.get("model"),
                    "sharechat_content_redacted": True,
                })
                count += 1
            offset += len(rows)
            if len(rows) < 100:
                break
            if offset > 5000:
                raise RuntimeError("Filtered result unexpectedly exceeded 5,000 rows")
        source_status(name, True, count,
                      dataset="https://huggingface.co/datasets/qdwqwd/sharechat-conversations",
                      policy="PII-redacted derivative; output retains aggregate metrics only")
    except Exception as exc:
        source_status(name, False, count, repr(exc))


def ingest_wayback() -> None:
    name = "wayback_cdx"
    endpoint = "https://web.archive.org/cdx/search/cdx"
    count = 0
    try:
        response = get(endpoint, params={
            "url": "claude.ai/share/*",
            "output": "json",
            "fl": "original,timestamp,statuscode,mimetype,digest,length",
            "filter": "statuscode:200",
            "collapse": "urlkey",
            "limit": 100000,
        }, timeout=240)
        response.raise_for_status()
        payload = response.json()
        if payload:
            header, *rows = payload
            for row in rows:
                obs = dict(zip(header, row))
                item = item_for(obs.get("original"))
                if not item:
                    continue
                item.sources.add(name)
                item.observations.append({"source": name, **obs})
                ts = str(obs.get("timestamp", ""))
                first = item.values.get("wayback_first_seen")
                last = item.values.get("wayback_last_seen")
                item.values["wayback_first_seen"] = min(first, ts) if first and ts else first or ts
                item.values["wayback_last_seen"] = max(last, ts) if last and ts else last or ts
                item.values["wayback_http_status"] = as_int(obs.get("statuscode"))
                item.values["wayback_mimetype"] = obs.get("mimetype")
                item.values["wayback_digest"] = obs.get("digest")
                item.values["wayback_compressed_record_bytes"] = as_int(obs.get("length"))
                count += 1
        source_status(name, True, count, query_url=response.url)
    except Exception as exc:
        source_status(name, False, count, repr(exc))


def query_cc(collection: dict[str, Any]) -> tuple[str, list[dict[str, Any]], str | None]:
    cid = str(collection.get("id", ""))
    api = collection.get("cdx-api")
    if not api:
        return cid, [], "missing cdx-api"
    try:
        response = get(str(api), params={
            "url": "claude.ai/share/",
            "matchType": "prefix",
            "output": "json",
            "filter": "status:200",
            "collapse": "urlkey",
        }, timeout=240, attempts=3)
        if response.status_code == 404:
            return cid, [], None
        response.raise_for_status()
        rows = []
        for line in response.text.splitlines():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return cid, rows, None
    except Exception as exc:
        return cid, [], repr(exc)


def ingest_commoncrawl() -> None:
    name = "common_crawl"
    observations = 0
    try:
        response = get("https://index.commoncrawl.org/collinfo.json", timeout=120)
        response.raise_for_status()
        collections = [c for c in response.json() if str(c.get("id", "")) >= "CC-MAIN-2023-01"]
        errors: dict[str, str] = {}
        hits = Counter()
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(query_cc, collection) for collection in collections]
            for future in as_completed(futures):
                cid, rows, error = future.result()
                if error:
                    errors[cid] = error
                    continue
                for obs in rows:
                    item = item_for(obs.get("url"))
                    if not item:
                        continue
                    item.sources.add(name)
                    item.observations.append({"source": name, "collection": cid, **obs})
                    ts = str(obs.get("timestamp", ""))
                    first = item.values.get("commoncrawl_first_seen")
                    last = item.values.get("commoncrawl_last_seen")
                    item.values["commoncrawl_first_seen"] = min(first, ts) if first and ts else first or ts
                    item.values["commoncrawl_last_seen"] = max(last, ts) if last and ts else last or ts
                    seen = set(str(item.values.get("commoncrawl_collections", "")).split(";")) - {""}
                    seen.add(cid)
                    item.values["commoncrawl_collections"] = ";".join(sorted(seen))
                    item.values["commoncrawl_latest_status"] = as_int(obs.get("status"))
                    item.values["commoncrawl_latest_mime"] = obs.get("mime")
                    item.values["commoncrawl_latest_digest"] = obs.get("digest")
                    item.values["commoncrawl_latest_compressed_record_bytes"] = as_int(obs.get("length"))
                    hits[cid] += 1
                    observations += 1
        source_status(name, True, observations, collections_queried=len(collections),
                      collections_with_hits=len(hits), collection_errors=errors)
    except Exception as exc:
        source_status(name, False, observations, repr(exc))


def ingest_urlscan() -> None:
    name = "urlscan_existing_records"
    count = 0
    try:
        response = get("https://urlscan.io/api/v1/search/",
                       params={"q": "domain:claude.ai", "size": 10000}, timeout=180)
        response.raise_for_status()
        results = response.json().get("results", [])
        for result in results:
            candidates = [result.get("page", {}).get("url"), result.get("task", {}).get("url")]
            for value in candidates:
                item = item_for(value)
                if not item:
                    continue
                item.sources.add(name)
                item.values["urlscan_seen_at"] = result.get("task", {}).get("time")
                item.values["urlscan_page_status"] = as_int(result.get("page", {}).get("status"))
                item.values["urlscan_result_url"] = result.get("result")
                count += 1
        source_status(name, True, count, records_examined=len(results),
                      policy="Existing records searched only; no URL was submitted")
    except Exception as exc:
        source_status(name, False, count, repr(exc))


def ingest_github_code_search() -> None:
    name = "github_public_code_search"
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        source_status(name, False, error="GITHUB_TOKEN unavailable")
        return
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.text-match+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    count = 0
    errors: list[str] = []
    for prefix in "0123456789abcdef":
        try:
            response = get("https://api.github.com/search/code", headers=headers, params={
                "q": f'"claude.ai/share/{prefix}"',
                "per_page": 100,
                "page": 1,
            }, timeout=120, attempts=3)
            if response.status_code in {403, 422}:
                errors.append(f"{prefix}: HTTP {response.status_code}")
                time.sleep(7)
                continue
            response.raise_for_status()
            for result in response.json().get("items", []):
                ref = result.get("html_url")
                repo = result.get("repository", {}).get("full_name")
                for match in result.get("text_matches", []) or []:
                    for found in UUID_RE.finditer(str(match.get("fragment", ""))):
                        item = item_for(found.group(0))
                        if not item:
                            continue
                        item.sources.add(name)
                        refs = set(str(item.values.get("github_code_references", "")).split(";")) - {""}
                        repos = set(str(item.values.get("github_repositories", "")).split(";")) - {""}
                        if ref:
                            refs.add(str(ref))
                        if repo:
                            repos.add(str(repo))
                        item.values["github_code_references"] = ";".join(sorted(refs))
                        item.values["github_repositories"] = ";".join(sorted(repos))
                        count += 1
        except Exception as exc:
            errors.append(f"{prefix}: {exc!r}")
        time.sleep(7)
    source_status(name, count > 0 or not errors, count, " | ".join(errors[:8]) or None,
                  prefix_buckets=16, pages_per_bucket=1)


def select_best(item: Entry) -> None:
    v = item.values
    if v.get("archive_text_bytes_utf8") is not None:
        prefix = "archive"
        v["best_measurement_basis"] = "public_github_archive_snapshot"
        v["measurement_confidence"] = "high_archive_snapshot"
    elif v.get("sharechat_text_bytes_utf8") is not None:
        prefix = "sharechat"
        v["best_measurement_basis"] = "pii_redacted_sharechat_derivative"
        v["measurement_confidence"] = "medium_redacted_derivative"
    else:
        prefix = None
        v["best_measurement_basis"] = "no_conversation_text_measurement"
        v["measurement_confidence"] = "url_or_crawl_metadata_only"
    if prefix:
        for target, suffix in [
            ("best_text_bytes", "text_bytes_utf8"),
            ("best_character_count", "character_count"),
            ("best_nonspace_character_count", "nonspace_character_count"),
            ("best_word_count_estimate", "word_count_estimate"),
            ("best_word_count_method", "word_count_method"),
            ("best_word_count_confidence", "word_count_confidence"),
            ("best_estimated_tokens", "estimated_tokens_heuristic"),
            ("best_estimated_tokens_method", "estimated_tokens_method"),
            ("best_code_block_count", "code_block_count"),
            ("best_code_character_count", "code_character_count"),
            ("best_content_sha256", "content_sha256"),
        ]:
            v[target] = v.get(f"{prefix}_{suffix}")
    for basis, key in [("github_archive", "archive_message_count"),
                       ("sharechat_derivative", "sharechat_message_count")]:
        if v.get(key) is not None:
            v["best_message_count"] = v.get(key)
            v["best_message_count_basis"] = basis
            break
    v["historically_observed_public"] = bool(
        v.get("historically_public_export_succeeded")
        or v.get("sharechat_message_count") is not None
        or v.get("wayback_http_status") == 200
        or v.get("commoncrawl_latest_status") == 200
    )
    v["current_validity"] = "not_live_checked"
    v["current_validity_reason"] = (
        "Historical export/crawl discovery does not prove the link remains public; "
        "bulk live validation was intentionally omitted."
    )


def rows() -> list[dict[str, Any]]:
    output = []
    for share_id in sorted(entries):
        item = entries[share_id]
        select_best(item)
        output.append({
            "url": item.url,
            "share_id": item.share_id,
            "discovery_sources": ";".join(sorted(item.sources)),
            "discovery_source_count": len(item.sources),
            **item.values,
            "inventory_generated_at": RUN_AT,
        })
    return output


def ordered_columns(data: list[dict[str, Any]]) -> list[str]:
    all_columns = sorted({key for row in data for key in row})
    first = [
        "url", "share_id", "title", "current_validity", "historically_observed_public",
        "best_text_bytes", "best_word_count_estimate", "best_character_count",
        "best_nonspace_character_count", "best_estimated_tokens", "best_message_count",
        "best_code_block_count", "best_code_character_count", "best_measurement_basis",
        "measurement_confidence", "conversation_created", "archive_exported_at",
        "wayback_first_seen", "wayback_last_seen", "commoncrawl_first_seen",
        "commoncrawl_last_seen", "discovery_sources", "discovery_source_count",
    ]
    return [x for x in first if x in all_columns] + [x for x in all_columns if x not in first]


def write_csv_gz(path: Path, data: list[dict[str, Any]]) -> None:
    cols = ordered_columns(data)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)


def write_parquet(path: Path, data: list[dict[str, Any]]) -> None:
    normalized = []
    for row in data:
        normalized.append({k: (json.dumps(v, ensure_ascii=False, sort_keys=True)
                               if isinstance(v, (dict, list, set)) else v)
                           for k, v in row.items()})
    pq.write_table(pa.Table.from_pylist(normalized), path, compression="zstd")


def write_observations(path: Path) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for item in entries.values():
            for observation in item.observations:
                handle.write(json.dumps({
                    "url": item.url,
                    "share_id": item.share_id,
                    **observation,
                }, ensure_ascii=False, sort_keys=True) + "\n")


def write_search_plan(path: Path) -> None:
    engines = {
        "Google": "https://www.google.com/search?q=",
        "Bing": "https://www.bing.com/search?q=",
        "Brave": "https://search.brave.com/search?q=",
        "DuckDuckGo": "https://duckduckgo.com/?q=",
        "Mojeek": "https://www.mojeek.com/search?q=",
        "Yandex": "https://yandex.com/search/?text=",
    }
    records = []
    for engine, base in engines.items():
        for first in "0123456789abcdef":
            for second in "0123456789abcdef":
                prefix = first + second
                query = f"site:https://claude.ai/share/{prefix}"
                records.append({
                    "engine": engine,
                    "uuid_prefix": prefix,
                    "query": query,
                    "search_url": base + quote_plus(query),
                    "execution_status": "query_plan_not_api_executed",
                })
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def make_summary(data: list[dict[str, Any]]) -> dict[str, Any]:
    source_counts = Counter()
    overlaps = Counter()
    for row in data:
        overlaps[int(row.get("discovery_source_count") or 0)] += 1
        for source in str(row.get("discovery_sources", "")).split(";"):
            if source:
                source_counts[source] += 1
    words = [as_int(row.get("best_word_count_estimate")) for row in data]
    words = [x for x in words if x is not None]
    sizes = [as_int(row.get("best_text_bytes")) for row in data]
    sizes = [x for x in sizes if x is not None]
    f1, f2 = overlaps.get(1, 0), overlaps.get(2, 0)
    chao1 = len(data) + (f1 * f1 / (2 * f2)) if f2 else None

    def stats(values: list[int]) -> dict[str, Any]:
        return {
            "count": len(values),
            "min": min(values) if values else None,
            "median": statistics.median(values) if values else None,
            "mean": statistics.mean(values) if values else None,
            "max": max(values) if values else None,
            "sum": sum(values) if values else None,
        }

    return {
        "generated_at": RUN_AT,
        "unique_urls": len(data),
        "historically_observed_public": sum(bool(r.get("historically_observed_public")) for r in data),
        "rows_with_text_measurement": len(sizes),
        "rows_with_word_measurement": len(words),
        "source_unique_url_counts": dict(sorted(source_counts.items())),
        "source_overlap_histogram": {str(k): v for k, v in sorted(overlaps.items())},
        "chao1_discoverable_population_lower_bound": chao1,
        "word_count_estimate": stats(words),
        "text_bytes_utf8": stats(sizes),
        "source_status": status,
        "limitations": [
            "Maximum-recall public-discovery snapshot, not every share ever created.",
            "Current live validity was not bulk checked.",
            "Search-engine API credentials were unavailable; a 1,536-query plan is included.",
            "Archive record lengths are compressed WARC/CDX records, not conversation text sizes.",
            "CJK-heavy word counts are character-equivalent estimates and marked low-confidence.",
            "ShareChat measurements use a PII-redacted derivative and can differ from originals.",
        ],
    }


def write_methodology(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(f"""# Claude Share URL Inventory Methodology

Generated: `{RUN_AT}`

## Scope

This is a metadata-only, maximum-recall inventory of publicly discoverable
`https://claude.ai/share/<uuid>` URLs. Every inventory row includes the full canonical
URL. The collector does not brute-force share IDs, bypass access controls, or submit
URLs to public scanning services.

## Executed sources

- Public `fuadmefleh/Shared-Claude-Chats` URL list, successful-export manifest, and
  Markdown snapshots. Conversation bodies are measured in memory and not emitted.
- PII-redacted `qdwqwd/sharechat-conversations` derivative through Hugging Face's
  filtered Dataset Viewer API.
- Internet Archive Wayback CDX.
- Every listed Common Crawl collection from 2023 onward.
- Existing urlscan.io records only; no scan submission.
- GitHub public code search, partitioned by the first hexadecimal UUID character.

A separate plan contains 256 UUID-prefix searches for each of Google, Bing, Brave,
DuckDuckGo, Mojeek, and Yandex. Those consumer-engine queries were not falsely marked
as executed because reliable automated APIs and credentials were unavailable.

## Size and length definitions

- `best_text_bytes`: UTF-8 bytes of extracted message-body text.
- `best_character_count`: Unicode code points in extracted text.
- `best_word_count_estimate`: Unicode alphanumeric token sequences; CJK-heavy text
  uses character-equivalents and is explicitly marked low-confidence.
- `best_estimated_tokens`: heuristic only, not an Anthropic billing token count.
- `archive_markdown_bytes`: size of the archived Markdown file including metadata.
- `*_compressed_record_bytes`: compressed crawl-record size, never substituted for
  text-body size.

## Validity

`current_validity=not_live_checked` is deliberate. A historical export or crawl proves
that a URL was observed, but not that the owner has kept the link public.

## Summary

```json
{json.dumps(summary, ensure_ascii=False, indent=2)}
```
""", encoding="utf-8")


def main() -> int:
    ingest_public_archive()
    ingest_sharechat()
    ingest_wayback()
    ingest_commoncrawl()
    ingest_urlscan()
    ingest_github_code_search()

    data = rows()
    summary = make_summary(data)

    write_csv_gz(OUT / "claude-share-inventory.csv.gz", data)
    write_parquet(OUT / "claude-share-inventory.parquet", data)
    (OUT / "claude-share-urls.txt").write_text(
        "\n".join(row["url"] for row in data) + "\n", encoding="utf-8")
    write_observations(OUT / "source-observations.ndjson.gz")
    write_search_plan(OUT / "search-engine-query-plan.csv")
    (OUT / "coverage-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT / "source-status.json").write_text(
        json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    write_methodology(OUT / "methodology.md", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
