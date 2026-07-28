#!/usr/bin/env python3
"""Build a metadata-only inventory of publicly discoverable ChatGPT share URLs.

Outputs retain full canonical URLs and aggregate measurements, never conversation
bodies. The collector does not brute-force IDs, bypass access controls, bulk-live-
validate ChatGPT, or submit URLs to public scanning services.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import html
import io
import json
import math
import os
import re
import statistics
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests

OUT = Path(os.environ.get("INVENTORY_OUT", "inventory-output"))
OUT.mkdir(parents=True, exist_ok=True)
RUN_AT = datetime.now(timezone.utc).isoformat()
UA = "ChimeraWerks-ChatGPTShareInventory/1.0 (metadata-only public-index research)"
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": UA, "Accept": "application/json,text/plain,text/event-stream,*/*"})

UUID_PATTERN = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
SHARE_RE = re.compile(
    rf"https?://(?:www\.)?(?:chatgpt\.com|chat\.openai\.com)/share/({UUID_PATTERN})",
    re.IGNORECASE,
)
BARE_UUID_RE = re.compile(rf"^\s*({UUID_PATTERN})\s*$", re.IGNORECASE)
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


def canonical(value: Any) -> tuple[str, str] | None:
    if value is None:
        return None
    text = str(value).strip()
    match = SHARE_RE.search(text) or BARE_UUID_RE.match(text)
    if not match:
        return None
    share_id = match.group(1).lower()
    return share_id, f"https://chatgpt.com/share/{share_id}"


def get(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 120,
    attempts: int = 4,
) -> requests.Response:
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


def extract_text(value: Any) -> str:
    """Extract readable text from heterogeneous message/content structures."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        return "\n".join(part for x in value if (part := extract_text(x)))
    if isinstance(value, dict):
        preferred = []
        for key in ("text", "value", "content", "parts", "result", "caption", "title"):
            if key in value:
                part = extract_text(value.get(key))
                if part:
                    preferred.append(part)
        if preferred:
            return "\n".join(preferred)
        return "\n".join(
            part
            for k, v in value.items()
            if k not in {"metadata", "citations", "content_references"}
            and (part := extract_text(v))
        )
    return str(value)


def extract_message_texts(messages: Any) -> tuple[list[str], Counter, set[str]]:
    parts: list[str] = []
    roles: Counter = Counter()
    models: set[str] = set()
    if not isinstance(messages, list):
        return parts, roles, models
    for message in messages:
        if not isinstance(message, dict):
            text = extract_text(message)
            if text:
                parts.append(text)
            continue
        role = message.get("role")
        if isinstance(role, dict):
            role = role.get("role") or role.get("name")
        role_text = str(role or message.get("from") or "unknown")
        roles[role_text] += 1
        meta = message.get("metadata")
        if isinstance(meta, dict):
            model = meta.get("$modelId") or meta.get("model_slug") or meta.get("model")
            if model:
                models.add(str(model))
        content = message.get("content")
        if content is None:
            content = message.get("value")
        text = extract_text(content)
        if text:
            parts.append(text)
    return parts, roles, models


@dataclass
class Entry:
    share_id: str
    url: str
    sources: set[str] = field(default_factory=set)
    values: dict[str, Any] = field(default_factory=dict)
    observations: list[dict[str, Any]] = field(default_factory=list)


entries: dict[str, Entry] = {}
status: dict[str, dict[str, Any]] = {}


def item_for(value: Any) -> Entry | None:
    parsed = canonical(value)
    if not parsed:
        return None
    share_id, url = parsed
    if share_id not in entries:
        entries[share_id] = Entry(share_id=share_id, url=url)
    return entries[share_id]


def source_status(
    name: str, ok: bool, count: int = 0, error: str | None = None, **details: Any
) -> None:
    status[name] = {
        "ok": ok,
        "count": count,
        "error": error,
        "checked_at": now(),
        "details": details,
    }


def add_reference(item: Entry, prefix: str, reference: str | None) -> None:
    if not reference:
        return
    values = set(str(item.values.get(prefix, "")).split(";")) - {""}
    values.add(str(reference))
    item.values[prefix] = ";".join(sorted(values))


def hf_parquet_files(dataset: str, config: str | None = None, split: str | None = None) -> list[str]:
    response = get(
        "https://datasets-server.huggingface.co/parquet",
        params={"dataset": dataset},
        timeout=240,
        attempts=4,
    )
    response.raise_for_status()
    files = []
    for entry in response.json().get("parquet_files", []):
        if config is not None and entry.get("config") != config:
            continue
        if split is not None and entry.get("split") != split:
            continue
        url = entry.get("url")
        if url:
            files.append(str(url))
    return files


def duckdb_batches(
    urls: list[str], select_sql: str, batch_size: int = 256
) -> Iterable[list[dict[str, Any]]]:
    if not urls:
        return
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        conn.execute("SET enable_http_metadata_cache=true")
        conn.execute("SET threads=4")
        escaped = ",".join("'" + u.replace("'", "''") + "'" for u in urls)
        query = select_sql.format(files=f"[{escaped}]")
        reader = conn.execute(query).fetch_record_batch(rows_per_batch=batch_size)
        for batch in reader:
            yield batch.to_pylist()
    finally:
        conn.close()


def ingest_sharechat() -> None:
    """Use the PII-redacted ShareChat derivative for full URLs and measurements."""
    name = "huggingface_sharechat_derivative"
    count = 0
    try:
        urls = hf_parquet_files("qdwqwd/sharechat-conversations", "default", "train")
        if not urls:
            raise RuntimeError("No Parquet files reported")
        sql = """
            SELECT url, messages, language, model, platform
            FROM read_parquet({files}, union_by_name=true)
            WHERE lower(platform) = 'chatgpt'
        """
        for batch in duckdb_batches(urls, sql, batch_size=128):
            for row in batch:
                item = item_for(row.get("url"))
                if not item:
                    continue
                item.sources.add(name)
                parts, roles, models = extract_message_texts(row.get("messages"))
                joined = "\n\n".join(parts)
                if joined:
                    item.values.update(
                        {f"sharechat_{k}": v for k, v in metrics(joined).items()}
                    )
                item.values.update(
                    {
                        "sharechat_message_count": sum(roles.values())
                        or (
                            len(row.get("messages"))
                            if isinstance(row.get("messages"), list)
                            else None
                        ),
                        "sharechat_user_message_count": roles.get("user", 0),
                        "sharechat_assistant_message_count": roles.get("assistant", 0),
                        "sharechat_language": row.get("language"),
                        "sharechat_model": row.get("model")
                        or (";".join(sorted(models)) if models else None),
                        "sharechat_content_redacted": True,
                    }
                )
                count += 1
        source_status(
            name,
            True,
            count,
            dataset="https://huggingface.co/datasets/qdwqwd/sharechat-conversations",
            parquet_files=len(urls),
            policy="PII-redacted derivative; output retains aggregate metrics only",
        )
    except Exception as exc:
        source_status(name, False, count, repr(exc))


def ingest_sharegpt_x() -> None:
    """Use UUID IDs in ShareGPT-X's Simple config to reconstruct canonical share URLs."""
    name = "huggingface_sharegpt_x"
    count = 0
    try:
        urls = hf_parquet_files("DSULT-Core/ShareGPT-X", "Simple", "full")
        if not urls:
            raise RuntimeError("No Simple/full Parquet files reported")
        sql = "SELECT id, conversations FROM read_parquet({files}, union_by_name=true)"
        for batch in duckdb_batches(urls, sql, batch_size=64):
            for row in batch:
                share_id = row.get("id")
                item = item_for(share_id)
                if not item:
                    continue
                item.sources.add(name)
                parts, roles, models = extract_message_texts(row.get("conversations"))
                joined = "\n\n".join(parts)
                if joined:
                    item.values.update(
                        {f"sharegptx_{k}": v for k, v in metrics(joined).items()}
                    )
                created_values = []
                for message in row.get("conversations") or []:
                    if isinstance(message, dict) and message.get("created") is not None:
                        created_values.append(message.get("created"))
                item.values.update(
                    {
                        "sharegptx_message_count": sum(roles.values())
                        or (
                            len(row.get("conversations"))
                            if isinstance(row.get("conversations"), list)
                            else None
                        ),
                        "sharegptx_user_message_count": roles.get("user", 0)
                        + roles.get("human", 0),
                        "sharegptx_assistant_message_count": roles.get("assistant", 0)
                        + roles.get("gpt", 0),
                        "sharegptx_models": ";".join(sorted(models)) if models else None,
                        "sharegptx_first_message_time": min(created_values)
                        if created_values
                        else None,
                        "sharegptx_last_message_time": max(created_values)
                        if created_values
                        else None,
                        "sharegptx_url_reconstruction": "canonical_url_reconstructed_from_uuid_id",
                    }
                )
                count += 1
        source_status(
            name,
            True,
            count,
            dataset="https://huggingface.co/datasets/DSULT-Core/ShareGPT-X",
            config="Simple",
            split="full",
            parquet_files=len(urls),
            policy="Conversation bodies processed transiently; output retains aggregates only",
        )
    except Exception as exc:
        source_status(name, False, count, repr(exc))


def parse_wayback_json(response: requests.Response) -> list[dict[str, Any]]:
    payload = response.json()
    if not payload:
        return []
    header, *rows = payload
    return [dict(zip(header, row)) for row in rows]


def ingest_wayback() -> None:
    name = "wayback_cdx"
    count = 0
    errors: list[str] = []
    for host in ("chatgpt.com/share/", "chat.openai.com/share/"):
        try:
            response = get(
                "https://web.archive.org/cdx/search/cdx",
                params={
                    "url": host,
                    "matchType": "prefix",
                    "output": "json",
                    "fl": "original,timestamp,statuscode,mimetype,digest,length",
                    "filter": "statuscode:200",
                    "collapse": "urlkey",
                    "limit": 250000,
                },
                timeout=600,
                attempts=4,
            )
            response.raise_for_status()
            for obs in parse_wayback_json(response):
                item = item_for(obs.get("original"))
                if not item:
                    continue
                item.sources.add(name)
                item.observations.append({"source": name, "queried_host": host, **obs})
                timestamp = str(obs.get("timestamp", ""))
                first = item.values.get("wayback_first_seen")
                last = item.values.get("wayback_last_seen")
                item.values["wayback_first_seen"] = (
                    min(first, timestamp) if first and timestamp else first or timestamp
                )
                item.values["wayback_last_seen"] = (
                    max(last, timestamp) if last and timestamp else last or timestamp
                )
                item.values["wayback_http_status"] = as_int(obs.get("statuscode"))
                item.values["wayback_mimetype"] = obs.get("mimetype")
                item.values["wayback_digest"] = obs.get("digest")
                item.values["wayback_compressed_record_bytes"] = as_int(obs.get("length"))
                if str(obs.get("original", "")).lower().startswith("http"):
                    item.values["legacy_host_observed"] = "chat.openai.com" in str(
                        obs.get("original", "")
                    ).lower()
                count += 1
        except Exception as exc:
            errors.append(f"{host}: {exc!r}")
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        hosts_queried=2,
    )


def query_cc(collection: dict[str, Any], host: str) -> tuple[str, str, list[dict[str, Any]], str | None]:
    cid = str(collection.get("id", "unknown"))
    api = collection.get("cdx-api")
    if not api:
        return cid, host, [], "missing cdx-api"
    try:
        response = get(
            str(api),
            params={
                "url": host,
                "matchType": "prefix",
                "output": "json",
                "filter": "status:200",
                "collapse": "urlkey",
            },
            timeout=150,
            attempts=2,
        )
        if response.status_code == 404:
            return cid, host, [], None
        response.raise_for_status()
        rows = []
        for line in response.text.splitlines():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return cid, host, rows, None
    except Exception as exc:
        return cid, host, [], repr(exc)


def ingest_commoncrawl() -> None:
    name = "common_crawl"
    observations = 0
    try:
        response = get("https://index.commoncrawl.org/collinfo.json", timeout=120)
        response.raise_for_status()
        collections = [
            c for c in response.json() if str(c.get("id", "")) >= "CC-MAIN-2023-01"
        ]
        hosts = ("chatgpt.com/share/", "chat.openai.com/share/")
        errors: dict[str, str] = {}
        hits = Counter()
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(query_cc, collection, host)
                for collection in collections
                for host in hosts
            ]
            for future in as_completed(futures):
                cid, host, rows, error = future.result()
                if error:
                    errors[f"{cid}:{host}"] = error
                    continue
                for obs in rows:
                    item = item_for(obs.get("url"))
                    if not item:
                        continue
                    item.sources.add(name)
                    item.observations.append(
                        {"source": name, "collection": cid, "queried_host": host, **obs}
                    )
                    timestamp = str(obs.get("timestamp", ""))
                    first = item.values.get("commoncrawl_first_seen")
                    last = item.values.get("commoncrawl_last_seen")
                    item.values["commoncrawl_first_seen"] = (
                        min(first, timestamp) if first and timestamp else first or timestamp
                    )
                    item.values["commoncrawl_last_seen"] = (
                        max(last, timestamp) if last and timestamp else last or timestamp
                    )
                    seen = set(
                        str(item.values.get("commoncrawl_collections", "")).split(";")
                    ) - {""}
                    seen.add(cid)
                    item.values["commoncrawl_collections"] = ";".join(sorted(seen))
                    item.values["commoncrawl_latest_status"] = as_int(obs.get("status"))
                    item.values["commoncrawl_latest_mime"] = obs.get("mime")
                    item.values["commoncrawl_latest_digest"] = obs.get("digest")
                    item.values["commoncrawl_latest_compressed_record_bytes"] = as_int(
                        obs.get("length")
                    )
                    hits[f"{cid}:{host}"] += 1
                    observations += 1
        source_status(
            name,
            True,
            observations,
            collections_queried=len(collections),
            host_collection_queries=len(collections) * len(hosts),
            queries_with_hits=len(hits),
            query_errors=errors,
        )
    except Exception as exc:
        source_status(name, False, observations, repr(exc))


def ingest_urlscan() -> None:
    name = "urlscan_existing_records"
    count = 0
    examined = 0
    errors: list[str] = []
    for query in ("domain:chatgpt.com AND filename:share", "domain:chat.openai.com AND filename:share"):
        try:
            response = get(
                "https://urlscan.io/api/v1/search/",
                params={"q": query, "size": 10000},
                timeout=180,
            )
            response.raise_for_status()
            results = response.json().get("results", [])
            examined += len(results)
            for result in results:
                candidates = [
                    result.get("page", {}).get("url"),
                    result.get("task", {}).get("url"),
                ]
                for value in candidates:
                    item = item_for(value)
                    if not item:
                        continue
                    item.sources.add(name)
                    item.values["urlscan_seen_at"] = result.get("task", {}).get("time")
                    item.values["urlscan_page_status"] = as_int(
                        result.get("page", {}).get("status")
                    )
                    item.values["urlscan_result_url"] = result.get("result")
                    count += 1
        except Exception as exc:
            errors.append(f"{query}: {exc!r}")
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        records_examined=examined,
        policy="Existing records searched only; no URL was submitted",
    )


def extract_urls_from_fragment(fragment: str) -> list[str]:
    return [match.group(0) for match in SHARE_RE.finditer(fragment or "")]


def github_search_bucket(prefix: str, headers: dict[str, str], page: int = 1) -> tuple[list[dict[str, Any]], int | None, str | None]:
    queries = [
        f'"chatgpt.com/share/{prefix}"',
        f'"chat.openai.com/share/{prefix}"',
    ]
    combined: list[dict[str, Any]] = []
    total_count = 0
    errors = []
    for query in queries:
        try:
            response = get(
                "https://api.github.com/search/code",
                headers=headers,
                params={"q": query, "per_page": 100, "page": page},
                timeout=120,
                attempts=3,
            )
            if response.status_code in {403, 422}:
                errors.append(f"{query}: HTTP {response.status_code}")
                continue
            response.raise_for_status()
            payload = response.json()
            total_count += as_int(payload.get("total_count")) or 0
            combined.extend(payload.get("items", []))
        except Exception as exc:
            errors.append(f"{query}: {exc!r}")
    return combined, total_count, " | ".join(errors) or None


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
    queue = list("0123456789abcdef")
    queried = 0
    max_queries = 80
    while queue and queried < max_queries:
        prefix = queue.pop(0)
        queried += 1
        results, total, error = github_search_bucket(prefix, headers)
        if error:
            errors.append(f"{prefix}: {error}")
        for result in results:
            ref = result.get("html_url")
            repo = result.get("repository", {}).get("full_name")
            fragments = [
                match.get("fragment", "") for match in result.get("text_matches", []) or []
            ]
            for found_url in extract_urls_from_fragment("\n".join(fragments)):
                item = item_for(found_url)
                if not item:
                    continue
                item.sources.add(name)
                add_reference(item, "github_code_references", ref)
                add_reference(item, "github_repositories", repo)
                count += 1
        if (total or 0) >= 800 and len(prefix) < 2 and queried + 16 <= max_queries:
            queue.extend(prefix + char for char in "0123456789abcdef")
        time.sleep(7)
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors[:12]) or None,
        adaptive_prefix_queries=queried,
        max_queries=max_queries,
        pages_per_bucket=1,
    )


def parse_sse_events(text: str) -> Iterable[tuple[str, Any]]:
    event = "message"
    data_lines: list[str] = []
    for line in text.splitlines() + [""]:
        if not line:
            if data_lines:
                raw = "\n".join(data_lines)
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    data = raw
                yield event, data
            event = "message"
            data_lines = []
        elif line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())


def ingest_sourcegraph() -> None:
    name = "sourcegraph_public_code_search"
    count = 0
    errors: list[str] = []
    seen_matches = 0
    queries = [
        '"chatgpt.com/share/" type:file count:all archived:yes fork:yes',
        '"chat.openai.com/share/" type:file count:all archived:yes fork:yes',
    ]
    for query in queries:
        try:
            response = get(
                "https://sourcegraph.com/.api/search/stream",
                params={"q": query, "v": "V3"},
                timeout=600,
                attempts=3,
            )
            response.raise_for_status()
            for event, data in parse_sse_events(response.text):
                if event != "matches" or not isinstance(data, list):
                    continue
                for match in data:
                    if not isinstance(match, dict):
                        continue
                    seen_matches += 1
                    repo = match.get("repository") or match.get("repositoryID")
                    path = match.get("path")
                    ref = (
                        f"https://sourcegraph.com/{repo}/-/blob/{path}"
                        if repo and path
                        else None
                    )
                    chunks = [json.dumps(match, ensure_ascii=False)]
                    for found_url in extract_urls_from_fragment("\n".join(chunks)):
                        item = item_for(found_url)
                        if not item:
                            continue
                        item.sources.add(name)
                        add_reference(item, "sourcegraph_references", ref)
                        add_reference(item, "sourcegraph_repositories", str(repo) if repo else None)
                        count += 1
        except Exception as exc:
            errors.append(f"{query}: {exc!r}")
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        result_objects_examined=seen_matches,
        policy="Unauthenticated public-code search only",
    )


def ingest_grep_app() -> None:
    name = "grep_app_public_code_search"
    count = 0
    errors: list[str] = []
    hits_examined = 0
    for query in ("chatgpt.com/share/", "chat.openai.com/share/"):
        for page in range(1, 11):
            try:
                response = get(
                    "https://grep.app/api/search",
                    params={"q": query, "page": page},
                    timeout=120,
                    attempts=3,
                )
                if response.status_code == 429:
                    errors.append(f"{query} page {page}: HTTP 429")
                    break
                response.raise_for_status()
                payload = response.json()
                hits = payload.get("hits", {}).get("hits", []) or []
                if not hits:
                    break
                for hit in hits:
                    hits_examined += 1
                    repo = hit.get("repo", {}).get("raw") if isinstance(hit.get("repo"), dict) else hit.get("repo")
                    path = hit.get("path", {}).get("raw") if isinstance(hit.get("path"), dict) else hit.get("path")
                    content = hit.get("content", {})
                    snippet = content.get("snippet", "") if isinstance(content, dict) else str(content)
                    snippet = html.unescape(re.sub(r"<[^>]+>", "", snippet))
                    ref = f"https://github.com/{repo}/blob/HEAD/{path}" if repo and path else None
                    for found_url in extract_urls_from_fragment(snippet):
                        item = item_for(found_url)
                        if not item:
                            continue
                        item.sources.add(name)
                        add_reference(item, "grep_app_references", ref)
                        add_reference(item, "grep_app_repositories", str(repo) if repo else None)
                        count += 1
                if len(hits) < 10:
                    break
            except Exception as exc:
                errors.append(f"{query} page {page}: {exc!r}")
                break
            time.sleep(1)
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        hits_examined=hits_examined,
        pages_per_query_limit=10,
    )


def ingest_marginalia() -> None:
    name = "marginalia_public_search_api"
    count = 0
    errors: list[str] = []
    results_examined = 0
    queries = [
        '"chatgpt.com/share/"',
        '"chat.openai.com/share/"',
        "site:chatgpt.com/share",
        "site:chat.openai.com/share",
    ]
    for query in queries:
        try:
            response = get(
                "https://api2.marginalia-search.com/search",
                params={"query": query, "count": 100, "index": 0},
                headers={"Authorization": "Bearer public"},
                timeout=180,
                attempts=3,
            )
            response.raise_for_status()
            payload = response.json()
            results = payload.get("results", payload if isinstance(payload, list) else [])
            for result in results or []:
                results_examined += 1
                url = result.get("url") if isinstance(result, dict) else None
                item = item_for(url)
                if not item:
                    continue
                item.sources.add(name)
                item.values["marginalia_title"] = result.get("title")
                item.values["marginalia_description"] = result.get("description")
                count += 1
        except Exception as exc:
            errors.append(f"{query}: {exc!r}")
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        results_examined=results_examined,
        api_key="public",
    )


def ingest_mwmbl() -> None:
    name = "mwmbl_public_search_api"
    count = 0
    errors: list[str] = []
    results_examined = 0
    for query in ('"chatgpt.com/share/"', '"chat.openai.com/share/"'):
        try:
            response = get(
                "https://mwmbl.org/api/v2/search/",
                params={"q": query},
                timeout=180,
                attempts=3,
            )
            response.raise_for_status()
            payload = response.json()
            results = payload.get("results", payload if isinstance(payload, list) else [])
            for result in results or []:
                results_examined += 1
                url = result.get("url") if isinstance(result, dict) else None
                item = item_for(url)
                if not item:
                    continue
                item.sources.add(name)
                item.values["mwmbl_title"] = result.get("title")
                item.values["mwmbl_extract"] = result.get("extract") or result.get("snippet")
                count += 1
        except Exception as exc:
            errors.append(f"{query}: {exc!r}")
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        results_examined=results_examined,
    )


def ingest_hackernews() -> None:
    name = "hackernews_algolia"
    count = 0
    hits_examined = 0
    errors: list[str] = []
    for query in ("chatgpt.com/share", "chat.openai.com/share"):
        for page in range(0, 20):
            try:
                response = get(
                    "https://hn.algolia.com/api/v1/search_by_date",
                    params={"query": query, "tags": "story,comment", "hitsPerPage": 100, "page": page},
                    timeout=120,
                )
                response.raise_for_status()
                payload = response.json()
                hits = payload.get("hits", [])
                if not hits:
                    break
                for hit in hits:
                    hits_examined += 1
                    text = "\n".join(str(hit.get(key, "")) for key in ("url", "story_url", "comment_text", "title", "story_title"))
                    ref_id = hit.get("objectID")
                    ref = f"https://news.ycombinator.com/item?id={ref_id}" if ref_id else None
                    for found_url in extract_urls_from_fragment(html.unescape(text)):
                        item = item_for(found_url)
                        if not item:
                            continue
                        item.sources.add(name)
                        add_reference(item, "hackernews_references", ref)
                        count += 1
                if page + 1 >= as_int(payload.get("nbPages")) or 0:
                    break
            except Exception as exc:
                errors.append(f"{query} page {page}: {exc!r}")
                break
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        hits_examined=hits_examined,
    )


def ingest_reddit() -> None:
    name = "reddit_public_search"
    count = 0
    children_examined = 0
    errors: list[str] = []
    for query in ("chatgpt.com/share", "chat.openai.com/share"):
        after = None
        for _ in range(10):
            try:
                params = {"q": query, "restrict_sr": "false", "sort": "new", "limit": 100, "raw_json": 1}
                if after:
                    params["after"] = after
                response = get(
                    "https://www.reddit.com/search.json",
                    params=params,
                    timeout=120,
                    attempts=2,
                )
                response.raise_for_status()
                payload = response.json().get("data", {})
                children = payload.get("children", [])
                for child in children:
                    data = child.get("data", {}) if isinstance(child, dict) else {}
                    children_examined += 1
                    text = "\n".join(str(data.get(key, "")) for key in ("url", "selftext", "title"))
                    permalink = data.get("permalink")
                    ref = f"https://www.reddit.com{permalink}" if permalink else None
                    for found_url in extract_urls_from_fragment(text):
                        item = item_for(found_url)
                        if not item:
                            continue
                        item.sources.add(name)
                        add_reference(item, "reddit_references", ref)
                        count += 1
                after = payload.get("after")
                if not after:
                    break
                time.sleep(2)
            except Exception as exc:
                errors.append(f"{query}: {exc!r}")
                break
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        records_examined=children_examined,
    )


def ingest_stackexchange() -> None:
    name = "stackexchange_public_search"
    count = 0
    items_examined = 0
    errors: list[str] = []
    sites = ("stackoverflow", "superuser", "webapps", "ai", "security")
    queries = ("chatgpt.com/share", "chat.openai.com/share")
    for site in sites:
        for query in queries:
            try:
                response = get(
                    "https://api.stackexchange.com/2.3/search/advanced",
                    params={
                        "site": site,
                        "q": query,
                        "filter": "withbody",
                        "pagesize": 100,
                        "page": 1,
                    },
                    timeout=120,
                    attempts=3,
                )
                response.raise_for_status()
                payload = response.json()
                for result in payload.get("items", []):
                    items_examined += 1
                    text = "\n".join(str(result.get(key, "")) for key in ("body", "title", "link"))
                    ref = result.get("link")
                    for found_url in extract_urls_from_fragment(html.unescape(text)):
                        item = item_for(found_url)
                        if not item:
                            continue
                        item.sources.add(name)
                        add_reference(item, "stackexchange_references", ref)
                        count += 1
                backoff = as_int(payload.get("backoff"))
                if backoff:
                    time.sleep(min(backoff, 30))
            except Exception as exc:
                errors.append(f"{site}:{query}: {exc!r}")
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        items_examined=items_examined,
        sites_queried=len(sites),
    )


def ingest_gdelt_references() -> None:
    """Find public articles that reference share links, then inspect a bounded set."""
    name = "gdelt_doc_references"
    count = 0
    articles_examined = 0
    errors: list[str] = []
    article_urls: set[str] = set()
    for query in ('"chatgpt.com/share/"', '"chat.openai.com/share/"'):
        try:
            response = get(
                "https://api.gdeltproject.org/api/v2/doc/doc",
                params={
                    "query": query,
                    "mode": "artlist",
                    "maxrecords": 250,
                    "format": "json",
                    "sort": "datedesc",
                },
                timeout=180,
                attempts=3,
            )
            response.raise_for_status()
            for article in response.json().get("articles", []):
                if article.get("url"):
                    article_urls.add(str(article["url"]))
        except Exception as exc:
            errors.append(f"{query}: {exc!r}")
    for article_url in list(article_urls)[:300]:
        try:
            response = get(article_url, timeout=30, attempts=1)
            if response.status_code >= 400 or len(response.content) > 5_000_000:
                continue
            articles_examined += 1
            for found_url in extract_urls_from_fragment(response.text):
                item = item_for(found_url)
                if not item:
                    continue
                item.sources.add(name)
                add_reference(item, "gdelt_article_references", article_url)
                count += 1
        except Exception:
            continue
    source_status(
        name,
        count > 0 or not errors,
        count,
        " | ".join(errors) or None,
        article_search_results=len(article_urls),
        articles_examined=articles_examined,
        article_fetch_limit=300,
    )


def select_best(item: Entry) -> None:
    values = item.values
    if values.get("sharegptx_text_bytes_utf8") is not None:
        prefix = "sharegptx"
        values["best_measurement_basis"] = "sharegpt_x_public_research_snapshot"
        values["measurement_confidence"] = "high_public_research_snapshot"
    elif values.get("sharechat_text_bytes_utf8") is not None:
        prefix = "sharechat"
        values["best_measurement_basis"] = "pii_redacted_sharechat_derivative"
        values["measurement_confidence"] = "medium_redacted_derivative"
    else:
        prefix = None
        values["best_measurement_basis"] = "no_conversation_text_measurement"
        values["measurement_confidence"] = "url_or_crawl_metadata_only"
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
            values[target] = values.get(f"{prefix}_{suffix}")
    for basis, key in [
        ("sharegpt_x", "sharegptx_message_count"),
        ("sharechat_derivative", "sharechat_message_count"),
    ]:
        if values.get(key) is not None:
            values["best_message_count"] = values.get(key)
            values["best_message_count_basis"] = basis
            break
    values["historically_observed_public"] = bool(
        values.get("sharegptx_message_count") is not None
        or values.get("sharechat_message_count") is not None
        or values.get("wayback_http_status") == 200
        or values.get("commoncrawl_latest_status") == 200
    )
    values["current_validity"] = "not_live_checked"
    values["current_validity_reason"] = (
        "Historical dataset/crawl/reference discovery does not prove the link remains public; "
        "bulk live validation was intentionally omitted."
    )


def rows() -> list[dict[str, Any]]:
    output = []
    for share_id in sorted(entries):
        item = entries[share_id]
        select_best(item)
        output.append(
            {
                "url": item.url,
                "share_id": item.share_id,
                "discovery_sources": ";".join(sorted(item.sources)),
                "discovery_source_count": len(item.sources),
                **item.values,
                "inventory_generated_at": RUN_AT,
            }
        )
    return output


def ordered_columns(data: list[dict[str, Any]]) -> list[str]:
    all_columns = sorted({key for row in data for key in row})
    first = [
        "url",
        "share_id",
        "title",
        "current_validity",
        "historically_observed_public",
        "best_text_bytes",
        "best_word_count_estimate",
        "best_character_count",
        "best_nonspace_character_count",
        "best_estimated_tokens",
        "best_message_count",
        "best_code_block_count",
        "best_code_character_count",
        "best_measurement_basis",
        "measurement_confidence",
        "sharechat_language",
        "sharechat_model",
        "sharegptx_models",
        "sharegptx_first_message_time",
        "sharegptx_last_message_time",
        "wayback_first_seen",
        "wayback_last_seen",
        "commoncrawl_first_seen",
        "commoncrawl_last_seen",
        "discovery_sources",
        "discovery_source_count",
    ]
    return [x for x in first if x in all_columns] + [
        x for x in all_columns if x not in first
    ]


def write_csv_gz(path: Path, data: list[dict[str, Any]]) -> None:
    columns = ordered_columns(data)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)


def write_parquet(path: Path, data: list[dict[str, Any]]) -> None:
    normalized = []
    for row in data:
        normalized.append(
            {
                key: (
                    json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (dict, list, set))
                    else value
                )
                for key, value in row.items()
            }
        )
    pq.write_table(pa.Table.from_pylist(normalized), path, compression="zstd")


def write_observations(path: Path) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for item in entries.values():
            for observation in item.observations:
                handle.write(
                    json.dumps(
                        {"url": item.url, "share_id": item.share_id, **observation},
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    + "\n"
                )


def write_search_plan(path: Path) -> None:
    engines = {
        "Google": "https://www.google.com/search?q=",
        "Bing": "https://www.bing.com/search?q=",
        "Brave": "https://search.brave.com/search?q=",
        "DuckDuckGo": "https://duckduckgo.com/?q=",
        "Mojeek": "https://www.mojeek.com/search?q=",
        "Yandex": "https://yandex.com/search/?text=",
        "Yahoo": "https://search.yahoo.com/search?p=",
        "Qwant": "https://www.qwant.com/?q=",
        "Ecosia": "https://www.ecosia.org/search?q=",
        "Yep": "https://yep.com/web?q=",
        "Marginalia": "https://search.marginalia.nu/search?query=",
        "Mwmbl": "https://mwmbl.org/search?q=",
        "Baidu": "https://www.baidu.com/s?wd=",
        "Naver": "https://search.naver.com/search.naver?query=",
    }
    records = []
    for engine, base in engines.items():
        for first in "0123456789abcdef":
            for second in "0123456789abcdef":
                prefix = first + second
                query = f"site:https://chatgpt.com/share/{prefix}"
                records.append(
                    {
                        "engine": engine,
                        "uuid_prefix": prefix,
                        "query": query,
                        "search_url": base + quote_plus(query),
                        "execution_status": (
                            "public_api_executed_separately"
                            if engine in {"Marginalia", "Mwmbl"}
                            else "query_plan_not_api_executed"
                        ),
                    }
                )
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
        "historically_observed_public": sum(
            bool(row.get("historically_observed_public")) for row in data
        ),
        "rows_with_text_measurement": len(sizes),
        "rows_with_word_measurement": len(words),
        "source_unique_url_counts": dict(sorted(source_counts.items())),
        "source_overlap_histogram": {str(k): v for k, v in sorted(overlaps.items())},
        "chao1_discoverable_population_lower_bound": chao1,
        "word_count_estimate": stats(words),
        "text_bytes_utf8": stats(sizes),
        "source_status": status,
        "limitations": [
            "Maximum-recall public-discovery snapshot, not every ChatGPT share ever created.",
            "Current live validity was not bulk checked.",
            "ShareGPT-X UUID IDs are used to reconstruct canonical chatgpt.com/share URLs.",
            "Consumer search-engine queries are supplied as a plan unless a free API was executed.",
            "Archive record lengths are compressed WARC/CDX records, not conversation text sizes.",
            "CJK-heavy word counts are character-equivalent estimates and marked low-confidence.",
            "ShareChat measurements use a PII-redacted derivative and can differ from originals.",
            "Common Crawl and public APIs may rate-limit, time out, or provide partial coverage.",
        ],
    }


def write_methodology(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        f"""# ChatGPT Share URL Inventory Methodology

Generated: `{RUN_AT}`

## Scope

This is a metadata-only, maximum-recall inventory of publicly discoverable
`https://chatgpt.com/share/<uuid>` URLs. Legacy `chat.openai.com/share/<uuid>` links
are normalized to the current canonical host. Every row includes the full canonical URL.

The collector does not brute-force share IDs, bypass access controls, bulk-live-check
ChatGPT, or submit URLs to public scanning services. Conversation bodies from public
research datasets are processed transiently to calculate aggregate metrics and are not
included in the output package.

## Executed sources

- PII-redacted `qdwqwd/sharechat-conversations` ChatGPT subset through Hugging Face
  auto-converted Parquet.
- `DSULT-Core/ShareGPT-X` Simple/full subset; canonical URLs reconstructed from its
  UUID `id` field and aggregate metrics calculated without emitting message content.
- Internet Archive Wayback CDX for current and legacy share hosts.
- Common Crawl CDX indexes for every listed collection from 2023 onward.
- Existing urlscan.io records only; no scan submission.
- GitHub public code search with adaptive hexadecimal partitioning.
- Sourcegraph's unauthenticated exhaustive public-code stream API.
- grep.app's public code-search endpoint.
- Marginalia Search's public API key.
- Mwmbl's public search endpoint.
- Hacker News Algolia, Reddit public search, Stack Exchange public search.
- GDELT DOC article search followed by a bounded inspection of returned public pages.

A separate plan contains 256 UUID-prefix searches for each of fourteen search engines.
Consumer-engine queries are never falsely marked as executed.

## Size and length definitions

- `best_text_bytes`: UTF-8 bytes of extracted message text.
- `best_character_count`: Unicode code points in extracted text.
- `best_word_count_estimate`: Unicode alphanumeric token sequences; CJK-heavy text
  uses character-equivalents and is explicitly marked low-confidence.
- `best_estimated_tokens`: heuristic only, not an OpenAI billing-token count.
- `*_compressed_record_bytes`: compressed crawl-record size, never substituted for
  text-body size.

## Validity

`current_validity=not_live_checked` is deliberate. A historical dataset, archive
capture, or public web reference proves discovery, not that the owner has kept the link
public. OpenAI's help documentation says anyone possessing a shared link can view it and
the owner can later delete or invalidate it.

## Summary

```json
{json.dumps(summary, ensure_ascii=False, indent=2)}
```
""",
        encoding="utf-8",
    )


def main() -> int:
    stages = [
        ingest_sharechat,
        ingest_sharegpt_x,
        ingest_wayback,
        ingest_commoncrawl,
        ingest_urlscan,
        ingest_github_code_search,
        ingest_sourcegraph,
        ingest_grep_app,
        ingest_marginalia,
        ingest_mwmbl,
        ingest_hackernews,
        ingest_reddit,
        ingest_stackexchange,
        ingest_gdelt_references,
    ]
    for stage in stages:
        started = time.time()
        print(
            json.dumps({"stage": stage.__name__, "status": "start", "at": now()}),
            flush=True,
        )
        stage()
        print(
            json.dumps(
                {
                    "stage": stage.__name__,
                    "status": "done",
                    "elapsed_seconds": round(time.time() - started, 2),
                    "unique_urls_so_far": len(entries),
                }
            ),
            flush=True,
        )

    data = rows()
    summary = make_summary(data)

    write_csv_gz(OUT / "chatgpt-share-inventory.csv.gz", data)
    write_parquet(OUT / "chatgpt-share-inventory.parquet", data)
    (OUT / "chatgpt-share-urls.txt").write_text(
        "\n".join(row["url"] for row in data) + "\n", encoding="utf-8"
    )
    write_observations(OUT / "source-observations.ndjson.gz")
    write_search_plan(OUT / "search-engine-query-plan.csv")
    (OUT / "coverage-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (OUT / "source-status.json").write_text(
        json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    write_methodology(OUT / "methodology.md", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
