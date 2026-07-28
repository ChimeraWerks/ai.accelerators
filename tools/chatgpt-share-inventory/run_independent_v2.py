#!/usr/bin/env python3
"""Independent public discovery of ChatGPT share URLs.

This run deliberately DOES NOT read Hugging Face, ShareChat, ShareGPT-X, or any
previous URL inventory. It discovers candidates from public archives, source-code
indexes, public forum/social APIs, threat-intelligence indexes, and metasearch.
Conversation bodies are never retained or emitted.
"""
from __future__ import annotations

import concurrent.futures
import csv
import html
import json
import os
import random
import re
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import quote_plus, urlencode, urljoin, urlparse

import requests

import build_inventory as collector

# Explicit guardrail: this runner must never invoke dataset-derived URL sources.
FORBIDDEN_STAGE_NAMES = {
    "ingest_sharechat",
    "ingest_sharegpt_x",
    "ingest_huggingface",
}

DOMAINS = ("chatgpt.com", "chat.openai.com")
HEX = "0123456789abcdef"
RUN_AT = datetime.now(timezone.utc).isoformat()
TOKEN = os.environ.get("GITHUB_TOKEN")
OUT = collector.OUT

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "ChimeraWerks-ChatGPTShareIndependentDiscovery/2.0 (+metadata-only public research)",
    "Accept": "application/json,text/html,text/plain,*/*",
})

RATE_LOCK = threading.Lock()
LAST_CALL: dict[str, float] = defaultdict(float)


def log(stage: str, event: str, **data: Any) -> None:
    print(json.dumps({"at": collector.now(), "stage": stage, "event": event, **data}, ensure_ascii=False), flush=True)


def throttle(key: str, interval: float) -> None:
    with RATE_LOCK:
        delay = interval - (time.monotonic() - LAST_CALL[key])
        if delay > 0:
            time.sleep(delay)
        LAST_CALL[key] = time.monotonic()


def request(
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 60,
    attempts: int = 4,
    rate_key: str | None = None,
    min_interval: float = 0.0,
    stream: bool = False,
) -> requests.Response:
    last: Exception | None = None
    for attempt in range(attempts):
        if rate_key and min_interval:
            throttle(rate_key, min_interval)
        try:
            response = SESSION.request(
                method,
                url,
                params=params,
                headers=headers,
                timeout=timeout,
                stream=stream,
            )
            if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else min(30.0, 2.0 ** attempt + random.random())
                except ValueError:
                    delay = min(30.0, 2.0 ** attempt + random.random())
                time.sleep(delay)
                continue
            return response
        except requests.RequestException as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(30.0, 2.0 ** attempt + random.random()))
    raise RuntimeError(f"{method} {url} failed after {attempts} attempts: {last!r}")


def extract_urls(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    elif isinstance(value, str):
        text = html.unescape(value)
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        except Exception:
            text = str(value)
    return {match.group(0) for match in collector.SHARE_RE.finditer(text)}


def record(value: Any, source: str, *, ref: str | None = None, metadata: dict[str, Any] | None = None) -> bool:
    added = False
    for found in extract_urls(value):
        item = collector.item_for(found)
        if not item:
            continue
        item.sources.add(source)
        if ref:
            refs = set(str(item.values.get("public_references", "")).split(";")) - {""}
            refs.add(ref)
            item.values["public_references"] = ";".join(sorted(refs))
        if metadata:
            for key, val in metadata.items():
                if val is not None and val != "":
                    item.values.setdefault(key, val)
        added = True
    return added


class LinkTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []
        self.text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            for key, value in attrs:
                if key.lower() == "href" and value:
                    self.hrefs.append(value)

    def handle_data(self, data: str) -> None:
        self.text_parts.append(data)


def parse_html_candidates(base_url: str, body: str) -> tuple[set[str], list[str]]:
    parser = LinkTextParser()
    try:
        parser.feed(body)
    except Exception:
        pass
    candidates = extract_urls(body)
    outbound = []
    for href in parser.hrefs:
        absolute = urljoin(base_url, href)
        candidates.update(extract_urls(absolute))
        if absolute.startswith(("http://", "https://")):
            outbound.append(absolute)
    return candidates, outbound


def stage(name: str) -> Callable[[Callable[[], None]], Callable[[], None]]:
    def decorator(fn: Callable[[], None]) -> Callable[[], None]:
        def wrapped() -> None:
            before = len(collector.entries)
            started = time.time()
            log(name, "start", unique_before=before)
            try:
                fn()
                ok = collector.status.get(name, {}).get("ok", True)
                error = collector.status.get(name, {}).get("error")
            except Exception as exc:
                collector.source_status(name, False, len(collector.entries) - before, repr(exc))
                ok = False
                error = repr(exc)
            log(
                name,
                "done",
                ok=ok,
                error=error,
                elapsed_seconds=round(time.time() - started, 2),
                unique_added=len(collector.entries) - before,
                unique_total=len(collector.entries),
            )
        wrapped.__name__ = name
        return wrapped
    return decorator


# ---------------------------------------------------------------------------
# Historical direct-URL enumeration
# ---------------------------------------------------------------------------

@stage("wayback_partitioned_cdx")
def ingest_wayback_partitioned() -> None:
    endpoint = "https://web.archive.org/cdx/search/cdx"
    observations = 0
    errors: list[str] = []
    buckets = [(domain, prefix) for domain in DOMAINS for prefix in HEX]

    def one(bucket: tuple[str, str]) -> tuple[int, str | None]:
        domain, prefix = bucket
        params = {
            "url": f"{domain}/share/{prefix}",
            "matchType": "prefix",
            "output": "json",
            "fl": "original,timestamp,statuscode,mimetype,digest,length",
            "filter": "statuscode:200",
            "collapse": "urlkey",
            "limit": "100000",
        }
        try:
            response = request("GET", endpoint, params=params, timeout=180, attempts=5, rate_key="wayback", min_interval=0.25)
            response.raise_for_status()
            payload = response.json()
            if not payload or not isinstance(payload, list):
                return 0, None
            header, *rows = payload
            count = 0
            for row in rows:
                if not isinstance(row, list):
                    continue
                observation = dict(zip(header, row))
                if record(
                    observation.get("original"),
                    "wayback_partitioned_cdx",
                    ref=response.url,
                    metadata={
                        "wayback_first_seen": observation.get("timestamp"),
                        "wayback_http_status": observation.get("statuscode"),
                        "wayback_mimetype": observation.get("mimetype"),
                        "wayback_digest": observation.get("digest"),
                        "wayback_compressed_record_bytes": observation.get("length"),
                    },
                ):
                    count += 1
            return count, None
        except Exception as exc:
            return 0, f"{domain}/{prefix}: {exc!r}"

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for count, error in pool.map(one, buckets):
            observations += count
            if error:
                errors.append(error)
    collector.source_status(
        "wayback_partitioned_cdx",
        observations > 0 or len(errors) < len(buckets),
        observations,
        " | ".join(errors[:12]) or None,
        bucket_count=len(buckets),
        failed_buckets=len(errors),
        strategy="domain x first-hex prefix; collapsed URL keys",
    )


@stage("commoncrawl_cdx_all_collections")
def ingest_commoncrawl_cdx() -> None:
    coll = request("GET", "https://index.commoncrawl.org/collinfo.json", timeout=90, attempts=5).json()
    collections = [c for c in coll if str(c.get("id", "")) >= "CC-MAIN-2023-01"]
    observations = 0
    errors: list[str] = []

    def query(collection: dict[str, Any], domain: str) -> tuple[int, str | None]:
        cid = str(collection.get("id"))
        endpoint = collection.get("cdx-api")
        if not endpoint:
            return 0, f"{cid}: missing endpoint"
        params = {
            "url": f"{domain}/share/*",
            "output": "json",
            "filter": "status:200",
            "collapse": "urlkey",
        }
        try:
            response = request("GET", str(endpoint), params=params, timeout=150, attempts=5, rate_key="cc", min_interval=0.10)
            if response.status_code == 404:
                return 0, None
            response.raise_for_status()
            count = 0
            for line in response.text.splitlines():
                try:
                    observation = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record(
                    observation.get("url"),
                    "commoncrawl_cdx_all_collections",
                    ref=response.url,
                    metadata={
                        "commoncrawl_first_seen": observation.get("timestamp"),
                        "commoncrawl_latest_status": observation.get("status"),
                        "commoncrawl_latest_mime": observation.get("mime"),
                        "commoncrawl_latest_digest": observation.get("digest"),
                        "commoncrawl_latest_compressed_record_bytes": observation.get("length"),
                        "commoncrawl_collection": cid,
                    },
                ):
                    count += 1
            return count, None
        except Exception as exc:
            return 0, f"{cid}/{domain}: {exc!r}"

    tasks = [(c, d) for c in collections for d in DOMAINS]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(query, c, d) for c, d in tasks]
        for future in concurrent.futures.as_completed(futures):
            count, error = future.result()
            observations += count
            if error:
                errors.append(error)
    collector.source_status(
        "commoncrawl_cdx_all_collections",
        observations > 0 or len(errors) < len(tasks),
        observations,
        " | ".join(errors[:12]) or None,
        collections_queried=len(collections),
        queries=len(tasks),
        failed_queries=len(errors),
    )


# ---------------------------------------------------------------------------
# Archived referring-page mining
# ---------------------------------------------------------------------------

@stage("arquivo_pt_referring_pages")
def ingest_arquivo_pt() -> None:
    endpoint = "https://arquivo.pt/textsearch"
    page_urls: dict[str, str] = {}
    response_items = 0
    fetched_pages = 0
    errors: list[str] = []
    queries = ['"chatgpt.com/share/"', '"chat.openai.com/share/"']

    for query in queries:
        for offset in range(0, 2000, 500):
            try:
                response = request(
                    "GET",
                    endpoint,
                    params={"q": query, "maxItems": 500, "offset": offset, "prettyPrint": "false"},
                    timeout=180,
                    attempts=5,
                    rate_key="arquivo-search",
                    min_interval=0.5,
                )
                response.raise_for_status()
                payload = response.json()
                items = payload.get("response_items", [])
                response_items += len(items)
                for item in items:
                    record(item, "arquivo_pt_referring_pages", ref=item.get("linkToArchive"))
                    extracted = item.get("linkToExtractedText")
                    archived = item.get("linkToNoFrame") or item.get("linkToArchive")
                    if extracted:
                        page_urls[str(extracted)] = str(archived or extracted)
                if len(items) < 500:
                    break
            except Exception as exc:
                errors.append(f"{query}/{offset}: {exc!r}")
                break

    # Fetch archived extracted text only to locate embedded share links. Do not retain text.
    def fetch_page(pair: tuple[str, str]) -> tuple[int, str | None]:
        url, ref = pair
        try:
            response = request("GET", url, timeout=90, attempts=3, rate_key="arquivo-text", min_interval=0.05)
            response.raise_for_status()
            found = extract_urls(response.text)
            for candidate in found:
                record(candidate, "arquivo_pt_referring_pages", ref=ref)
            return len(found), None
        except Exception as exc:
            return 0, repr(exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for count, error in pool.map(fetch_page, list(page_urls.items())):
            fetched_pages += 1
            if error:
                errors.append(error)
    collector.source_status(
        "arquivo_pt_referring_pages",
        response_items > 0 or not errors,
        response_items,
        " | ".join(errors[:10]) or None,
        search_results=response_items,
        extracted_pages_attempted=fetched_pages,
        policy="archived referring-page text scanned transiently; no page text retained",
    )


# ---------------------------------------------------------------------------
# Public source-code and developer-discussion indexes
# ---------------------------------------------------------------------------

GITHUB_HEADERS = {
    "Accept": "application/vnd.github.text-match+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
if TOKEN:
    GITHUB_HEADERS["Authorization"] = f"Bearer {TOKEN}"


def github_search(endpoint: str, query: str, source: str, *, max_pages: int = 10) -> tuple[int, int, list[str]]:
    count = 0
    total = 0
    errors: list[str] = []
    for page in range(1, max_pages + 1):
        try:
            response = request(
                "GET",
                f"https://api.github.com/search/{endpoint}",
                params={"q": query, "per_page": 100, "page": page},
                headers=GITHUB_HEADERS,
                timeout=120,
                attempts=4,
                rate_key=f"github-{endpoint}",
                min_interval=6.2 if endpoint == "code" else 2.2,
            )
            if response.status_code in {403, 422}:
                errors.append(f"HTTP {response.status_code}: {response.text[:200]}")
                break
            response.raise_for_status()
            payload = response.json()
            total = int(payload.get("total_count") or 0)
            items = payload.get("items", [])
            for item in items:
                ref = item.get("html_url")
                if record(item, source, ref=ref):
                    count += 1
            if len(items) < 100 or page * 100 >= min(total, 1000):
                break
        except Exception as exc:
            errors.append(repr(exc))
            break
    return count, total, errors


@stage("github_code_adaptive")
def ingest_github_code() -> None:
    if not TOKEN:
        collector.source_status("github_code_adaptive", False, error="GITHUB_TOKEN unavailable")
        return
    observations = 0
    queries = 0
    errors: list[str] = []
    queue: list[tuple[str, str]] = [(domain, prefix) for domain in DOMAINS for prefix in HEX]
    while queue:
        domain, prefix = queue.pop(0)
        query = f'"{domain}/share/{prefix}"'
        count, total, errs = github_search("code", query, "github_code_adaptive", max_pages=10)
        observations += count
        queries += 1
        errors.extend(errs)
        if total > 1000 and len(prefix) < 2:
            queue.extend((domain, prefix + second) for second in HEX)
    collector.source_status(
        "github_code_adaptive",
        observations > 0 or not errors,
        observations,
        " | ".join(errors[:10]) or None,
        adaptive_queries=queries,
        strategy="first-hex buckets, split to second hex only when GitHub's 1000-result cap is hit",
    )


@stage("github_issues_comments")
def ingest_github_issues() -> None:
    observations = 0
    errors: list[str] = []
    queries = 0
    for domain in DOMAINS:
        for year in (2023, 2024, 2025, 2026):
            query = f'"{domain}/share/" in:title,body,comments created:{year}-01-01..{year}-12-31'
            count, total, errs = github_search("issues", query, "github_issues_comments", max_pages=10)
            observations += count
            queries += 1
            errors.extend(errs)
            # GitHub caps results at 1000; split saturated years by month.
            if total > 1000:
                for month in range(1, 13):
                    start = f"{year}-{month:02d}-01"
                    end_month = month + 1
                    if end_month == 13:
                        end = f"{year + 1}-01-01"
                    else:
                        end = f"{year}-{end_month:02d}-01"
                    monthly = f'"{domain}/share/" in:title,body,comments created:{start}..{end}'
                    c, _, e = github_search("issues", monthly, "github_issues_comments", max_pages=10)
                    observations += c
                    queries += 1
                    errors.extend(e)
    collector.source_status(
        "github_issues_comments",
        observations > 0 or not errors,
        observations,
        " | ".join(errors[:10]) or None,
        queries=queries,
        scope="issue titles, bodies, and indexed comment text matches",
    )


@stage("github_commit_messages")
def ingest_github_commits() -> None:
    observations = 0
    errors: list[str] = []
    queries = 0
    for domain in DOMAINS:
        for year in (2023, 2024, 2025, 2026):
            query = f'"{domain}/share/" committer-date:{year}-01-01..{year}-12-31'
            count, _, errs = github_search("commits", query, "github_commit_messages", max_pages=10)
            observations += count
            queries += 1
            errors.extend(errs)
    collector.source_status(
        "github_commit_messages",
        observations > 0 or not errors,
        observations,
        " | ".join(errors[:10]) or None,
        queries=queries,
    )


@stage("sourcegraph_exhaustive_stream")
def ingest_sourcegraph() -> None:
    endpoint = "https://sourcegraph.com/.api/search/stream"
    observations = 0
    errors: list[str] = []
    regex = r"https?://(?:chatgpt\\.com|chat\\.openai\\.com)/share/[0-9a-fA-F-]{36}"
    queries = [
        f"/{regex}/ patternType:regexp count:all archived:yes fork:yes",
        f"/{regex}/ patternType:regexp count:all archived:yes fork:yes type:diff",
        f"/{regex}/ patternType:regexp count:all archived:yes fork:yes type:commit",
    ]
    for query in queries:
        try:
            response = request(
                "GET",
                endpoint,
                params={"q": query, "v": "V3", "display": "100000", "cm": "true"},
                headers={"Accept": "text/event-stream"},
                timeout=600,
                attempts=3,
                rate_key="sourcegraph",
                min_interval=2.0,
            )
            response.raise_for_status()
            event = None
            for line in response.text.splitlines():
                if line.startswith("event:"):
                    event = line.split(":", 1)[1].strip()
                elif line.startswith("data:") and event == "matches":
                    try:
                        payload = json.loads(line.split(":", 1)[1].strip())
                    except json.JSONDecodeError:
                        continue
                    if record(payload, "sourcegraph_exhaustive_stream", ref=response.url):
                        observations += 1
        except Exception as exc:
            errors.append(f"{query}: {exc!r}")
    collector.source_status(
        "sourcegraph_exhaustive_stream",
        observations > 0 or not errors,
        observations,
        " | ".join(errors[:6]) or None,
        queries=len(queries),
        strategy="content + diff + commit exhaustive streaming searches",
    )


@stage("grep_app_paginated")
def ingest_grep_app() -> None:
    endpoint = "https://grep.app/api/search"
    observations = 0
    errors: list[str] = []
    pages = 0
    for domain in DOMAINS:
        for page in range(1, 101):
            try:
                response = request(
                    "GET",
                    endpoint,
                    params={"q": f"{domain}/share/", "page": page},
                    timeout=90,
                    attempts=5,
                    rate_key="grep-app",
                    min_interval=2.5,
                )
                if response.status_code == 429:
                    errors.append(f"{domain}/page{page}: 429")
                    break
                response.raise_for_status()
                payload = response.json()
                hits = payload.get("hits", {}).get("hits", [])
                pages += 1
                for hit in hits:
                    repo = hit.get("repo", {}).get("raw") if isinstance(hit.get("repo"), dict) else hit.get("repo")
                    path = hit.get("path", {}).get("raw") if isinstance(hit.get("path"), dict) else hit.get("path")
                    ref = f"https://github.com/{repo}/blob/HEAD/{path}" if repo and path else None
                    if record(hit, "grep_app_paginated", ref=ref):
                        observations += 1
                if not hits:
                    break
            except Exception as exc:
                errors.append(f"{domain}/page{page}: {exc!r}")
                break
    collector.source_status(
        "grep_app_paginated",
        observations > 0 or not errors,
        observations,
        " | ".join(errors[:8]) or None,
        pages=pages,
    )


# ---------------------------------------------------------------------------
# Search/metasearch surfaces
# ---------------------------------------------------------------------------

@stage("searxng_distributed_metasearch")
def ingest_searxng() -> None:
    observations = 0
    errors: list[str] = []
    instances: list[str] = []
    try:
        payload = request("GET", "https://searx.space/data/instances.json", timeout=90, attempts=5).json()
        for url, info in payload.get("instances", {}).items():
            if not str(url).startswith("https://"):
                continue
            if info.get("network_type") not in {None, "normal"}:
                continue
            timing = info.get("timing", {})
            if timing.get("initial") is not None and float(timing.get("initial")) > 2.5:
                continue
            instances.append(str(url).rstrip("/"))
        instances = instances[:12]
    except Exception as exc:
        errors.append(f"instance-list: {exc!r}")

    if not instances:
        # Known public fallbacks; failures remain isolated.
        instances = [
            "https://search.bus-hit.me",
            "https://searx.be",
            "https://search.sapti.me",
            "https://searx.tiekoetter.com",
        ]

    queries = [(domain, prefix) for domain in DOMAINS for prefix in HEX]
    successful_instances = Counter()
    for index, (domain, prefix) in enumerate(queries):
        query = f"site:{domain}/share/{prefix}"
        # Rotate each query across instances; fall back through up to three.
        for attempt in range(min(3, len(instances))):
            base = instances[(index + attempt) % len(instances)]
            try:
                response = request(
                    "GET",
                    base + "/search",
                    params={"q": query, "format": "json", "pageno": 1, "safesearch": 0},
                    timeout=45,
                    attempts=2,
                    rate_key=f"searx-{base}",
                    min_interval=1.5,
                )
                if response.status_code == 403:
                    # Most public instances disable JSON. Parse the HTML UI instead.
                    response = request(
                        "GET",
                        base + "/search",
                        params={"q": query, "pageno": 1, "safesearch": 0},
                        timeout=45,
                        attempts=2,
                        rate_key=f"searx-{base}",
                        min_interval=1.5,
                    )
                    response.raise_for_status()
                    candidates, _ = parse_html_candidates(response.url, response.text)
                    for candidate in candidates:
                        record(candidate, "searxng_distributed_metasearch", ref=response.url)
                    observations += len(candidates)
                else:
                    response.raise_for_status()
                    data = response.json()
                    for result in data.get("results", []):
                        if record(result, "searxng_distributed_metasearch", ref=response.url):
                            observations += 1
                successful_instances[base] += 1
                break
            except Exception as exc:
                errors.append(f"{base}/{domain}/{prefix}: {exc!r}")
    collector.source_status(
        "searxng_distributed_metasearch",
        observations > 0 or bool(successful_instances),
        observations,
        " | ".join(errors[:10]) or None,
        instances_attempted=len(instances),
        successful_instances=dict(successful_instances),
        query_count=len(queries),
    )


def generic_search_html(
    source: str,
    build_url: Callable[[str, int], str],
    *,
    pages: int,
    interval: float,
) -> tuple[int, list[str]]:
    observations = 0
    errors: list[str] = []
    for domain in DOMAINS:
        for prefix in HEX:
            query = f"site:{domain}/share/{prefix}"
            for page in range(pages):
                url = build_url(query, page)
                try:
                    response = request(
                        "GET",
                        url,
                        timeout=60,
                        attempts=3,
                        rate_key=source,
                        min_interval=interval,
                    )
                    if response.status_code in {403, 429}:
                        errors.append(f"{response.status_code}: {url}")
                        break
                    response.raise_for_status()
                    candidates, _ = parse_html_candidates(response.url, response.text)
                    for candidate in candidates:
                        record(candidate, source, ref=response.url)
                    observations += len(candidates)
                    if not candidates and page > 0:
                        break
                except Exception as exc:
                    errors.append(f"{url}: {exc!r}")
                    break
    return observations, errors


@stage("duckduckgo_html_partitioned")
def ingest_duckduckgo() -> None:
    count, errors = generic_search_html(
        "duckduckgo_html_partitioned",
        lambda q, page: "https://html.duckduckgo.com/html/?" + urlencode({"q": q, "s": page * 30}),
        pages=4,
        interval=1.5,
    )
    collector.source_status("duckduckgo_html_partitioned", count > 0 or not errors, count, " | ".join(errors[:8]) or None)


@stage("bing_html_partitioned")
def ingest_bing() -> None:
    count, errors = generic_search_html(
        "bing_html_partitioned",
        lambda q, page: "https://www.bing.com/search?" + urlencode({"q": q, "first": page * 10 + 1, "count": 50}),
        pages=4,
        interval=1.5,
    )
    collector.source_status("bing_html_partitioned", count > 0 or not errors, count, " | ".join(errors[:8]) or None)


@stage("mojeek_html_partitioned")
def ingest_mojeek() -> None:
    count, errors = generic_search_html(
        "mojeek_html_partitioned",
        lambda q, page: "https://www.mojeek.com/search?" + urlencode({"q": q, "s": page * 10}),
        pages=4,
        interval=1.5,
    )
    collector.source_status("mojeek_html_partitioned", count > 0 or not errors, count, " | ".join(errors[:8]) or None)


@stage("qwant_api_partitioned")
def ingest_qwant() -> None:
    endpoint = "https://api.qwant.com/v3/search/web"
    observations = 0
    errors: list[str] = []
    for domain in DOMAINS:
        for prefix in HEX:
            query = f"site:{domain}/share/{prefix}"
            for offset in (0, 10, 20, 30):
                try:
                    response = request(
                        "GET",
                        endpoint,
                        params={"q": query, "count": 10, "offset": offset, "locale": "en_US", "safesearch": 0},
                        headers={"Referer": "https://www.qwant.com/"},
                        timeout=60,
                        attempts=3,
                        rate_key="qwant",
                        min_interval=1.5,
                    )
                    if response.status_code in {403, 429}:
                        errors.append(f"HTTP {response.status_code}")
                        break
                    response.raise_for_status()
                    payload = response.json()
                    if record(payload, "qwant_api_partitioned", ref=response.url):
                        observations += 1
                except Exception as exc:
                    errors.append(repr(exc))
                    break
    collector.source_status("qwant_api_partitioned", observations > 0 or not errors, observations, " | ".join(errors[:8]) or None)


# ---------------------------------------------------------------------------
# Social, forum, news, and threat-intelligence reference mining
# ---------------------------------------------------------------------------

@stage("urlscan_paginated_existing")
def ingest_urlscan_paginated() -> None:
    endpoint = "https://urlscan.io/api/v1/search/"
    observations = 0
    errors: list[str] = []
    pages = 0
    for domain in DOMAINS:
        search_after: str | None = None
        for _ in range(100):
            params: dict[str, Any] = {"q": f"domain:{domain}", "size": 100}
            if search_after:
                params["search_after"] = search_after
            try:
                response = request("GET", endpoint, params=params, timeout=120, attempts=5, rate_key="urlscan", min_interval=1.0)
                response.raise_for_status()
                payload = response.json()
                results = payload.get("results", [])
                pages += 1
                for result in results:
                    if record(result, "urlscan_paginated_existing", ref=result.get("result")):
                        observations += 1
                if len(results) < 100:
                    break
                sort_value = results[-1].get("sort")
                if not sort_value:
                    break
                search_after = ",".join(map(str, sort_value)) if isinstance(sort_value, list) else str(sort_value)
            except Exception as exc:
                errors.append(f"{domain}: {exc!r}")
                break
    collector.source_status(
        "urlscan_paginated_existing",
        observations > 0 or not errors,
        observations,
        " | ".join(errors[:8]) or None,
        pages=pages,
        policy="existing records only; no scans submitted",
    )


@stage("alienvault_otx_url_list")
def ingest_otx() -> None:
    observations = 0
    errors: list[str] = []
    for domain in DOMAINS:
        for page in range(1, 101):
            try:
                response = request(
                    "GET",
                    f"https://otx.alienvault.com/api/v1/indicators/domain/{domain}/url_list",
                    params={"limit": 500, "page": page},
                    timeout=90,
                    attempts=4,
                    rate_key="otx",
                    min_interval=1.0,
                )
                if response.status_code in {401, 403}:
                    errors.append(f"{domain}: authentication required")
                    break
                response.raise_for_status()
                payload = response.json()
                url_list = payload.get("url_list", [])
                for item in url_list:
                    if record(item, "alienvault_otx_url_list", ref=response.url):
                        observations += 1
                if not payload.get("has_next") or not url_list:
                    break
            except Exception as exc:
                errors.append(f"{domain}/page{page}: {exc!r}")
                break
    collector.source_status("alienvault_otx_url_list", observations > 0 or not errors, observations, " | ".join(errors[:8]) or None)


@stage("bluesky_public_search")
def ingest_bluesky() -> None:
    endpoint = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
    observations = 0
    errors: list[str] = []
    for query in ("chatgpt.com/share/", "chat.openai.com/share/"):
        cursor = None
        for _ in range(100):
            params: dict[str, Any] = {"q": query, "limit": 100, "sort": "latest"}
            if cursor:
                params["cursor"] = cursor
            try:
                response = request("GET", endpoint, params=params, timeout=90, attempts=4, rate_key="bluesky", min_interval=0.5)
                response.raise_for_status()
                payload = response.json()
                posts = payload.get("posts", [])
                for post in posts:
                    uri = post.get("uri")
                    ref = None
                    if uri and uri.startswith("at://"):
                        parts = uri.split("/")
                        if len(parts) >= 5:
                            ref = f"https://bsky.app/profile/{parts[2]}/post/{parts[-1]}"
                    if record(post, "bluesky_public_search", ref=ref):
                        observations += 1
                cursor = payload.get("cursor")
                if not cursor or not posts:
                    break
            except Exception as exc:
                errors.append(f"{query}: {exc!r}")
                break
    collector.source_status("bluesky_public_search", observations > 0 or not errors, observations, " | ".join(errors[:8]) or None)


@stage("reddit_public_submission_search")
def ingest_reddit_public() -> None:
    observations = 0
    errors: list[str] = []
    for query in ('"chatgpt.com/share/"', '"chat.openai.com/share/"'):
        after = None
        for _ in range(10):
            params: dict[str, Any] = {"q": query, "limit": 100, "sort": "new", "t": "all", "raw_json": 1}
            if after:
                params["after"] = after
            try:
                response = request(
                    "GET",
                    "https://www.reddit.com/search.json",
                    params=params,
                    timeout=90,
                    attempts=4,
                    rate_key="reddit",
                    min_interval=2.0,
                )
                if response.status_code in {403, 429}:
                    errors.append(f"HTTP {response.status_code}")
                    break
                response.raise_for_status()
                payload = response.json().get("data", {})
                children = payload.get("children", [])
                for child in children:
                    data = child.get("data", {})
                    ref = "https://www.reddit.com" + str(data.get("permalink", "")) if data.get("permalink") else None
                    if record(data, "reddit_public_submission_search", ref=ref):
                        observations += 1
                after = payload.get("after")
                if not after or not children:
                    break
            except Exception as exc:
                errors.append(f"{query}: {exc!r}")
                break
    collector.source_status("reddit_public_submission_search", observations > 0 or not errors, observations, " | ".join(errors[:8]) or None)


@stage("arctic_shift_reddit_archive")
def ingest_arctic_shift() -> None:
    base = "https://arctic-shift.photon-reddit.com/api"
    observations = 0
    errors: list[str] = []
    requests_made = 0
    specs = [
        ("comments/search", "body"),
        ("posts/search", "selftext"),
        ("posts/search", "title"),
    ]
    for endpoint, field in specs:
        for needle in ("chatgpt.com/share/", "chat.openai.com/share/"):
            after = None
            for _ in range(100):
                params: dict[str, Any] = {field: needle, "limit": 100, "sort": "asc"}
                if after:
                    params["after"] = after
                try:
                    response = request(
                        "GET",
                        f"{base}/{endpoint}",
                        params=params,
                        timeout=120,
                        attempts=3,
                        rate_key="arctic",
                        min_interval=1.0,
                    )
                    requests_made += 1
                    if response.status_code in {400, 404, 422}:
                        errors.append(f"{endpoint}/{field}: HTTP {response.status_code}")
                        break
                    response.raise_for_status()
                    payload = response.json()
                    rows = payload.get("data", payload if isinstance(payload, list) else [])
                    if not isinstance(rows, list):
                        break
                    for row in rows:
                        permalink = row.get("permalink") if isinstance(row, dict) else None
                        ref = "https://www.reddit.com" + permalink if permalink else None
                        if record(row, "arctic_shift_reddit_archive", ref=ref):
                            observations += 1
                    if len(rows) < 100:
                        break
                    timestamps = [r.get("created_utc") for r in rows if isinstance(r, dict) and r.get("created_utc")]
                    if not timestamps:
                        break
                    after = str(max(timestamps))
                except Exception as exc:
                    errors.append(f"{endpoint}/{field}: {exc!r}")
                    break
    collector.source_status(
        "arctic_shift_reddit_archive",
        observations > 0 or not errors,
        observations,
        " | ".join(errors[:10]) or None,
        requests=requests_made,
    )


@stage("hackernews_algolia_full")
def ingest_hackernews() -> None:
    observations = 0
    errors: list[str] = []
    for query in ("chatgpt.com/share/", "chat.openai.com/share/"):
        for tags in ("story", "comment"):
            for page in range(100):
                try:
                    response = request(
                        "GET",
                        "https://hn.algolia.com/api/v1/search_by_date",
                        params={"query": query, "tags": tags, "hitsPerPage": 1000, "page": page},
                        timeout=90,
                        attempts=4,
                        rate_key="hn",
                        min_interval=0.5,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    hits = payload.get("hits", [])
                    for hit in hits:
                        object_id = hit.get("objectID")
                        ref = f"https://news.ycombinator.com/item?id={object_id}" if object_id else None
                        if record(hit, "hackernews_algolia_full", ref=ref):
                            observations += 1
                    if page + 1 >= int(payload.get("nbPages") or 0) or not hits:
                        break
                except Exception as exc:
                    errors.append(f"{query}/{tags}/{page}: {exc!r}")
                    break
    collector.source_status("hackernews_algolia_full", observations > 0 or not errors, observations, " | ".join(errors[:8]) or None)


@stage("stackexchange_multisite")
def ingest_stackexchange() -> None:
    sites = [
        "stackoverflow", "superuser", "serverfault", "askubuntu", "softwareengineering",
        "ai", "datascience", "security", "webapps", "math", "academia", "workplace",
    ]
    observations = 0
    errors: list[str] = []
    quota = None
    for site in sites:
        for query in ("chatgpt.com/share/", "chat.openai.com/share/"):
            page = 1
            while page <= 100:
                try:
                    response = request(
                        "GET",
                        "https://api.stackexchange.com/2.3/search/advanced",
                        params={"site": site, "q": query, "filter": "withbody", "pagesize": 100, "page": page, "order": "desc", "sort": "creation"},
                        timeout=90,
                        attempts=4,
                        rate_key="stackexchange",
                        min_interval=0.2,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    quota = payload.get("quota_remaining", quota)
                    for item in payload.get("items", []):
                        if record(item, "stackexchange_multisite", ref=item.get("link")):
                            observations += 1
                    if not payload.get("has_more"):
                        break
                    if payload.get("backoff"):
                        time.sleep(int(payload["backoff"]))
                    page += 1
                except Exception as exc:
                    errors.append(f"{site}/{query}: {exc!r}")
                    break
    collector.source_status(
        "stackexchange_multisite",
        observations > 0 or not errors,
        observations,
        " | ".join(errors[:8]) or None,
        sites=len(sites),
        quota_remaining=quota,
    )


@stage("lemmy_federated_search")
def ingest_lemmy() -> None:
    instances = [
        "https://lemmy.world", "https://lemmy.ml", "https://sh.itjust.works",
        "https://programming.dev", "https://beehaw.org", "https://discuss.online",
    ]
    observations = 0
    errors: list[str] = []
    for instance in instances:
        for query in ("chatgpt.com/share/", "chat.openai.com/share/"):
            for page in range(1, 21):
                try:
                    response = request(
                        "GET",
                        instance + "/api/v3/search",
                        params={"q": query, "type_": "All", "sort": "New", "listing_type": "All", "page": page, "limit": 50},
                        timeout=60,
                        attempts=3,
                        rate_key=f"lemmy-{instance}",
                        min_interval=0.5,
                    )
                    if response.status_code in {403, 404, 429}:
                        errors.append(f"{instance}: HTTP {response.status_code}")
                        break
                    response.raise_for_status()
                    payload = response.json()
                    before = observations
                    if record(payload, "lemmy_federated_search", ref=response.url):
                        observations += 1
                    counts = sum(len(payload.get(k, [])) for k in ("posts", "comments", "communities", "users") if isinstance(payload.get(k), list))
                    if counts == 0 or observations == before and page > 1:
                        break
                except Exception as exc:
                    errors.append(f"{instance}/{query}: {exc!r}")
                    break
    collector.source_status("lemmy_federated_search", observations > 0 or not errors, observations, " | ".join(errors[:10]) or None, instances=len(instances))


@stage("discourse_forum_search")
def ingest_discourse() -> None:
    forums = [
        "https://community.openai.com",
        "https://discuss.huggingface.co",
        "https://community.brave.com",
        "https://forums.fast.ai",
        "https://discuss.pytorch.org",
    ]
    observations = 0
    errors: list[str] = []
    for forum in forums:
        for query in ("chatgpt.com/share/", "chat.openai.com/share/"):
            for page in range(1, 21):
                try:
                    response = request(
                        "GET",
                        forum + "/search.json",
                        params={"q": query, "page": page},
                        timeout=60,
                        attempts=3,
                        rate_key=f"discourse-{forum}",
                        min_interval=1.0,
                    )
                    if response.status_code in {403, 404, 429}:
                        errors.append(f"{forum}: HTTP {response.status_code}")
                        break
                    response.raise_for_status()
                    payload = response.json()
                    if record(payload, "discourse_forum_search", ref=response.url):
                        observations += 1
                    posts = payload.get("posts", [])
                    topics = payload.get("topics", [])
                    if not posts and not topics:
                        break
                except Exception as exc:
                    errors.append(f"{forum}/{query}: {exc!r}")
                    break
    collector.source_status("discourse_forum_search", observations > 0 or not errors, observations, " | ".join(errors[:10]) or None, forums=len(forums))


@stage("gdelt_referring_articles")
def ingest_gdelt() -> None:
    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"
    observations = 0
    errors: list[str] = []
    referring: set[str] = set()
    # Quarterly partitions prevent result-window saturation.
    for year in (2023, 2024, 2025, 2026):
        for month in (1, 4, 7, 10):
            start = f"{year}{month:02d}01000000"
            end_month = month + 3
            end_year = year
            if end_month > 12:
                end_month -= 12
                end_year += 1
            end = f"{end_year}{end_month:02d}01000000"
            for query in ('"chatgpt.com/share/"', '"chat.openai.com/share/"'):
                try:
                    response = request(
                        "GET",
                        endpoint,
                        params={"query": query, "mode": "artlist", "maxrecords": 250, "format": "json", "startdatetime": start, "enddatetime": end},
                        timeout=120,
                        attempts=5,
                        rate_key="gdelt",
                        min_interval=3.0,
                    )
                    if response.status_code == 429:
                        errors.append(f"{year}-{month}: 429")
                        continue
                    response.raise_for_status()
                    payload = response.json()
                    record(payload, "gdelt_referring_articles", ref=response.url)
                    for article in payload.get("articles", []):
                        if article.get("url"):
                            referring.add(str(article["url"]))
                except Exception as exc:
                    errors.append(f"{year}-{month}/{query}: {exc!r}")

    # Fetch article HTML transiently to extract the actual embedded share URL.
    def fetch_article(url: str) -> tuple[int, str | None]:
        try:
            response = request("GET", url, timeout=45, attempts=2, rate_key="article", min_interval=0.02)
            if response.status_code >= 400 or len(response.content) > 8_000_000:
                return 0, None
            candidates = extract_urls(response.content)
            for candidate in candidates:
                record(candidate, "gdelt_referring_articles", ref=url)
            return len(candidates), None
        except Exception as exc:
            return 0, repr(exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        for count, error in pool.map(fetch_article, list(referring)[:5000]):
            observations += count
            if error:
                errors.append(error)
    collector.source_status(
        "gdelt_referring_articles",
        observations > 0 or not errors,
        observations,
        " | ".join(errors[:10]) or None,
        referring_articles=len(referring),
        fetched_articles=min(len(referring), 5000),
    )


# ---------------------------------------------------------------------------
# Finalization
# ---------------------------------------------------------------------------


def write_independent_search_plan(path: Path) -> None:
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
        "Startpage": "https://www.startpage.com/do/search?q=",
        "Yep": "https://yep.com/web?q=",
        "Marginalia": "https://search.marginalia.nu/search?query=",
        "Wiby": "https://wiby.me/?q=",
        "Mwmbl": "https://mwmbl.org/?q=",
    }
    rows = []
    for domain in DOMAINS:
        for engine, base in engines.items():
            for first in HEX:
                for second in HEX:
                    prefix = first + second
                    query = f"site:{domain}/share/{prefix}"
                    rows.append({
                        "domain_variant": domain,
                        "engine": engine,
                        "uuid_prefix": prefix,
                        "query": query,
                        "search_url": base + quote_plus(query),
                        "execution_status": "query_plan_not_counted_as_discovery",
                    })
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def finalize() -> int:
    forbidden = [name for name in collector.status if name in FORBIDDEN_STAGE_NAMES or "huggingface" in name or "sharechat" in name]
    if forbidden:
        raise RuntimeError(f"Forbidden dataset-derived stages executed: {forbidden}")

    data = collector.rows()
    summary = collector.make_summary(data)
    summary.update({
        "run_profile": "independent_public_discovery_v2_no_huggingface_import",
        "huggingface_or_prior_inventory_used_as_input": False,
        "candidate_filtering_against_prior_inventory": "performed after artifact download, outside this runner",
        "executed_stage_count": len(collector.status),
        "executed_stages": sorted(collector.status),
    })

    collector.write_csv_gz(OUT / "chatgpt-independent-candidates.csv.gz", data)
    collector.write_parquet(OUT / "chatgpt-independent-candidates.parquet", data)
    (OUT / "chatgpt-independent-candidate-urls.txt").write_text(
        "\n".join(row["url"] for row in data) + ("\n" if data else ""), encoding="utf-8"
    )
    collector.write_observations(OUT / "source-observations.ndjson.gz")
    write_independent_search_plan(OUT / "search-engine-query-plan.csv")
    (OUT / "coverage-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT / "source-status.json").write_text(json.dumps(collector.status, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT / "methodology.md").write_text(
        "# Independent ChatGPT Share URL Discovery v2\n\n"
        f"Generated: `{RUN_AT}`\n\n"
        "This run does not read Hugging Face, ShareChat, ShareGPT-X, or any prior URL inventory. "
        "It enumerates public archive URL indexes, mines archived/current referring pages, searches "
        "public code and developer-discussion indexes, distributes metasearch over public SearXNG "
        "instances, and queries public social/forum/threat-intelligence APIs.\n\n"
        "Conversation bodies are not retained. URLs already present in the previous Hugging Face-derived "
        "inventory are removed only after this artifact is downloaded, so they cannot influence discovery.\n\n"
        "## Executed sources\n\n"
        + "\n".join(f"- `{name}`" for name in sorted(collector.status))
        + "\n\n## Summary\n\n```json\n"
        + json.dumps(summary, ensure_ascii=False, indent=2)
        + "\n```\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


def main() -> int:
    stages: list[Callable[[], None]] = [
        ingest_wayback_partitioned,
        ingest_commoncrawl_cdx,
        ingest_arquivo_pt,
        ingest_github_code,
        ingest_github_issues,
        ingest_github_commits,
        ingest_sourcegraph,
        ingest_grep_app,
        ingest_searxng,
        ingest_duckduckgo,
        ingest_bing,
        ingest_mojeek,
        ingest_qwant,
        ingest_urlscan_paginated,
        ingest_otx,
        ingest_bluesky,
        ingest_reddit_public,
        ingest_arctic_shift,
        ingest_hackernews,
        ingest_stackexchange,
        ingest_lemmy,
        ingest_discourse,
        ingest_gdelt,
    ]
    for fn in stages:
        fn()
    return finalize()


if __name__ == "__main__":
    raise SystemExit(main())
