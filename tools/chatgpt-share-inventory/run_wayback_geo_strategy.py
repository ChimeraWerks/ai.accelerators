#!/usr/bin/env python3
"""Independent Wayback discovery using the proven Public-GEO query strategy.

Unlike the earlier prefix/collapse query, this adapter uses wildcard URL patterns,
explicit monthly date windows, duplicate CDX filters, and local canonical dedupe.
The strategy is adapted from Qingbolan/Public-GEO-DataSet's public collector.
"""
from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime
from urllib.parse import urlencode

import requests

import build_inventory as collector

SOURCE = "wayback_date_partitioned_wildcard"
ENDPOINT = "https://web.archive.org/cdx/search/cdx"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "ChimeraWerks-ChatGPTShareIndependentDiscovery/2.0",
    "Accept": "application/json,text/plain,*/*",
})


def month_windows(start: str, end: str):
    year = int(start[:4])
    month = int(start[4:6])
    end_year = int(end[:4])
    end_month = int(end[4:6])
    while (year, month) <= (end_year, end_month):
        next_year, next_month = year, month + 1
        if next_month == 13:
            next_year, next_month = year + 1, 1
        yield f"{year:04d}{month:02d}", f"{next_year:04d}{next_month:02d}"
        year, month = next_year, next_month


def get_json(url: str, attempts: int = 6):
    last = None
    for attempt in range(attempts):
        try:
            response = SESSION.get(url, timeout=240)
            if response.status_code in {429, 500, 502, 503, 504} and attempt + 1 < attempts:
                time.sleep(min(60, 2 ** attempt))
                continue
            response.raise_for_status()
            return response.json(), response.url
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"CDX query failed: {last!r}")


def main() -> int:
    patterns = [
        ("chat.openai.com/share/*", "202305", "202506"),
        ("chatgpt.com/share/*", "202404", "202607"),
    ]
    raw_rows = 0
    accepted_rows = 0
    errors = []
    windows_with_hits = Counter()

    for pattern, start, end in patterns:
        for date_from, date_to in month_windows(start, end):
            params = [
                ("url", pattern),
                ("from", date_from),
                ("to", date_to),
                ("output", "json"),
                ("fl", "timestamp,original,statuscode,mimetype,digest,length"),
                ("filter", "statuscode:200"),
                ("filter", "mimetype:text/html"),
                ("limit", "100000"),
            ]
            query_url = f"{ENDPOINT}?{urlencode(params)}"
            try:
                payload, final_url = get_json(query_url)
                if not payload:
                    continue
                header, *rows = payload
                raw_rows += len(rows)
                if rows:
                    windows_with_hits[f"{pattern}:{date_from}"] += len(rows)
                for values in rows:
                    if not isinstance(values, list):
                        continue
                    observation = dict(zip(header, values))
                    item = collector.item_for(observation.get("original"))
                    if not item:
                        continue
                    accepted_rows += 1
                    item.sources.add(SOURCE)
                    timestamps = set(str(item.values.get("wayback_timestamps", "")).split(";")) - {""}
                    if observation.get("timestamp"):
                        timestamps.add(str(observation["timestamp"]))
                    item.values["wayback_timestamps"] = ";".join(sorted(timestamps))
                    item.values["wayback_first_seen"] = min(timestamps) if timestamps else None
                    item.values["wayback_last_seen"] = max(timestamps) if timestamps else None
                    item.values["wayback_http_status"] = collector.as_int(observation.get("statuscode"))
                    item.values["wayback_mimetype"] = observation.get("mimetype")
                    item.values["wayback_digest"] = observation.get("digest")
                    item.values["wayback_compressed_record_bytes"] = collector.as_int(observation.get("length"))
                    item.values["wayback_query_window"] = f"{date_from}-{date_to}"
                    item.values["wayback_archive_url"] = (
                        f"https://web.archive.org/web/{observation.get('timestamp')}/{observation.get('original')}"
                    )
                print(json.dumps({
                    "pattern": pattern,
                    "from": date_from,
                    "to": date_to,
                    "rows": len(rows),
                    "unique_total": len(collector.entries),
                }), flush=True)
            except Exception as exc:
                errors.append(f"{pattern}/{date_from}-{date_to}: {exc!r}")
            time.sleep(0.35)

    data = collector.rows()
    for row in data:
        row["historically_observed_public"] = True
        row["current_validity"] = "not_live_checked"
        row["current_validity_reason"] = "Wayback historical 200 HTML capture; current availability not inferred."

    collector.source_status(
        SOURCE,
        len(data) > 0,
        accepted_rows,
        " | ".join(errors[:20]) or None,
        strategy="wildcard route + monthly date windows + status/mimetype filters + local dedupe",
        adapted_from="https://github.com/Qingbolan/Public-GEO-DataSet",
        raw_cdx_rows=raw_rows,
        unique_urls=len(data),
        windows_with_hits=dict(windows_with_hits),
        failed_windows=len(errors),
    )

    summary = collector.make_summary(data)
    summary.update({
        "run_profile": "independent_wayback_public_geo_strategy",
        "huggingface_or_prior_inventory_used_as_input": False,
        "query_strategy_source": "https://github.com/Qingbolan/Public-GEO-DataSet",
        "raw_cdx_rows": raw_rows,
    })

    collector.write_csv_gz(collector.OUT / "chatgpt-independent-candidates.csv.gz", data)
    collector.write_parquet(collector.OUT / "chatgpt-independent-candidates.parquet", data)
    (collector.OUT / "chatgpt-independent-candidate-urls.txt").write_text(
        "\n".join(row["url"] for row in data) + ("\n" if data else ""), encoding="utf-8"
    )
    collector.write_observations(collector.OUT / "source-observations.ndjson.gz")
    (collector.OUT / "coverage-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (collector.OUT / "source-status.json").write_text(
        json.dumps(collector.status, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (collector.OUT / "methodology.md").write_text(
        "# Independent Wayback Discovery — Date-Partitioned Wildcard Strategy\n\n"
        "This run uses wildcard CDX URL patterns, explicit monthly windows, statuscode:200 "
        "and mimetype:text/html filters, and local deduplication. It is adapted from the "
        "working Public-GEO-DataSet collector rather than the earlier failed prefix/collapse query.\n\n"
        "No Hugging Face dataset or previous URL inventory was used as input.\n\n"
        "```json\n" + json.dumps(summary, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
