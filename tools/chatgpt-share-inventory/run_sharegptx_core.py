#!/usr/bin/env python3
"""ShareGPT-X-first ChatGPT share inventory fallback.

Produces a large measured URL inventory without waiting for the 2.47 GB cross-platform
ShareChat derivative or archive-wide crawl endpoints.
"""
from __future__ import annotations

import json
import time

import build_inventory as collector


# Hugging Face's converted configuration is named Simple-ShareGPT. The original
# collector intentionally keeps source-specific logic centralized, so adapt the
# config name at this runner boundary without duplicating the ingestion function.
_original_hf_parquet_files = collector.hf_parquet_files


def _patched_hf_parquet_files(dataset: str, config: str | None = None, split: str | None = None):
    if dataset == "DSULT-Core/ShareGPT-X" and config == "Simple":
        config = "Simple-ShareGPT"
    return _original_hf_parquet_files(dataset, config, split)


collector.hf_parquet_files = _patched_hf_parquet_files


def main() -> int:
    stages = [
        collector.ingest_sharegpt_x,
        collector.ingest_urlscan,
        collector.ingest_sourcegraph,
        collector.ingest_grep_app,
        collector.ingest_marginalia,
        collector.ingest_mwmbl,
        collector.ingest_hackernews,
        collector.ingest_stackexchange,
        collector.ingest_gdelt_references,
    ]
    for stage in stages:
        started = time.time()
        print(
            json.dumps({"stage": stage.__name__, "status": "start", "at": collector.now()}),
            flush=True,
        )
        stage()
        print(
            json.dumps(
                {
                    "stage": stage.__name__,
                    "status": "done",
                    "elapsed_seconds": round(time.time() - started, 2),
                    "unique_urls_so_far": len(collector.entries),
                }
            ),
            flush=True,
        )

    data = collector.rows()
    summary = collector.make_summary(data)
    summary["run_profile"] = "sharegpt_x_measured_core_plus_fast_free_indexes"
    summary["sharegpt_x_parquet_config"] = "Simple-ShareGPT/full"
    summary["omitted_from_fallback_run"] = [
        "huggingface_sharechat_derivative",
        "wayback_cdx",
        "common_crawl",
        "github_public_code_search",
        "reddit_public_search",
    ]

    collector.write_csv_gz(collector.OUT / "chatgpt-share-inventory.csv.gz", data)
    collector.write_parquet(collector.OUT / "chatgpt-share-inventory.parquet", data)
    (collector.OUT / "chatgpt-share-urls.txt").write_text(
        "\n".join(row["url"] for row in data) + "\n", encoding="utf-8"
    )
    collector.write_observations(collector.OUT / "source-observations.ndjson.gz")
    collector.write_search_plan(collector.OUT / "search-engine-query-plan.csv")
    (collector.OUT / "coverage-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (collector.OUT / "source-status.json").write_text(
        json.dumps(collector.status, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    collector.write_methodology(collector.OUT / "methodology.md", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
