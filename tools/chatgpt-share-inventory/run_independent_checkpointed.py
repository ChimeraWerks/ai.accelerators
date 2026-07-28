#!/usr/bin/env python3
"""Checkpointed orchestrator for independent ChatGPT share discovery."""
from __future__ import annotations

import json
import time
from pathlib import Path

import build_inventory as collector
import run_independent_v2 as run


def checkpoint(index: int, stage_name: str) -> None:
    data = collector.rows()
    summary = collector.make_summary(data)
    summary.update({
        "run_profile": "independent_public_discovery_v2_no_huggingface_import",
        "checkpoint_after_stage_index": index,
        "checkpoint_after_stage": stage_name,
        "huggingface_or_prior_inventory_used_as_input": False,
    })
    (collector.OUT / "chatgpt-independent-candidate-urls.txt").write_text(
        "\n".join(row["url"] for row in data) + ("\n" if data else ""),
        encoding="utf-8",
    )
    (collector.OUT / "coverage-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (collector.OUT / "source-status.json").write_text(
        json.dumps(collector.status, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    # Full typed outputs every fourth stage, plus the first stage.
    if index == 1 or index % 4 == 0:
        collector.write_csv_gz(collector.OUT / "chatgpt-independent-candidates.csv.gz", data)
        collector.write_parquet(collector.OUT / "chatgpt-independent-candidates.parquet", data)
        collector.write_observations(collector.OUT / "source-observations.ndjson.gz")
    run.log("checkpoint", "written", stage_index=index, stage=stage_name, unique_urls=len(data))


def main() -> int:
    stages = [
        run.ingest_wayback_partitioned,
        run.ingest_commoncrawl_cdx,
        run.ingest_arquivo_pt,
        run.ingest_github_code,
        run.ingest_github_issues,
        run.ingest_github_commits,
        run.ingest_sourcegraph,
        run.ingest_grep_app,
        run.ingest_searxng,
        run.ingest_duckduckgo,
        run.ingest_bing,
        run.ingest_mojeek,
        run.ingest_qwant,
        run.ingest_urlscan_paginated,
        run.ingest_otx,
        run.ingest_bluesky,
        run.ingest_reddit_public,
        run.ingest_arctic_shift,
        run.ingest_hackernews,
        run.ingest_stackexchange,
        run.ingest_lemmy,
        run.ingest_discourse,
        run.ingest_gdelt,
    ]
    for index, fn in enumerate(stages, start=1):
        fn()
        checkpoint(index, fn.__name__)
    return run.finalize()


if __name__ == "__main__":
    raise SystemExit(main())
