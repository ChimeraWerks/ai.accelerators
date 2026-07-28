#!/usr/bin/env python3
"""Run one isolated source group for independent ChatGPT share discovery."""
from __future__ import annotations

import json
import os

import build_inventory as collector
import run_independent_v2 as run

GROUPS = {
    "archives": [
        run.ingest_wayback_partitioned,
        run.ingest_commoncrawl_cdx,
        run.ingest_arquivo_pt,
    ],
    "code": [
        run.ingest_github_code,
        run.ingest_github_issues,
        run.ingest_github_commits,
        run.ingest_sourcegraph,
        run.ingest_grep_app,
    ],
    "search": [
        run.ingest_searxng,
        run.ingest_duckduckgo,
        run.ingest_bing,
        run.ingest_mojeek,
        run.ingest_qwant,
    ],
    "social": [
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
    ],
}


def checkpoint(group: str, stage_name: str) -> None:
    data = collector.rows()
    summary = collector.make_summary(data)
    summary.update({
        "run_profile": "independent_public_discovery_v2_source_group",
        "source_group": group,
        "checkpoint_after_stage": stage_name,
        "huggingface_or_prior_inventory_used_as_input": False,
    })
    (collector.OUT / "chatgpt-independent-candidate-urls.txt").write_text(
        "\n".join(row["url"] for row in data) + ("\n" if data else ""),
        encoding="utf-8",
    )
    collector.write_csv_gz(collector.OUT / "chatgpt-independent-candidates.csv.gz", data)
    collector.write_parquet(collector.OUT / "chatgpt-independent-candidates.parquet", data)
    collector.write_observations(collector.OUT / "source-observations.ndjson.gz")
    (collector.OUT / "coverage-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (collector.OUT / "source-status.json").write_text(
        json.dumps(collector.status, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main() -> int:
    group = os.environ.get("DISCOVERY_GROUP", "").strip().lower()
    if group not in GROUPS:
        raise SystemExit(f"Unknown DISCOVERY_GROUP={group!r}; expected one of {sorted(GROUPS)}")
    for fn in GROUPS[group]:
        fn()
        checkpoint(group, fn.__name__)
    result = run.finalize()
    summary_path = collector.OUT / "coverage-summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["source_group"] = group
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
