#!/usr/bin/env python3
"""Run exactly one independent ChatGPT share discovery source adapter."""
from __future__ import annotations

import json
import os

import build_inventory as collector
# Importing group installs reversible URL decoding before source execution.
import run_independent_group as group_runner
import run_independent_v2 as run

STAGES = {
    "wayback": run.ingest_wayback_partitioned,
    "commoncrawl": run.ingest_commoncrawl_cdx,
    "arquivo": run.ingest_arquivo_pt,
    "github-code": run.ingest_github_code,
    "github-issues": run.ingest_github_issues,
    "github-commits": run.ingest_github_commits,
    "sourcegraph": run.ingest_sourcegraph,
    "grep-app": run.ingest_grep_app,
    "searxng": run.ingest_searxng,
    "duckduckgo": run.ingest_duckduckgo,
    "bing": run.ingest_bing,
    "mojeek": run.ingest_mojeek,
    "qwant": run.ingest_qwant,
    "urlscan": run.ingest_urlscan_paginated,
    "otx": run.ingest_otx,
    "bluesky": run.ingest_bluesky,
    "reddit": run.ingest_reddit_public,
    "arctic-shift": run.ingest_arctic_shift,
    "hackernews": run.ingest_hackernews,
    "stackexchange": run.ingest_stackexchange,
    "lemmy": run.ingest_lemmy,
    "discourse": run.ingest_discourse,
    "gdelt": run.ingest_gdelt,
}


def main() -> int:
    source = os.environ.get("DISCOVERY_SOURCE", "").strip().lower()
    if source not in STAGES:
        raise SystemExit(f"Unknown DISCOVERY_SOURCE={source!r}; expected one of {sorted(STAGES)}")

    STAGES[source]()
    data = collector.rows()
    summary = collector.make_summary(data)
    summary.update({
        "run_profile": "independent_public_discovery_v2_isolated_source",
        "isolated_source": source,
        "huggingface_or_prior_inventory_used_as_input": False,
        "encoded_url_expansion": "iterative percent, HTML, and JSON slash decoding",
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
        "# Independent ChatGPT Share Discovery — Isolated Source\n\n"
        f"Source adapter: `{source}`\n\n"
        "No Hugging Face, ShareChat, ShareGPT-X, or previous URL inventory was used as input. "
        "The adapter queried its public source directly and retained URLs plus provenance only.\n\n"
        "```json\n" + json.dumps(summary, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
