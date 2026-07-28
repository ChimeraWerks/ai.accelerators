#!/usr/bin/env python3
"""Import independently collected DevGPT/GPTShare URL snapshots from GitHub.

This source is not Hugging Face and is not a prior Chimera inventory. DevGPT's
researchers independently harvested ChatGPT share-link mentions from GitHub and
Hacker News in 2023. Only URLs, historical HTTP status, and mention provenance are
retained here; conversation HTML/text is not downloaded.
"""
from __future__ import annotations

import csv
import io
import json
from collections import Counter

import requests

import build_inventory as collector

SNAPSHOTS = [
    "snapshot_20230727",
    "snapshot_20230803",
    "snapshot_20230810",
    "snapshot_20230817",
    "snapshot_20230824",
    "snapshot_20230831",
    "snapshot_20230907",
    "snapshot_20230914",
    "snapshot_20231012",
]
SOURCE = "github_devgpt_gptshare_snapshots"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "ChimeraWerks-ChatGPTShareIndependentDiscovery/2.0",
    "Accept": "text/csv,text/plain,*/*",
})


def main() -> int:
    raw_rows = 0
    accepted_rows = 0
    snapshot_counts = Counter()
    status_counts = Counter()
    errors = []

    for snapshot in SNAPSHOTS:
        url = (
            "https://raw.githubusercontent.com/NAIST-SE/DevGPT/main/"
            f"{snapshot}/ChatGPT_Link_Sharing.csv"
        )
        try:
            response = SESSION.get(url, timeout=180)
            response.raise_for_status()
            reader = csv.DictReader(io.StringIO(response.text))
            for row in reader:
                raw_rows += 1
                item = collector.item_for(row.get("URL"))
                if not item:
                    continue
                accepted_rows += 1
                snapshot_counts[snapshot] += 1
                status = collector.as_int(row.get("Status"))
                if status is not None:
                    status_counts[str(status)] += 1
                item.sources.add(SOURCE)

                snapshots = set(str(item.values.get("devgpt_snapshots", "")).split(";")) - {""}
                snapshots.add(snapshot)
                item.values["devgpt_snapshots"] = ";".join(sorted(snapshots))

                mention_refs = set(str(item.values.get("public_references", "")).split(";")) - {""}
                if row.get("MentionedURL"):
                    mention_refs.add(str(row["MentionedURL"]))
                item.values["public_references"] = ";".join(sorted(mention_refs))

                mention_sources = set(str(item.values.get("devgpt_mention_sources", "")).split(";")) - {""}
                if row.get("MentionedSource"):
                    mention_sources.add(str(row["MentionedSource"]))
                item.values["devgpt_mention_sources"] = ";".join(sorted(mention_sources))

                properties = set(str(item.values.get("devgpt_mentioned_properties", "")).split(";")) - {""}
                if row.get("MentionedProperty"):
                    properties.add(str(row["MentionedProperty"]))
                item.values["devgpt_mentioned_properties"] = ";".join(sorted(properties))

                authors = set(str(item.values.get("devgpt_mentioned_authors", "")).split(";")) - {""}
                if row.get("MentionedAuthor"):
                    authors.add(str(row["MentionedAuthor"]))
                item.values["devgpt_mentioned_authors"] = ";".join(sorted(authors))

                if status is not None:
                    previous = collector.as_int(item.values.get("devgpt_best_historical_status"))
                    if previous is None or (previous != 200 and status == 200):
                        item.values["devgpt_best_historical_status"] = status
                item.values["devgpt_historically_observed"] = True
        except Exception as exc:
            errors.append(f"{snapshot}: {exc!r}")

    data = collector.rows()
    for row in data:
        if row.get("devgpt_best_historical_status") == 200:
            row["historically_observed_public"] = True
        row["current_validity"] = "not_live_checked"
        row["current_validity_reason"] = (
            "DevGPT recorded historical status in 2023; current availability was not inferred."
        )

    collector.source_status(
        SOURCE,
        accepted_rows > 0,
        accepted_rows,
        " | ".join(errors) or None,
        repository="https://github.com/NAIST-SE/DevGPT",
        snapshots_attempted=len(SNAPSHOTS),
        snapshot_row_counts=dict(snapshot_counts),
        raw_rows=raw_rows,
        unique_urls=len(data),
        historical_status_counts=dict(status_counts),
        policy="URLs/status/provenance only; no conversation HTML or body imported",
    )

    summary = collector.make_summary(data)
    summary.update({
        "run_profile": "independent_devgpt_gptshare_snapshots",
        "huggingface_or_prior_inventory_used_as_input": False,
        "source_repository": "https://github.com/NAIST-SE/DevGPT",
        "snapshot_count": len(SNAPSHOTS),
    })

    collector.write_csv_gz(collector.OUT / "chatgpt-independent-candidates.csv.gz", data)
    collector.write_parquet(collector.OUT / "chatgpt-independent-candidates.parquet", data)
    (collector.OUT / "chatgpt-independent-candidate-urls.txt").write_text(
        "\n".join(row["url"] for row in data) + ("\n" if data else ""), encoding="utf-8"
    )
    (collector.OUT / "coverage-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (collector.OUT / "source-status.json").write_text(
        json.dumps(collector.status, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (collector.OUT / "methodology.md").write_text(
        "# Independent ChatGPT Share Discovery — DevGPT/GPTShare\n\n"
        "Source: https://github.com/NAIST-SE/DevGPT\n\n"
        "Nine dated 2023 ChatGPT_Link_Sharing.csv snapshots were downloaded directly from "
        "the public GitHub repository. The source project independently harvested mentions "
        "from GitHub issues, pull requests, discussions, commits, code files, and Hacker News.\n\n"
        "No Hugging Face dataset or previous Chimera URL inventory was used as input. Only "
        "share URLs, historical HTTP status, and public mention provenance were retained.\n\n"
        "```json\n" + json.dumps(summary, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
