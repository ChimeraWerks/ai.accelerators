#!/usr/bin/env python3
"""Produce the guaranteed core inventory from the maintained public archive."""

from __future__ import annotations

import json

import build_inventory as collector


def main() -> int:
    collector.ingest_public_archive()
    for name in [
        "huggingface_sharechat_derivative",
        "wayback_cdx",
        "common_crawl",
        "urlscan_existing_records",
        "github_public_code_search",
    ]:
        collector.source_status(
            name,
            False,
            error="Not executed in guaranteed core run; see the bounded multi-index run.",
        )

    data = collector.rows()
    summary = collector.make_summary(data)
    collector.write_csv_gz(collector.OUT / "claude-share-inventory.csv.gz", data)
    collector.write_parquet(collector.OUT / "claude-share-inventory.parquet", data)
    (collector.OUT / "claude-share-urls.txt").write_text(
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
    (collector.OUT / "methodology.md").write_text(
        f"""# Claude Share URL Inventory — Core Snapshot

Generated: `{collector.RUN_AT}`

This guaranteed snapshot is built from the maintained public
`fuadmefleh/Shared-Claude-Chats` repository. Every row includes the full canonical
`https://claude.ai/share/<uuid>` URL. The source provides a current link list plus a
manifest and Markdown snapshot for links that successfully exported while public.

Conversation bodies were processed in memory only to calculate byte, character, word,
code-block, token-heuristic, and hash metadata; they are not included in the outputs.
No IDs were guessed, no access controls were bypassed, and no live Claude validation
was performed.

The separate bounded multi-index run covers ShareChat, Wayback, Common Crawl,
urlscan.io, and GitHub code search. `search-engine-query-plan.csv` supplies 1,536
clickable two-hex-prefix searches across Google, Bing, Brave, DuckDuckGo, Mojeek, and
Yandex.

```json
{json.dumps(summary, ensure_ascii=False, indent=2)}
```
""",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
