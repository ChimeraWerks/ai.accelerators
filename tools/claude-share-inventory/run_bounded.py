#!/usr/bin/env python3
"""Run the inventory collector with bounded external-source latency."""

from __future__ import annotations

import os

import build_inventory as collector

_original_get = collector.get


def bounded_get(url, *, params=None, headers=None, timeout=120, attempts=4):
    return _original_get(
        url,
        params=params,
        headers=headers,
        timeout=min(int(timeout), 75),
        attempts=min(int(attempts), 2),
    )


collector.get = bounded_get


def bounded_github_code_search() -> None:
    name = "github_public_code_search"
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        collector.source_status(name, False, error="GITHUB_TOKEN unavailable")
        return
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.text-match+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    count = 0
    try:
        response = collector.get(
            "https://api.github.com/search/code",
            headers=headers,
            params={"q": '"claude.ai/share/"', "per_page": 100, "page": 1},
            timeout=60,
            attempts=2,
        )
        response.raise_for_status()
        for result in response.json().get("items", []):
            ref = result.get("html_url")
            repo = result.get("repository", {}).get("full_name")
            for match in result.get("text_matches", []) or []:
                for found in collector.UUID_RE.finditer(str(match.get("fragment", ""))):
                    item = collector.item_for(found.group(0))
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
        collector.source_status(name, True, count, query_mode="single_bounded_query")
    except Exception as exc:
        collector.source_status(name, False, count, repr(exc), query_mode="single_bounded_query")


collector.ingest_github_code_search = bounded_github_code_search

if __name__ == "__main__":
    raise SystemExit(collector.main())
