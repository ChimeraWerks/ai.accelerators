#!/usr/bin/env python3
"""Import cleaned DevChat URLs and non-content metadata from its public GitHub release.

DevChat independently collected ChatGPT share links from GitHub Code, Commits,
Issues, Pull Requests, and Discussions through June 2024. This adapter retains
URL, source metadata, dates, prompt counts, and token totals. Conversation turns
and HTML are skipped and never emitted.
"""
from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path
from typing import Any, Iterable

import requests

import build_inventory as collector

SOURCE = "github_ruiyinl_devchat"
REPO_ARCHIVE = "https://codeload.github.com/RuiyinL/DevChat/zip/refs/heads/main"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "ChimeraWerks-ChatGPTShareIndependentDiscovery/2.0",
    "Accept": "application/zip,*/*",
})


def walk(value: Any) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    """Yield (sharing record, containing artifact metadata) without copying content."""
    if isinstance(value, dict):
        sharing = value.get("ChatgptSharing")
        if isinstance(sharing, list):
            context = {
                key: value.get(key)
                for key in (
                    "Type", "URL", "Author", "Title", "RepoName", "RepoLanguage",
                    "CreatedAt", "AuthorAt", "State", "Number", "Path",
                )
                if value.get(key) is not None
            }
            for record in sharing:
                if isinstance(record, dict) and record.get("URL"):
                    yield record, context
        # Also support files containing sharing records directly.
        if value.get("URL") and any(
            key in value for key in (
                "NumberOfPrompts", "TokenOfPrompts", "TokenOfAnswers",
                "TokensOfPrompts", "TokensOfAnswers", "DateOfConversation", "Model",
            )
        ):
            yield value, {}
        for key, child in value.items():
            if key in {"ChatgptSharing", "Conversations", "HTMLContent"}:
                continue
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> int:
    response = SESSION.get(REPO_ARCHIVE, timeout=(60, 600))
    response.raise_for_status()
    files_scanned = 0
    sharing_records = 0
    parse_errors: list[str] = []

    with zipfile.ZipFile(io.BytesIO(response.content)) as repository:
        nested_zip_names = [name for name in repository.namelist() if name.endswith("/DevChat.zip")]
        if not nested_zip_names:
            raise RuntimeError("DevChat.zip was not present in repository archive")
        with zipfile.ZipFile(io.BytesIO(repository.read(nested_zip_names[0]))) as dataset:
            json_names = [name for name in dataset.namelist() if name.lower().endswith(".json")]
            for name in json_names:
                files_scanned += 1
                try:
                    with dataset.open(name) as stream:
                        payload = json.load(stream)
                except Exception as exc:
                    parse_errors.append(f"{name}: {exc!r}")
                    continue
                for sharing, context in walk(payload):
                    sharing_records += 1
                    item = collector.item_for(sharing.get("URL"))
                    if not item:
                        continue
                    item.sources.add(SOURCE)
                    item.values["devchat_dataset_file"] = name
                    if context.get("Type"):
                        types = set(str(item.values.get("devchat_github_types", "")).split(";")) - {""}
                        types.add(str(context["Type"]))
                        item.values["devchat_github_types"] = ";".join(sorted(types))
                    if context.get("URL"):
                        refs = set(str(item.values.get("public_references", "")).split(";")) - {""}
                        refs.add(str(context["URL"]))
                        item.values["public_references"] = ";".join(sorted(refs))
                    for src_key, dst_key in (
                        ("RepoName", "devchat_repositories"),
                        ("RepoLanguage", "devchat_repository_languages"),
                        ("Author", "devchat_authors"),
                    ):
                        if context.get(src_key):
                            values = set(str(item.values.get(dst_key, "")).split(";")) - {""}
                            values.add(str(context[src_key]))
                            item.values[dst_key] = ";".join(sorted(values))

                    prompt_count = as_int(sharing.get("NumberOfPrompts"))
                    prompt_tokens = as_int(sharing.get("TokenOfPrompts"))
                    if prompt_tokens is None:
                        prompt_tokens = as_int(sharing.get("TokensOfPrompts"))
                    answer_tokens = as_int(sharing.get("TokenOfAnswers"))
                    if answer_tokens is None:
                        answer_tokens = as_int(sharing.get("TokensOfAnswers"))
                    total_tokens = None
                    if prompt_tokens is not None or answer_tokens is not None:
                        total_tokens = (prompt_tokens or 0) + (answer_tokens or 0)
                    for key, value in (
                        ("devchat_prompt_count", prompt_count),
                        ("devchat_prompt_tokens", prompt_tokens),
                        ("devchat_answer_tokens", answer_tokens),
                        ("devchat_total_tokens", total_tokens),
                    ):
                        if value is not None:
                            previous = as_int(item.values.get(key))
                            if previous is None or value > previous:
                                item.values[key] = value
                    if sharing.get("Model"):
                        models = set(str(item.values.get("devchat_models", "")).split(";")) - {""}
                        models.add(str(sharing["Model"]))
                        item.values["devchat_models"] = ";".join(sorted(models))
                    if sharing.get("DateOfConversation"):
                        item.values.setdefault("conversation_created", sharing.get("DateOfConversation"))
                    if sharing.get("DateOfAccess"):
                        item.values["devchat_last_access_date"] = sharing.get("DateOfAccess")
                    if sharing.get("Title"):
                        item.values.setdefault("title", sharing.get("Title"))
                    status = as_int(sharing.get("Status"))
                    if status is not None:
                        previous = as_int(item.values.get("devchat_best_historical_status"))
                        if previous is None or (previous != 200 and status == 200):
                            item.values["devchat_best_historical_status"] = status

    data = collector.rows()
    for row in data:
        total_tokens = as_int(row.get("devchat_total_tokens"))
        prompt_count = as_int(row.get("devchat_prompt_count"))
        if total_tokens is not None:
            row["best_estimated_tokens"] = total_tokens
            row["best_estimated_tokens_method"] = "devchat_recorded_prompt_plus_answer_tokens"
            row["best_measurement_basis"] = "public_devchat_recorded_metadata"
            row["measurement_confidence"] = "high_recorded_token_counts"
            row["best_word_count_estimate"] = round(total_tokens * 0.75)
            row["best_word_count_method"] = "token_count_times_0_75_word_equivalent"
            row["best_word_count_confidence"] = "low_token_to_word_conversion"
        if prompt_count is not None:
            row["best_message_count"] = prompt_count * 2
            row["best_message_count_basis"] = "prompt_count_times_two"
        row["historically_observed_public"] = row.get("devchat_best_historical_status") == 200
        row["current_validity"] = "not_live_checked"
        row["current_validity_reason"] = "Cleaned DevChat historical record; current availability not inferred."

    collector.source_status(
        SOURCE,
        len(data) > 0,
        sharing_records,
        " | ".join(parse_errors[:12]) or None,
        repository="https://github.com/RuiyinL/DevChat",
        repository_archive_bytes=len(response.content),
        dataset_json_files=files_scanned,
        sharing_records=sharing_records,
        unique_urls=len(data),
        policy="Conversations and HTMLContent skipped; URL and aggregate metadata only",
    )

    summary = collector.make_summary(data)
    summary.update({
        "run_profile": "independent_ruiyinl_devchat_metadata_only",
        "huggingface_or_prior_inventory_used_as_input": False,
        "conversation_bodies_retained": False,
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
        "# Independent DevChat Source\n\n"
        "The cleaned DevChat.zip release was downloaded directly from RuiyinL/DevChat. "
        "URLs and non-content metadata were retained; Conversations and HTMLContent were "
        "explicitly skipped. No Hugging Face or prior Chimera inventory was used as input.\n\n"
        "```json\n" + json.dumps(summary, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
