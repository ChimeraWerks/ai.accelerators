#!/usr/bin/env python3
"""Extract metadata-only length statistics from the full DevGPT Zenodo release.

Downloads the public Zenodo v10 ZIP to temporary storage and scans sharing JSON files.
Only URL, historical status, date, model, prompt count, prompt tokens, and answer tokens
are retained. Conversations and HTMLContent are never copied to output.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Iterable

import requests

import build_inventory as collector

SOURCE = "zenodo_devgpt_v10_metrics"
DOWNLOAD = "https://zenodo.org/records/16392320/files/DevGPT.zip?download=1"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "ChimeraWerks-ChatGPTShareIndependentDiscovery/2.0",
    "Accept": "application/zip,*/*",
})


def walk(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        # DevGPT uses URL inside ChatgptSharing records.
        if value.get("URL") and any(
            key in value for key in (
                "NumberOfPrompts", "TokensOfPrompts", "TokensOfAnswers",
                "TokenOfPrompts", "TokenOfAnswers", "DateOfConversation",
                "Model", "Conversations", "HTMLContent",
            )
        ):
            yield value
        for key, child in value.items():
            if key in {"Conversations", "HTMLContent"}:
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


def download_archive(path: Path) -> int:
    response = SESSION.get(DOWNLOAD, timeout=(60, 900), stream=True)
    response.raise_for_status()
    size = 0
    with path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            if chunk:
                handle.write(chunk)
                size += len(chunk)
    return size


def main() -> int:
    records_seen = 0
    metric_records = 0
    files_scanned = 0
    parse_errors: list[str] = []

    with tempfile.TemporaryDirectory(prefix="devgpt-") as temp_dir:
        archive_path = Path(temp_dir) / "DevGPT.zip"
        archive_bytes = download_archive(archive_path)
        with zipfile.ZipFile(archive_path) as archive:
            names = [
                name for name in archive.namelist()
                if name.lower().endswith(".json") and "sharing" in name.lower()
            ]
            for name in names:
                files_scanned += 1
                try:
                    with archive.open(name) as stream:
                        payload = json.load(stream)
                except Exception as exc:
                    parse_errors.append(f"{name}: {exc!r}")
                    continue
                snapshot_match = re.search(r"snapshot[_-]?(\d{8})", name, re.IGNORECASE)
                snapshot = snapshot_match.group(1) if snapshot_match else Path(name).parts[0]
                for record in walk(payload):
                    records_seen += 1
                    item = collector.item_for(record.get("URL"))
                    if not item:
                        continue
                    item.sources.add(SOURCE)
                    metric_records += 1

                    snapshots = set(str(item.values.get("devgpt_metric_snapshots", "")).split(";")) - {""}
                    snapshots.add(snapshot)
                    item.values["devgpt_metric_snapshots"] = ";".join(sorted(snapshots))

                    prompt_count = as_int(record.get("NumberOfPrompts"))
                    prompt_tokens = as_int(record.get("TokensOfPrompts"))
                    if prompt_tokens is None:
                        prompt_tokens = as_int(record.get("TokenOfPrompts"))
                    answer_tokens = as_int(record.get("TokensOfAnswers"))
                    if answer_tokens is None:
                        answer_tokens = as_int(record.get("TokenOfAnswers"))
                    total_tokens = None
                    if prompt_tokens is not None or answer_tokens is not None:
                        total_tokens = (prompt_tokens or 0) + (answer_tokens or 0)

                    for key, value in (
                        ("devgpt_prompt_count", prompt_count),
                        ("devgpt_prompt_tokens", prompt_tokens),
                        ("devgpt_answer_tokens", answer_tokens),
                        ("devgpt_total_tokens", total_tokens),
                    ):
                        if value is not None:
                            previous = as_int(item.values.get(key))
                            if previous is None or value > previous:
                                item.values[key] = value

                    if record.get("Model"):
                        models = set(str(item.values.get("devgpt_models", "")).split(";")) - {""}
                        models.add(str(record["Model"]))
                        item.values["devgpt_models"] = ";".join(sorted(models))
                    if record.get("DateOfConversation"):
                        item.values.setdefault("conversation_created", record.get("DateOfConversation"))
                    if record.get("DateOfAccess"):
                        item.values["devgpt_last_access_date"] = record.get("DateOfAccess")
                    status = as_int(record.get("Status"))
                    if status is not None:
                        previous = as_int(item.values.get("devgpt_best_historical_status"))
                        if previous is None or (previous != 200 and status == 200):
                            item.values["devgpt_best_historical_status"] = status

    data = collector.rows()
    for row in data:
        total_tokens = as_int(row.get("devgpt_total_tokens"))
        prompt_count = as_int(row.get("devgpt_prompt_count"))
        if total_tokens is not None:
            row["best_estimated_tokens"] = total_tokens
            row["best_estimated_tokens_method"] = "devgpt_recorded_prompt_plus_answer_tokens"
            row["best_measurement_basis"] = "zenodo_devgpt_recorded_metadata"
            row["measurement_confidence"] = "high_recorded_token_counts"
            row["best_word_count_estimate"] = round(total_tokens * 0.75)
            row["best_word_count_method"] = "token_count_times_0_75_word_equivalent"
            row["best_word_count_confidence"] = "low_token_to_word_conversion"
        if prompt_count is not None:
            row["best_message_count"] = prompt_count * 2
            row["best_message_count_basis"] = "prompt_count_times_two"
        row["historically_observed_public"] = row.get("devgpt_best_historical_status") == 200
        row["current_validity"] = "not_live_checked"
        row["current_validity_reason"] = "Historical DevGPT metadata; current availability not inferred."

    collector.source_status(
        SOURCE,
        len(data) > 0,
        metric_records,
        " | ".join(parse_errors[:12]) or None,
        zenodo_record="https://zenodo.org/records/16392320",
        archive_bytes=archive_bytes,
        sharing_json_files=files_scanned,
        nested_records_seen=records_seen,
        metric_records=metric_records,
        unique_urls=len(data),
        policy="Conversations and HTMLContent skipped; aggregate metadata only",
    )

    summary = collector.make_summary(data)
    summary.update({
        "run_profile": "independent_zenodo_devgpt_metadata_only_enrichment",
        "huggingface_or_prior_inventory_used_as_input": False,
        "conversation_bodies_retained": False,
        "devgpt_metric_records": metric_records,
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
        "# Zenodo DevGPT Metadata-Only Length Enrichment\n\n"
        "The full public DevGPT v10 ZIP was downloaded from Zenodo. Sharing JSON files "
        "were scanned for URL, status, dates, model, prompt count, prompt tokens, and "
        "answer tokens. `Conversations` and `HTMLContent` were skipped and are absent "
        "from all outputs.\n\nNo Hugging Face or prior inventory was used as input.\n\n"
        "```json\n" + json.dumps(summary, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
