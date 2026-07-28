#!/usr/bin/env python3
"""Extract non-content length metadata from public DevGPT/GPTShare snapshots.

Downloads the public DevGPT repository archive, reads its sharing JSON files, and
retains only URL, status/date/model, prompt count, and token totals. Conversation
turns and HTMLContent are explicitly discarded and never emitted.
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

SOURCE = "github_devgpt_gptshare_metrics"
ARCHIVE = "https://codeload.github.com/NAIST-SE/DevGPT/zip/refs/heads/main"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "ChimeraWerks-ChatGPTShareIndependentDiscovery/2.0",
    "Accept": "application/zip,*/*",
})


def walk(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        if value.get("URL") and any(
            key in value
            for key in (
                "NumberOfPrompts", "TokensOfPrompts", "TokensOfAnswers",
                "DateOfConversation", "Model", "Conversations", "HTMLContent",
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


def main() -> int:
    response = SESSION.get(ARCHIVE, timeout=600)
    response.raise_for_status()
    records_seen = 0
    metric_records = 0
    files_scanned = 0
    parse_errors: list[str] = []

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        names = [
            name for name in archive.namelist()
            if name.endswith("_sharing.json") and "/snapshot_" in name
        ]
        for name in names:
            files_scanned += 1
            try:
                payload = json.loads(archive.read(name).decode("utf-8", errors="replace"))
            except Exception as exc:
                parse_errors.append(f"{name}: {exc!r}")
                continue
            snapshot_match = re.search(r"/snapshot_(\d{8})/", name)
            snapshot = snapshot_match.group(1) if snapshot_match else Path(name).parts[-2]
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
                answer_tokens = as_int(record.get("TokensOfAnswers"))
                total_tokens = None
                if prompt_tokens is not None or answer_tokens is not None:
                    total_tokens = (prompt_tokens or 0) + (answer_tokens or 0)

                # Retain the richest/highest observed snapshot for each metric.
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
            row["best_measurement_basis"] = "public_devgpt_historical_metadata"
            row["measurement_confidence"] = "high_recorded_token_counts"
            # Word-equivalent estimate is explicitly approximate; retain tokens as primary.
            row["best_word_count_estimate"] = round(total_tokens * 0.75)
            row["best_word_count_method"] = "devgpt_tokens_times_0_75_word_equivalent"
            row["best_word_count_confidence"] = "low_token_to_word_conversion"
        if prompt_count is not None:
            row["best_message_count"] = prompt_count * 2
            row["best_message_count_basis"] = "devgpt_prompt_count_times_two"
        row["historically_observed_public"] = row.get("devgpt_best_historical_status") == 200
        row["current_validity"] = "not_live_checked"
        row["current_validity_reason"] = "Historical DevGPT metrics; current availability not inferred."

    collector.source_status(
        SOURCE,
        len(data) > 0,
        metric_records,
        " | ".join(parse_errors[:12]) or None,
        repository="https://github.com/NAIST-SE/DevGPT",
        archive_bytes=len(response.content),
        sharing_json_files=files_scanned,
        nested_records_seen=records_seen,
        metric_records=metric_records,
        unique_urls=len(data),
        policy="Conversations and HTMLContent skipped; aggregate metadata only",
    )

    summary = collector.make_summary(data)
    summary.update({
        "run_profile": "independent_devgpt_metadata_only_enrichment",
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
        "# DevGPT Metadata-Only Length Enrichment\n\n"
        "Public DevGPT sharing JSON files were scanned for URL, status, dates, model, prompt "
        "count, prompt tokens, and answer tokens. `Conversations` and `HTMLContent` were "
        "explicitly skipped and are not present in any output.\n\n"
        "No Hugging Face or previous Chimera inventory was used as input.\n\n"
        "```json\n" + json.dumps(summary, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
