#!/usr/bin/env python3
"""Classify refined Batch 1 descriptor candidates without over-suppressing context."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FIELDS = [
    "raw_value", "affected_rows", "current_signal_type", "current_recommended_action",
    "contributing_raw_examples_if_available", "reviewer_decision",
    "proposed_destination_field", "proposed_standardized_value", "confidence",
    "rationale", "rule_file_to_update", "rule_to_add", "test_to_add", "notes",
]
PROTECTED = {
    "clinical sample", "respiratory sample", "environmental sample", "food sample",
    "animal sample", "plant sample", "human sample", "cloacal sample",
}
SAFE_SUPPRESS = {
    "sample", "specimen", "metagenome", "pathogen.cl", "metadata descriptor/non-source",
    "not applicable", "none", "missing", "unknown", "other", "uncategorized", "na", "n/a", "null",
}
HARDENING_COUNTS = {"specimen": 1, "uncategorized": 0, "pathogen.cl": 24468, "other": 3835}
ONTOLOGY_PATTERN = re.compile(r"^(?:ENVO|FOODON|PO)[:_]\d+$", re.I)
PROCESS_PATTERN = re.compile(r"\b(?:culture|enrichment|broth|cultured|culture collection)\b", re.I)
CONTEXT_SAMPLE_PATTERN = re.compile(r"\b(?:clinical|respiratory|environmental|food|animal|plant|human|cloacal|rectal|nasal|throat|wound|tissue)\b.*\bsample\b|\bsample\b.*\b(?:clinical|respiratory|environmental|food|animal|plant|human|cloacal|rectal|nasal|throat|wound|tissue)\b", re.I)


def load_json(path: Path | None) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path and path.exists() else {}


def examples(value: str, raw_examples: dict[str, list[dict[str, Any]]]) -> str:
    return "; ".join(
        f"{row.get('raw_value') or '<blank>'} ({int(row.get('count') or 0):,})"
        for row in raw_examples.get(value, [])[:5]
    )


def classify(value: str, signal: str) -> tuple[str, str, str, str]:
    key = value.strip().casefold()
    if key in PROTECTED or CONTEXT_SAMPLE_PATTERN.search(value):
        return (
            "context_bearing_keep_or_review", "context-specific routing", "high",
            "Context-bearing aggregate or phrase; inspect raw material/site evidence and do not blanket-suppress.",
        )
    if ONTOLOGY_PATTERN.fullmatch(value.strip()):
        return (
            "ontology_code_manual_review", "manual_review / ontology resolution", "high",
            "Ontology identifier requires a committed lookup or manual resolution; it is not a generic placeholder.",
        )
    if PROCESS_PATTERN.search(value):
        return (
            "process_descriptor_review", "process/sample review", "medium",
            "Culture/process terminology may coexist with an underlying biological source or sample.",
        )
    if key in SAFE_SUPPRESS:
        return (
            "safe_suppress", "non_source_descriptor / suppressed", "high",
            "Exact generic descriptor or placeholder carries no biological source, material, environment, or site evidence.",
        )
    if "raw_code_or_artifact" in signal:
        return (
            "unresolved_low_priority", "manual_review / unresolved", "low",
            "Opaque code may encode a cell line, device, location, host, or sample; suppressing without context is unsafe.",
        )
    return (
        "unresolved_low_priority", "manual_review / unresolved", "low",
        "Descriptor-like aggregate is not sufficiently specific for an automatic production rule.",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--raw-examples-json", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw_examples = load_json(args.raw_examples_json)
    with args.candidates.open(newline="", encoding="utf-8") as handle:
        candidates = [
            row for row in csv.DictReader(handle)
            if row.get("current_recommended_action") == "route_to_non_source_descriptor"
        ]

    reviewed: list[dict[str, Any]] = []
    seen = set()
    for row in candidates:
        value = str(row.get("current_standardized_value") or row.get("raw_value") or "").strip()
        signal = str(row.get("review_signal_type") or "")
        decision, destination, confidence, rationale = classify(value, signal)
        seen.add(value.casefold())
        reviewed.append({
            "raw_value": value,
            "affected_rows": int(row.get("affected_rows") or 0),
            "current_signal_type": signal,
            "current_recommended_action": str(row.get("current_recommended_action") or ""),
            "contributing_raw_examples_if_available": examples(value, raw_examples),
            "reviewer_decision": decision,
            "proposed_destination_field": destination,
            "proposed_standardized_value": "",
            "confidence": confidence,
            "rationale": rationale,
            "rule_file_to_update": "",
            "rule_to_add": "",
            "test_to_add": "",
            "notes": "No production change unless reviewer_decision is safe_suppress and regression evidence is explicit.",
        })

    for value, count in HARDENING_COUNTS.items():
        if value in seen:
            continue
        reviewed.append({
            "raw_value": value,
            "affected_rows": count,
            "current_signal_type": "regression_hardening",
            "current_recommended_action": "route_to_non_source_descriptor",
            "contributing_raw_examples_if_available": examples(value, raw_examples),
            "reviewer_decision": "safe_suppress",
            "proposed_destination_field": "non_source_descriptor / suppressed",
            "proposed_standardized_value": "",
            "confidence": "high",
            "rationale": "Exact generic descriptor or placeholder; current routing leaked into an exact source or Sample_Type_SD.",
            "rule_file_to_update": "app.py",
            "rule_to_add": "exact suppression and sample-type sentinel sanitization",
            "test_to_add": f"{value} produces no source/sample/environment/site value",
            "notes": "Preventive hardening row; affected_rows counts exact canonical raw occurrences, not review-signal rows.",
        })

    reviewed.sort(key=lambda row: (-int(row["affected_rows"]), str(row["raw_value"]).casefold()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(reviewed)
    counts: dict[str, dict[str, int]] = {}
    for row in reviewed:
        bucket = counts.setdefault(str(row["reviewer_decision"]), {"values": 0, "affected_rows": 0})
        bucket["values"] += 1
        bucket["affected_rows"] += int(row["affected_rows"])
    print(json.dumps({"reviewed": len(reviewed), "classifications": counts}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
