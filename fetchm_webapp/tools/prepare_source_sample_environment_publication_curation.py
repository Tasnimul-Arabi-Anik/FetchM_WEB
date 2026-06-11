#!/usr/bin/env python3
"""Prepare a non-mutating publication-curation workspace from canonical QA signals."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_QA_ROOT = ROOT / "standardization" / "review" / "source_sample_environment_qa"
DEFAULT_OUTPUT_ROOT = ROOT / "standardization" / "review" / "source_sample_environment_publication_curation"

CANDIDATE_FIELDS = [
    "raw_value", "affected_rows", "current_standardized_value", "current_broad_value",
    "review_signal_type", "current_recommended_action", "proposed_destination_field",
    "proposed_standardized_value", "proposed_broad_value", "confidence", "rationale",
    "reviewer_decision", "rule_file_to_update", "rule_to_add", "test_to_add", "notes",
]

ACTION_METADATA = {
    "route_to_non_source_descriptor": (
        "Batch 1 - metadata descriptors", "suppressed/non_source_descriptor",
        "Descriptor-like signal; verify raw variants before adding a suppression rule.",
    ),
    "route_to_sample_type": (
        "Batch 2 - sample materials", "Sample_Type_SD",
        "Sample-material signal; preserve source context only when independently supported.",
    ),
    "route_to_isolation_site": (
        "Batch 3 - anatomical sites", "Isolation_Site_SD",
        "Anatomical-site signal; split material and site when both are present.",
    ),
    "route_to_environment_medium": (
        "Batch 4 - environmental media", "Environment_Medium_SD",
        "Environmental-medium signal; retain broad source context only when intentional.",
    ),
    "route_to_food_source": (
        "Batch 5 - food and commodity context", "Isolation_Source_SD / food context",
        "Food or commodity context requires phrase-level review; animal terms alone are insufficient.",
    ),
    "route_to_host_context": (
        "Batch 6 - host context", "Host_Context_SD / host-associated context",
        "Host-only signal requires raw context before source or host routing.",
    ),
    "route_to_disease_or_health_state": (
        "Batch 7 - disease and health state", "Host_Disease_SD / Host_Health_State_SD",
        "Disease or health-state signal; preserve independent material and site evidence.",
    ),
}


def latest_qa_dir(root: Path) -> Path:
    candidates = [
        path for path in root.iterdir()
        if path.is_dir()
        and (path / "exact_isolation_source_suspicious_values.csv").exists()
        and (path / "source_sample_environment_qa_summary.json").exists()
    ]
    if not candidates:
        raise FileNotFoundError(f"No source/sample/environment QA checkpoint found under {root}")
    return max(candidates, key=lambda path: (path.stat().st_mtime_ns, path.name))


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def candidate_row(
    row: dict[str, str], enrichment: dict[str, dict[str, Any]]
) -> dict[str, str | int]:
    value = str(row.get("isolation_source_sd") or "").strip()
    action = str(row.get("recommended_action") or "").strip()
    batch, destination, rationale = ACTION_METADATA.get(
        action,
        (
            "Batch 8 - ambiguous context", "manual_review / unresolved",
            "No safe automatic destination; inspect raw variants and surrounding metadata.",
        ),
    )
    current = enrichment.get(value) or {}
    current_method = str(current.get("current_method") or "")
    return {
        "raw_value": value,
        "affected_rows": int(row.get("assembly_count") or 0),
        "current_standardized_value": value,
        "current_broad_value": str(current.get("current_broad_value") or ""),
        "review_signal_type": str(row.get("signal_classes") or "").strip(),
        "current_recommended_action": action,
        "proposed_destination_field": destination,
        "proposed_standardized_value": "",
        "proposed_broad_value": "",
        "confidence": "needs_review",
        "rationale": rationale,
        "reviewer_decision": "unreviewed",
        "rule_file_to_update": "",
        "rule_to_add": "",
        "test_to_add": "",
        "notes": (
            f"{batch}. Current mode routing method: {current_method or 'not recorded'}. "
            "This value comes from Isolation_Source_SD in the QA aggregate, not directly from the "
            "raw metadata field; inspect contributing raw variants before deciding."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qa-dir", type=Path, help="Specific canonical QA checkpoint directory.")
    parser.add_argument("--output-dir", type=Path, help="Publication-curation output directory.")
    parser.add_argument(
        "--enrichment-json", type=Path,
        help="Optional canonical mode broad-value/method mapping keyed by Isolation_Source_SD.",
    )
    args = parser.parse_args()

    qa_dir = args.qa_dir.resolve() if args.qa_dir else latest_qa_dir(DEFAULT_QA_ROOT)
    output_dir = args.output_dir.resolve() if args.output_dir else (
        DEFAULT_OUTPUT_ROOT / datetime.now(timezone.utc).strftime("%Y%m%d")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = load_json(qa_dir / "source_sample_environment_qa_summary.json")
    enrichment = load_json(args.enrichment_json.resolve()) if args.enrichment_json else {}
    with (qa_dir / "exact_isolation_source_suspicious_values.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        source_rows = list(csv.DictReader(handle))
    candidates = sorted(
        (candidate_row(row, enrichment) for row in source_rows),
        key=lambda row: (-int(row["affected_rows"]), str(row["current_standardized_value"]).casefold()),
    )

    candidate_path = output_dir / "source_sample_environment_publication_review_candidates.csv"
    with candidate_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANDIDATE_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(candidates)

    action_counts: Counter[str] = Counter()
    action_rows: Counter[str] = Counter()
    signal_counts: Counter[str] = Counter()
    signal_rows: Counter[str] = Counter()
    batch_counts: Counter[str] = Counter()
    batch_rows: Counter[str] = Counter()
    for row in candidates:
        action = str(row["current_recommended_action"] or "unclassified")
        action_counts[action] += 1
        action_rows[action] += int(row["affected_rows"])
        batch = str(row["notes"]).split(".", 1)[0]
        batch_counts[batch] += 1
        batch_rows[batch] += int(row["affected_rows"])
        for signal in filter(None, str(row["review_signal_type"]).split("|")):
            signal_counts[signal] += 1
            signal_rows[signal] += int(row["affected_rows"])

    metrics = summary.get("metrics") or {}
    baseline = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_qa_directory": display_path(qa_dir),
        "candidate_file": display_path(candidate_path),
        "input_semantics": (
            "Candidates are aggregated Isolation_Source_SD values. raw_value mirrors the QA signal "
            "label and is not guaranteed to be an original raw metadata value."
        ),
        "metrics": {
            "rows_audited": int(metrics.get("total_rows_scanned") or 0),
            "isolation_source_sd_coverage_percent": float(metrics.get("isolation_source_sd_present_percent") or 0),
            "isolation_source_sd_broad_coverage_percent": float(metrics.get("isolation_source_sd_broad_present_percent") or 0),
            "raw_present_standardization_percent": float(metrics.get("raw_present_isolation_source_standardization_percent") or 0),
            "hard_exact_leakage_rows": int(metrics.get("hard_exact_leakage_rows") or 0),
            "broad_leakage_rows": int(metrics.get("non_approved_broad_rows") or 0),
            "review_signal_rows": int(metrics.get("review_signal_exact_cross_field_rows") or 0),
            "review_signal_unique_values": int(metrics.get("review_signal_exact_cross_field_unique_values") or 0),
        },
        "top_100_review_signals": [
            {
                "value": row["current_standardized_value"],
                "affected_rows": row["affected_rows"],
                "signal_type": row["review_signal_type"],
                "recommended_action": row["current_recommended_action"],
            }
            for row in candidates[:100]
        ],
        "categories": {
            "by_recommended_action": [
                {"category": key, "unique_values": action_counts[key], "affected_rows": action_rows[key]}
                for key in sorted(action_counts, key=lambda key: (-action_rows[key], key))
            ],
            "by_signal_class": [
                {"category": key, "unique_values": signal_counts[key], "affected_rows": signal_rows[key]}
                for key in sorted(signal_counts, key=lambda key: (-signal_rows[key], key))
            ],
            "by_proposed_batch": [
                {"category": key, "unique_values": batch_counts[key], "affected_rows": batch_rows[key]}
                for key in sorted(batch_counts, key=lambda key: (-batch_rows[key], key))
            ],
        },
    }
    (output_dir / "publication_curation_baseline_summary.json").write_text(
        json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    metric = baseline["metrics"]
    lines = [
        "# Source, Sample, and Environment Publication Curation Baseline", "",
        f"- Canonical rows audited: {metric['rows_audited']:,}",
        f"- Isolation_Source_SD coverage: {metric['isolation_source_sd_coverage_percent']:.2f}%",
        f"- Isolation_Source_SD_Broad coverage: {metric['isolation_source_sd_broad_coverage_percent']:.2f}%",
        f"- Raw-present standardization: {metric['raw_present_standardization_percent']:.2f}%",
        f"- Hard exact leakage rows: {metric['hard_exact_leakage_rows']:,}",
        f"- Broad leakage rows: {metric['broad_leakage_rows']:,}",
        f"- Review-signal rows: {metric['review_signal_rows']:,}",
        f"- Review-signal unique values: {metric['review_signal_unique_values']:,}", "",
        "Review signals are triage candidates, not errors. Candidate `raw_value` entries mirror the",
        "aggregated exact standardized source label; each batch must inspect contributing raw values",
        "and surrounding metadata before adding rules.", "", "## Proposed Batches", "",
        "| Batch | Unique values | Affected rows |", "| --- | ---: | ---: |",
    ]
    for row in baseline["categories"]["by_proposed_batch"]:
        lines.append(f"| {row['category']} | {row['unique_values']:,} | {row['affected_rows']:,} |")
    lines.extend(["", "## Top 100 Review Signals", "", "| Value | Rows | Signal | Action |", "| --- | ---: | --- | --- |"])
    for row in baseline["top_100_review_signals"]:
        value = str(row["value"]).replace("|", "\\|")
        lines.append(f"| {value} | {row['affected_rows']:,} | {row['signal_type']} | {row['recommended_action']} |")
    (output_dir / "publication_curation_baseline_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({"qa_dir": str(qa_dir), "output_dir": str(output_dir), "candidates": len(candidates)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
