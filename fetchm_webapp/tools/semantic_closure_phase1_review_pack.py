#!/usr/bin/env python3
"""Build review evidence for Semantic Closure Phase 1 dry-run outputs.

This script does not write canonical metadata. It classifies the reviewed Phase 1
rules into an apply-now candidate bucket and a manual-review bucket, then exports
compact examples stratified by rule outcome for reviewer inspection.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools import semantic_closure_phase1_dry_run as closure

DEFAULT_DATE = datetime.now(timezone.utc).strftime("%Y%m%d")
DEFAULT_DRY_RUN_DIR = ROOT / "standardization" / "review" / "semantic_closure_phase1_dry_run" / "20260707"
DEFAULT_APPLY_NOW_DRY_RUN_DIR = ROOT / "standardization" / "review" / "semantic_closure_phase1_apply_now_dry_run" / "20260707"
DEFAULT_OUTPUT_ROOT = ROOT / "standardization" / "review" / "semantic_closure_phase1_review_pack"

APPLY_NOW_RULE_IDS = {
    "SC1-SITE-CATHETER-CLEAR",
    "SC1-HHS-HEALTHY-CONTROL-HEALTH",
    "SC1-HHS-HEALTHY-CONTROL-STUDY",
    "SC1-HHS-DISEASED-PATIENT-HEALTH",
    "SC1-HHS-DISEASED-PATIENT-HUMAN",
}

HOLD_REASONS = {
    "SC1-SITE-CATHETER-DEVICE": "both catheter rows skipped collection-device evidence in Phase 1 dry run",
    "SC1-HHS-DISEASED-PATIENT-SAMPLING": "all diseased/patient rows skipped clinical-subject evidence in Phase 1 dry run",
    "SC1-ENV-MEDIUM-MARINE": "environment axis correction affects many rows and requires reviewer confirmation",
    "SC1-ENV-BROAD-WASTEWATER": "environment axis correction has many existing-different medium values",
    "SC1-ENV-BROAD-ENV-SAMPLE": "large environment-context move requires reviewer confirmation",
    "SC1-ENV-LOCAL-ACTIVATED-SLUDGE": "environment axis correction has many already-same or existing-different destinations",
    "SC1-ENV-LOCAL-GROUNDWATER": "environment axis correction has many already-same or existing-different destinations",
    "SC1-ENV-MEDIUM-HEALTHCARE-FACILITY": "facility/local-scale routing requires reviewer confirmation",
    "SC1-ENV-MEDIUM-GLACIER": "environment axis correction held with other environment moves",
    "SC1-ENV-LOCAL-AGRICULTURAL-ENV": "environment axis correction can interact with broad-scale context",
}

EXAMPLE_COLUMNS = [
    "rule_id",
    "outcome_status",
    "recommended_reviewer_decision",
    "assembly_accession",
    "biosample",
    "organism",
    "current_field",
    "current_value",
    "destination_field",
    "destination_value",
    "existing_destination_value",
    "event_evidence",
    "event_detail",
    "raw_evidence_json",
    "Environment_Medium_SD",
    "Environment_Broad_Scale_SD",
    "Environment_Local_Scale_SD",
    "Sampling_Context_SD",
    "Host_Context_SD",
    "Host_Health_State_SD",
    "Host_Study_Group_SD",
    "Semantic_Axis_Provenance",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def git_commit() -> str:
    configured = str(os.environ.get("FETCHM_WEBAPP_GIT_COMMIT") or "").strip()
    if configured:
        return configured
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT.parent, text=True, capture_output=True, check=False)
    return result.stdout.strip() or "unknown"


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def read_tsv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def rule_to_row(rule: closure.ClosureRule, bucket: str, reason: str, counts: dict[str, str]) -> dict[str, Any]:
    return {
        **{column: getattr(rule, column) for column in closure.RULE_COLUMNS},
        "bucket": bucket,
        "bucket_reason": reason,
        "matched_rows": counts.get("matched_rows", "0"),
        "applied": counts.get("applied", "0"),
        "already_same": counts.get("already_same", "0"),
        "existing_different": counts.get("existing_different", "0"),
        "conditional_assignment_skip": counts.get("conditional_assignment_skip", "0"),
        "clear_only": counts.get("clear_only", "0"),
    }


def raw_evidence(payload: dict[str, Any]) -> dict[str, str]:
    evidence: dict[str, str] = {}
    for field in closure.RAW_EVIDENCE_FIELDS:
        value = payload.get(field)
        if closure.present(value):
            evidence[field] = str(value)
    return evidence


def reviewer_decision(rule_id: str, status: str) -> str:
    if rule_id in APPLY_NOW_RULE_IDS:
        if rule_id == "SC1-HHS-DISEASED-PATIENT-HUMAN":
            return "apply-now candidate; verify independent human evidence in examples"
        return "apply-now candidate"
    if status == "already_same":
        return "hold; destination already represented"
    if status == "existing_different":
        return "hold; preserve existing different destination pending review"
    if status == "conditional_assignment_skip":
        return "hold; evidence condition not met"
    return "hold for manual review"


def example_row(record: dict[str, Any], payload: dict[str, Any], rule: closure.ClosureRule, event: dict[str, Any], status: str) -> dict[str, Any]:
    destination_field = str(event.get("field") or rule.destination_field or "")
    existing_destination = str(payload.get(destination_field) or "") if destination_field else ""
    return {
        "rule_id": rule.rule_id,
        "outcome_status": status,
        "recommended_reviewer_decision": reviewer_decision(rule.rule_id, status),
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "current_field": rule.current_field,
        "current_value": str(payload.get(rule.current_field) or ""),
        "destination_field": destination_field,
        "destination_value": str(event.get("value") or rule.destination_value or ""),
        "existing_destination_value": existing_destination,
        "event_evidence": str(event.get("evidence") or ""),
        "event_detail": str(event.get("detail") or ""),
        "raw_evidence_json": closure.compact_json(raw_evidence(payload)),
        "Environment_Medium_SD": str(payload.get("Environment_Medium_SD") or ""),
        "Environment_Broad_Scale_SD": str(payload.get("Environment_Broad_Scale_SD") or ""),
        "Environment_Local_Scale_SD": str(payload.get("Environment_Local_Scale_SD") or ""),
        "Sampling_Context_SD": str(payload.get("Sampling_Context_SD") or ""),
        "Host_Context_SD": str(payload.get("Host_Context_SD") or ""),
        "Host_Health_State_SD": str(payload.get("Host_Health_State_SD") or ""),
        "Host_Study_Group_SD": str(payload.get("Host_Study_Group_SD") or ""),
        "Semantic_Axis_Provenance": closure.compact_json(payload.get(closure.PROVENANCE_FIELD) or {}),
    }


def collect_examples(snapshot_id: str, rules: list[closure.ClosureRule], examples_per_rule_outcome: int, chunk_size: int) -> dict[str, list[dict[str, Any]]]:
    grouped = closure.rules_by_current(rules)
    by_id = {rule.rule_id: rule for rule in rules}
    wanted_statuses = {"applied", "already_same", "existing_different", "conditional_assignment_skip", "clear_only"}
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    counts: dict[tuple[str, str], int] = defaultdict(int)
    target_pairs = {(rule.rule_id, status) for rule in rules for status in wanted_statuses}

    for record in closure.iter_records(snapshot_id, chunk_size=chunk_size):
        if not target_pairs:
            break
        payload = record["payload"]
        result = closure.apply_rules_to_payload(payload, grouped)
        if not result["matched_rules"]:
            continue
        for event in result["outcomes"]:
            status = str(event.get("status") or "")
            rule_id = str(event.get("rule_id") or "")
            pair = (rule_id, status)
            if status not in wanted_statuses or pair not in target_pairs:
                continue
            rule = by_id[rule_id]
            rows[status].append(example_row(record, payload, rule, event, status))
            counts[pair] += 1
            if counts[pair] >= examples_per_rule_outcome:
                target_pairs.discard(pair)
    return rows


def write_review_pack(
    output_dir: Path,
    snapshot_id: str,
    full_dry_run_dir: Path,
    apply_now_dry_run_dir: Path,
    rules_path: Path,
    examples_per_rule_outcome: int,
    chunk_size: int,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    rules = closure.load_rules(rules_path)
    rule_counts = {row["rule_id"]: row for row in read_tsv(full_dry_run_dir / "rule_level_before_after.tsv")}
    full_summary = read_json(full_dry_run_dir / "semantic_closure_phase1_dry_run_summary.json")
    apply_summary = read_json(apply_now_dry_run_dir / "semantic_closure_phase1_dry_run_summary.json")

    apply_rows = []
    hold_rows = []
    for rule in rules:
        counts = rule_counts.get(rule.rule_id, {})
        if rule.rule_id in APPLY_NOW_RULE_IDS:
            reason = "finite strict-field correction candidate; no environment-axis rule"
            if rule.rule_id == "SC1-HHS-DISEASED-PATIENT-HUMAN":
                reason = "evidence-gated human context candidate; dry-run applied only rows with independent human evidence"
            apply_rows.append(rule_to_row(rule, "apply_now_candidate", reason, counts))
        else:
            hold_rows.append(rule_to_row(rule, "hold_for_manual_review", HOLD_REASONS.get(rule.rule_id, "held for reviewer confirmation"), counts))

    review_columns = [*closure.RULE_COLUMNS, "bucket", "bucket_reason", "matched_rows", "applied", "already_same", "existing_different", "conditional_assignment_skip", "clear_only"]
    closure.write_tsv(output_dir / "apply_now_candidate_rules.tsv", review_columns, apply_rows)
    closure.write_tsv(output_dir / "hold_for_manual_review_rules.tsv", review_columns, hold_rows)

    examples = collect_examples(snapshot_id, rules, examples_per_rule_outcome, chunk_size)
    closure.write_tsv(output_dir / "rule_level_examples_applied.tsv", EXAMPLE_COLUMNS, examples.get("applied", []))
    closure.write_tsv(output_dir / "rule_level_examples_already_same.tsv", EXAMPLE_COLUMNS, examples.get("already_same", []))
    closure.write_tsv(output_dir / "rule_level_examples_existing_different.tsv", EXAMPLE_COLUMNS, examples.get("existing_different", []))
    closure.write_tsv(output_dir / "rule_level_examples_conditional_skips.tsv", EXAMPLE_COLUMNS, examples.get("conditional_assignment_skip", []))
    closure.write_tsv(output_dir / "rule_level_examples_clear_only.tsv", EXAMPLE_COLUMNS, examples.get("clear_only", []))

    field_rows = read_tsv(full_dry_run_dir / "projected_field_changes.tsv")
    closure.write_tsv(output_dir / "field_change_summary.tsv", ["field", "projected_changed_rows"], field_rows)

    apply_projected_clears = int(apply_summary.get("projected_clears") or 0)
    remaining_rows = [
        {
            "queue": "strict_field_violation_assignment_occurrences",
            "current_assignment_occurrences": "910373",
            "apply_now_projected_clears": str(apply_projected_clears),
            "expected_remaining_or_reviewed_after_apply_now": str(max(0, 910373 - apply_projected_clears)),
            "note": "only Bucket A strict health/catheter rules are included; environment rules remain held",
        },
        {
            "queue": "environment_axis_rules_held",
            "current_assignment_occurrences": str(sum(int(row.get("matched_rows") or 0) for row in hold_rows if row["rule_id"].startswith("SC1-ENV-"))),
            "apply_now_projected_clears": "0",
            "expected_remaining_or_reviewed_after_apply_now": str(sum(int(row.get("matched_rows") or 0) for row in hold_rows if row["rule_id"].startswith("SC1-ENV-"))),
            "note": "held for manual review pack inspection",
        },
        {
            "queue": "manual_review_or_condition_skips",
            "current_assignment_occurrences": str(sum(int(row.get("conditional_assignment_skip") or 0) for row in hold_rows)),
            "apply_now_projected_clears": "0",
            "expected_remaining_or_reviewed_after_apply_now": str(sum(int(row.get("conditional_assignment_skip") or 0) for row in hold_rows)),
            "note": "not applied by Bucket A dry run",
        },
    ]
    closure.write_tsv(
        output_dir / "expected_remaining_strict_signals_after_apply.tsv",
        ["queue", "current_assignment_occurrences", "apply_now_projected_clears", "expected_remaining_or_reviewed_after_apply_now", "note"],
        remaining_rows,
    )

    summary = {
        "generated_at": utc_now(),
        "implementation_commit": git_commit(),
        "snapshot_id": snapshot_id,
        "source_dry_run_dir": str(full_dry_run_dir),
        "apply_now_dry_run_dir": str(apply_now_dry_run_dir),
        "full_phase1_projected_rows_changed": full_summary.get("projected_rows_changed"),
        "full_phase1_projected_clears": full_summary.get("projected_clears"),
        "full_phase1_projected_new_axis_assignments": full_summary.get("projected_new_axis_assignments"),
        "full_phase1_destination_conflicts": full_summary.get("destination_conflicts"),
        "full_phase1_second_pass_changes": full_summary.get("second_pass_changes"),
        "apply_now_projected_rows_changed": apply_summary.get("projected_rows_changed"),
        "apply_now_projected_clears": apply_summary.get("projected_clears"),
        "apply_now_projected_new_axis_assignments": apply_summary.get("projected_new_axis_assignments"),
        "apply_now_destination_conflicts": apply_summary.get("destination_conflicts"),
        "apply_now_second_pass_changes": apply_summary.get("second_pass_changes"),
        "apply_now_rules": len(apply_rows),
        "hold_rules": len(hold_rows),
        "environment_rules_held": sum(1 for row in hold_rows if row["rule_id"].startswith("SC1-ENV-")),
        "canonical_write_run": False,
        "global_insights_regenerated": False,
        "deployment_run": False,
        "promotion_gate": {"safe_to_apply": False, "reason": "manual review required before canonical write"},
    }
    write_json(output_dir / "phase1_review_summary.json", summary)
    (output_dir / "phase1_review_summary.md").write_text(
        "# Semantic Closure Phase 1 Review Pack\n\n"
        f"Generated: {summary['generated_at']}\n\n"
        f"Snapshot: `{snapshot_id}`\n\n"
        f"Full Phase 1 dry-run projected changed rows: {summary['full_phase1_projected_rows_changed']:,}\n\n"
        f"Bucket A apply-now dry-run projected changed rows: {summary['apply_now_projected_rows_changed']:,}\n\n"
        f"Apply-now candidate rules: {summary['apply_now_rules']}\n\n"
        f"Held rules: {summary['hold_rules']} ({summary['environment_rules_held']} environment rules)\n\n"
        "Canonical metadata was not changed. Global Insights was not regenerated. Deployment was not run.\n\n"
        "Promotion gate remains closed: `safe_to_apply=false`.\n",
        encoding="utf-8",
    )
    return summary


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Semantic Closure Phase 1 review pack")
    parser.add_argument("--snapshot-id", default=closure.DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--rules", type=Path, default=closure.DEFAULT_RULES)
    parser.add_argument("--full-dry-run-dir", type=Path, default=DEFAULT_DRY_RUN_DIR)
    parser.add_argument("--apply-now-dry-run-dir", type=Path, default=DEFAULT_APPLY_NOW_DRY_RUN_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT / DEFAULT_DATE)
    parser.add_argument("--examples-per-rule-outcome", type=int, default=20)
    parser.add_argument("--chunk-size", type=int, default=5000)
    args = parser.parse_args(list(argv) if argv is not None else None)
    summary = write_review_pack(
        args.output_dir,
        args.snapshot_id,
        args.full_dry_run_dir,
        args.apply_now_dry_run_dir,
        args.rules,
        max(1, args.examples_per_rule_outcome),
        max(1, args.chunk_size),
    )
    print(json.dumps({
        "output_dir": str(args.output_dir),
        "apply_now_projected_rows_changed": summary.get("apply_now_projected_rows_changed"),
        "held_rules": summary.get("hold_rules"),
        "safe_to_apply": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
