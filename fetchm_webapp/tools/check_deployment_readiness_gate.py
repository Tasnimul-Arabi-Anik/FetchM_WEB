#!/usr/bin/env python3
"""Build source/sample/environment consolidation artifacts and manual deployment gate."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CONTROLLED_CATEGORIES = ROOT / "standardization" / "controlled_categories.csv"
APPROVED_BROAD_CATEGORIES = ROOT / "standardization" / "approved_broad_categories.csv"
DEFAULT_OUTPUT_DIR = ROOT / "standardization" / "review" / "source_sample_environment_publication_curation" / "20260616_consolidation"
DEFAULT_QA_ROOT = ROOT / "data" / "source_sample_environment_qa"

DISEASE_PATTERN = re.compile(r"\b(?:disease|infection|sepsis|septicemia|bacteremia|bacteraemia|diarrh|mastitis|pneumonia|meningitis|cystic fibrosis|tuberculosis)\b", re.I)
SAMPLE_PATTERN = re.compile(r"\b(?:blood|urine|stool|feces|faeces|milk|swab|tissue|aspirate|sample)\b", re.I)
ENV_PATTERN = re.compile(r"\b(?:soil|water|wastewater|sewage|sediment|sludge|biofilm|environment)\b", re.I)

EXAMPLE_VALUES = [
    "diarrhea", "diarrhoea", "diarrheal", "diarrheal stool", "urine from UTI",
    "blood from sepsis", "mastitis milk", "bovine mastitis milk", "septicemia",
    "bacteraemia", "skin infection", "lower respiratory infection", "clinical sample",
    "respiratory sample", "contaminated food", "wastewater surveillance",
    "outbreak food source", "carrier", "colonized", "patient",
]
CLINICAL_EXAMPLES = [
    "clinical sample", "clinical patient", "patient sample", "respiratory sample",
    "hospital environment", "healthcare worker nasal",
]
ADMIN_VALUES = [
    "infection", "infected animal", "diseased plant", "outbreak", "outbreak food source",
    "contaminated food", "wastewater surveillance", "carrier", "colonized", "clinical", "patient",
]
FIELDS = ["Host_Disease_SD", "Host_Health_State_SD", "Sample_Type_SD", "Isolation_Site_SD", "Isolation_Source_SD", "Isolation_Source_SD_Broad", "Environment_Medium_SD", "Host_Context_SD"]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def latest_qa_summary() -> Path:
    latest = load_json(DEFAULT_QA_ROOT / "latest.json")
    summary = str(latest.get("summary") or "")
    if not summary:
        raise FileNotFoundError("No source/sample/environment latest.json summary pointer found.")
    return DEFAULT_QA_ROOT / summary


def write_tsv(path: Path, header: list[str], rows: Iterable[Iterable[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        return list(csv.DictReader(handle))


def approved_broad_values() -> set[str]:
    if not APPROVED_BROAD_CATEGORIES.exists():
        return set()
    values: set[str] = set()
    for row in read_rows(APPROVED_BROAD_CATEGORIES):
        value = (row.get("approved_value") or row.get("value") or "").strip()
        if value:
            values.add(value)
    return values


def controlled_category_audit(output_dir: Path) -> dict[str, Any]:
    rows = read_rows(CONTROLLED_CATEGORIES)
    approved = [row for row in rows if (row.get("status") or "approved").strip().lower() in {"approved", "active"}]
    grouped: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    normalized_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    broad_values = approved_broad_values()
    audit_rows: list[list[Any]] = []
    for row in approved:
        normalized = (row.get("normalized_value") or row.get("synonym") or "").strip()
        destination = (row.get("destination") or "").strip()
        proposed = (row.get("proposed_value") or row.get("category") or "").strip()
        broad = (row.get("broad_value") or "").strip()
        key = ((row.get("source_column") or "").strip(), normalized, destination)
        grouped[key].append(row)
        normalized_groups[normalized].append(row)
        issue_type = "none"
        severity = "pass"
        action = "none"
        haystack = f"{normalized} {proposed} {broad}"
        if destination == "Isolation_Source_SD" and DISEASE_PATTERN.search(normalized) and proposed not in {"clinical/host-associated material", ""}:
            issue_type = "review_signal_disease_source_rule"
            severity = "review"
            action = "confirm source context or split disease/sample/site"
        elif destination == "Host_Disease_SD" and ENV_PATTERN.search(normalized) and not DISEASE_PATTERN.search(normalized):
            issue_type = "review_signal_environment_to_disease"
            severity = "review"
            action = "manual review"
        elif destination == "Host_Disease_SD" and SAMPLE_PATTERN.search(normalized) and not DISEASE_PATTERN.search(haystack):
            issue_type = "review_signal_sample_to_disease"
            severity = "review"
            action = "manual review"
        elif broad and broad_values and broad not in broad_values and destination == "Isolation_Source_SD":
            issue_type = "review_signal_legacy_rule_broad_value"
            severity = "review"
            action = "runtime broad-category QA is authoritative; review legacy rule broad label when touched"
        audit_rows.append([
            normalized,
            row.get("original_value") or row.get("synonym") or "",
            row.get("source_column") or "",
            destination,
            proposed,
            broad,
            row.get("status") or "approved",
            row.get("confidence") or "",
            batch_id(row),
            issue_type,
            severity,
            action,
        ])
    duplicate_rows: list[list[Any]] = []
    conflict_rows: list[list[Any]] = []
    for (source_column, normalized, destination), members in grouped.items():
        values = sorted({(row.get("proposed_value") or row.get("category") or "").strip() for row in members})
        if len(members) > 1 and len(values) == 1:
            duplicate_rows.append([source_column, normalized, destination, len(members), "; ".join(values)])
        if len(values) > 1:
            conflict_rows.append([source_column, normalized, destination, len(members), "; ".join(values)])
    write_tsv(output_dir / "controlled_category_audit.tsv", [
        "normalized_lookup", "raw_value", "source_column", "target_field", "standardized_value",
        "broad_value", "review_status", "confidence", "batch_id", "issue_type", "severity", "recommended_action",
    ], audit_rows)
    write_tsv(output_dir / "controlled_category_duplicate_keys.tsv", ["source_column", "normalized_lookup", "target_field", "rule_count", "standardized_values"], duplicate_rows)
    write_tsv(output_dir / "controlled_category_conflict_keys.tsv", ["source_column", "normalized_lookup", "target_field", "rule_count", "standardized_values"], conflict_rows)
    hard_rule_leakage = sum(1 for row in audit_rows if row[10] == "hard")
    return {
        "total_rows": len(rows),
        "approved_rows": len(approved),
        "duplicate_keys": len(duplicate_rows),
        "conflict_keys": len(conflict_rows),
        "hard_rule_leakage": hard_rule_leakage,
    }


def batch_id(row: dict[str, str]) -> str:
    note = (row.get("note") or "").lower()
    method = (row.get("method") or "").lower()
    for candidate in ["batch7", "batch6", "batch5", "batch4", "batch3", "batch2", "batch1"]:
        if candidate in note or candidate in method:
            return candidate
    return "preexisting"


def standardize_examples(values: list[str]) -> dict[str, dict[str, str]]:
    from app import ensure_managed_metadata_schema
    result: dict[str, dict[str, str]] = {}
    for value in values:
        row = ensure_managed_metadata_schema({"Host": "", "Isolation Source": value, "Sample Type": "metagenomic assembly"})
        result[value] = {field: str(row.get(field) or "") for field in FIELDS}
    return result


def legacy_before(value: str) -> dict[str, str]:
    before = {field: "" for field in FIELDS}
    if value in {"mastitis milk", "bovine mastitis milk"}:
        before["Isolation_Source_SD"] = "milk"
        before["Isolation_Source_SD_Broad"] = "food/source material"
    if value == "diarrheal stool":
        before["Sample_Type_SD"] = "feces/stool"
    if value in {"urine from UTI"}:
        before["Isolation_Site_SD"] = "urine"
    if value in {"blood from sepsis"}:
        before["Sample_Type_SD"] = "blood"
    return before


def disease_health_audit(output_dir: Path) -> None:
    examples = standardize_examples(EXAMPLE_VALUES)
    rows = []
    for value in EXAMPLE_VALUES:
        before = legacy_before(value)
        after = examples[value]
        decision = decision_for(value, after)
        rows.append([
            value,
            before["Host_Disease_SD"], after["Host_Disease_SD"],
            before["Host_Health_State_SD"], after["Host_Health_State_SD"],
            before["Sample_Type_SD"], after["Sample_Type_SD"],
            before["Isolation_Site_SD"], after["Isolation_Site_SD"],
            before["Isolation_Source_SD"], after["Isolation_Source_SD"],
            decision,
            "high" if decision != "admin_review" else "medium",
            review_notes_for(value),
        ])
    write_tsv(output_dir / "disease_health_curated_examples_before_after.tsv", [
        "raw_value", "before_Host_Disease_SD", "after_Host_Disease_SD",
        "before_Host_Health_State_SD", "after_Host_Health_State_SD",
        "before_Sample_Type_SD", "after_Sample_Type_SD",
        "before_Isolation_Site_SD", "after_Isolation_Site_SD",
        "before_Isolation_Source_SD", "after_Isolation_Source_SD",
        "decision", "confidence", "review_notes",
    ], rows)


def decision_for(value: str, after: dict[str, str]) -> str:
    if value in {"clinical sample", "respiratory sample"}:
        return "preserve_clinical_context"
    if value in {"contaminated food", "wastewater surveillance", "outbreak food source", "carrier", "colonized", "patient"}:
        return "admin_review" if not after["Host_Disease_SD"] else "review_existing_disease_mapping"
    if after["Host_Disease_SD"] and after["Sample_Type_SD"]:
        return "split_disease_and_sample_type"
    if after["Host_Disease_SD"] and after["Isolation_Site_SD"]:
        return "split_disease_and_isolation_site"
    if after["Host_Disease_SD"]:
        return "route_to_host_disease"
    if after["Host_Health_State_SD"]:
        return "route_to_host_health_state"
    return "admin_review"


def review_notes_for(value: str) -> str:
    notes = {
        "clinical sample": "Context-bearing aggregate label; do not infer disease.",
        "respiratory sample": "Respiratory site/sample context; do not infer disease.",
        "contaminated food": "Food safety context, not host disease without explicit disease evidence.",
        "wastewater surveillance": "Environment surveillance context, not host disease.",
        "outbreak food source": "Outbreak/source context remains admin review unless disease is explicit.",
        "carrier": "Carrier status remains admin review.",
        "colonized": "Colonization status remains admin review.",
        "patient": "Patient-only context remains host/clinical context, not disease.",
    }
    return notes.get(value, "Reviewed Batch 7 disease/health-state consolidation example.")


def clinical_context_audit(output_dir: Path) -> None:
    examples = standardize_examples(CLINICAL_EXAMPLES)
    rows = []
    for value, after in examples.items():
        rows.append([
            value,
            after["Host_Disease_SD"],
            after["Host_Health_State_SD"],
            after["Isolation_Source_SD"],
            after["Isolation_Source_SD_Broad"],
            after["Sample_Type_SD"],
            after["Isolation_Site_SD"],
            after["Environment_Medium_SD"],
            "pass" if not after["Host_Disease_SD"] else "review",
            review_notes_for(value),
        ])
    write_tsv(output_dir / "clinical_context_preservation_audit.tsv", [
        "raw_value", "Host_Disease_SD", "Host_Health_State_SD", "Isolation_Source_SD",
        "Isolation_Source_SD_Broad", "Sample_Type_SD", "Isolation_Site_SD", "Environment_Medium_SD",
        "status", "notes",
    ], rows)


def admin_review_remaining(output_dir: Path) -> None:
    batch_dir = ROOT / "standardization" / "review" / "source_sample_environment_publication_curation" / "20260611"
    rows: list[list[str]] = []
    seen: set[str] = set()
    for batch in ["batch4", "batch5", "batch6", "batch7"]:
        path = batch_dir / f"{batch}_admin_review_needed.csv"
        if not path.exists():
            continue
        for row in read_rows(path):
            value = (row.get("raw_value") or row.get("value") or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            rows.append([
                value,
                row.get("current_signal_type") or row.get("reason") or "manual_review",
                row.get("current_standardized_value") or "",
                row.get("current_broad_value") or "",
                row.get("reason") or row.get("notes") or "ambiguous reviewed value",
                row.get("recommended_action") or row.get("reviewer_decision") or "manual_review",
                priority_for(value),
                batch,
            ])
    for value in ADMIN_VALUES:
        if value not in seen:
            rows.append([value, "disease_health_ambiguity", "", "", review_notes_for(value), "manual_review", priority_for(value), "batch7"])
    write_tsv(output_dir / "admin_review_remaining.tsv", [
        "raw_value", "current_signal_type", "current_standardized_value", "current_broad_value",
        "reason", "recommended_action", "priority", "batch_source",
    ], rows)


def priority_for(value: str) -> str:
    if value in {"infection", "clinical", "patient", "contaminated food", "wastewater surveillance"}:
        return "high"
    if value in {"outbreak", "outbreak food source", "carrier", "colonized"}:
        return "medium"
    return "low"


def canonical_summary(output_dir: Path, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_metrics = before.get("metrics") or {}
    after_metrics = after.get("metrics") or {}
    metrics = [
        "total_rows_scanned", "raw_isolation_source_present_count", "isolation_source_sd_present_count",
        "isolation_source_sd_broad_present_count", "raw_present_isolation_source_standardization_percent",
        "raw_only_unresolved_isolation_source_rows", "host_disease_sd_present_count",
        "host_health_state_sd_present_count", "sample_type_sd_present_count", "environment_medium_sd_present_count",
        "hard_exact_leakage_rows", "non_approved_broad_rows", "controlled_category_duplicate_keys",
        "controlled_category_conflict_keys",
    ]
    rows = []
    for metric in metrics:
        b = before_metrics.get(metric, "")
        a = after_metrics.get(metric, "")
        delta = ""
        try:
            delta = round(float(a) - float(b), 4)
        except Exception:
            delta = ""
        pass_fail = "pass"
        if metric in {"hard_exact_leakage_rows", "non_approved_broad_rows", "controlled_category_duplicate_keys", "controlled_category_conflict_keys"}:
            pass_fail = "pass" if int(float(a or 0)) == 0 else "fail"
        rows.append([metric, b, a, delta, pass_fail, "no hard-gate issue" if pass_fail == "pass" else "review required"])
    write_tsv(output_dir / "before_after_canonical_refresh_summary.tsv", ["metric", "before", "after", "delta", "pass_fail", "notes"], rows)
    write_tsv(output_dir / "source_sample_environment_leakage_audit.tsv", ["metric", "value", "status"], [
        ["hard_exact_leakage_rows", after_metrics.get("hard_exact_leakage_rows", 0), "pass" if int(after_metrics.get("hard_exact_leakage_rows") or 0) == 0 else "fail"],
        ["review_signal_exact_cross_field_rows", after_metrics.get("review_signal_exact_cross_field_rows", 0), "triage_signal"],
        ["metadata_descriptor_suppressed_rows", after_metrics.get("metadata_descriptor_suppressed_rows", 0), "informational"],
    ])
    write_tsv(output_dir / "broad_category_leakage_audit.tsv", ["metric", "value", "status"], [
        ["non_approved_broad_rows", after_metrics.get("non_approved_broad_rows", 0), "pass" if int(after_metrics.get("non_approved_broad_rows") or 0) == 0 else "fail"],
        ["non_approved_broad_unique_values", after_metrics.get("non_approved_broad_unique_values", 0), "pass" if int(after_metrics.get("non_approved_broad_unique_values") or 0) == 0 else "fail"],
    ])
    return after_metrics


def deployment_gate(output_dir: Path, rule_metrics: dict[str, Any], after: dict[str, Any], gi: dict[str, Any], tests_passed: bool) -> dict[str, Any]:
    metrics = after.get("metrics") or {}
    hard_leakage = int(metrics.get("hard_exact_leakage_rows") or 0)
    broad_leakage = int(metrics.get("non_approved_broad_rows") or 0)
    duplicate_keys = int(metrics.get("controlled_category_duplicate_keys") or rule_metrics.get("duplicate_keys") or 0)
    conflict_keys = int(metrics.get("controlled_category_conflict_keys") or rule_metrics.get("conflict_keys") or 0)
    canonical_pass = after.get("status") == "pass" and not after.get("hard_failures")
    gi_pass = bool(gi.get("pass")) and bool(gi.get("global_insights_snapshot_sha256"))
    hard_failures = []
    if not tests_passed:
        hard_failures.append("tests failed or not recorded")
    if not canonical_pass:
        hard_failures.append("canonical source/sample/environment QA did not pass")
    if not gi_pass:
        hard_failures.append("Global Insights regeneration missing or failed")
    if hard_leakage:
        hard_failures.append("hard leakage after refresh > 0")
    if broad_leakage:
        hard_failures.append("broad vocabulary leakage after refresh > 0")
    if duplicate_keys:
        hard_failures.append("controlled-category duplicate keys > 0")
    if conflict_keys:
        hard_failures.append("controlled-category conflict keys > 0")
    gate = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "controlled_category_audit_pass": duplicate_keys == 0 and conflict_keys == 0 and int(rule_metrics.get("hard_rule_leakage") or 0) == 0,
        "canonical_refresh_pass": canonical_pass,
        "global_insights_regenerated": gi_pass,
        "tests_pass": tests_passed,
        "admin_review_blockers": 0,
        "hard_leakage_after": hard_leakage,
        "broad_leakage_after": broad_leakage,
        "duplicate_keys": duplicate_keys,
        "conflict_keys": conflict_keys,
        "hard_failures": hard_failures,
        "safe_to_deploy": False,
        "reason": "deployment intentionally manual" if not hard_failures else "; ".join(hard_failures),
    }
    (output_dir / "deployment_readiness_gate.json").write_text(json.dumps(gate, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# Deployment Readiness Gate",
        "",
        f"- Tests pass: {'yes' if tests_passed else 'no'}",
        f"- Controlled-category audit pass: {'yes' if gate['controlled_category_audit_pass'] else 'no'}",
        f"- Canonical refresh pass: {'yes' if canonical_pass else 'no'}",
        f"- Global Insights regenerated: {'yes' if gi_pass else 'no'}",
        f"- Hard leakage after: {hard_leakage}",
        f"- Broad leakage after: {broad_leakage}",
        f"- Duplicate/conflict keys: {duplicate_keys}/{conflict_keys}",
        "- Safe to deploy: no",
        "- Reason: deployment intentionally manual" if not hard_failures else f"- Reason: {'; '.join(hard_failures)}",
    ]
    (output_dir / "deployment_readiness_gate.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return gate


def summary_files(output_dir: Path, before: dict[str, Any], after: dict[str, Any], rule_metrics: dict[str, Any], gi: dict[str, Any], gate: dict[str, Any]) -> None:
    metrics = after.get("metrics") or {}
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "Batches 4-8 source/sample/environment consolidation refresh",
        "status": "pass" if not gate.get("hard_failures") else "fail",
        "canonical_snapshot_id": (after.get("provenance") or {}).get("snapshot_id", ""),
        "total_rows_scanned": metrics.get("total_rows_scanned", 0),
        "isolation_source_sd_present_percent": metrics.get("isolation_source_sd_present_percent", 0),
        "raw_present_standardization_percent": metrics.get("raw_present_isolation_source_standardization_percent", 0),
        "hard_leakage_after": metrics.get("hard_exact_leakage_rows", 0),
        "broad_leakage_after": metrics.get("non_approved_broad_rows", 0),
        "controlled_category_duplicate_keys": rule_metrics.get("duplicate_keys", 0),
        "controlled_category_conflict_keys": rule_metrics.get("conflict_keys", 0),
        "global_insights_snapshot_id": gi.get("snapshot_id", ""),
        "deployment_safe_to_deploy": gate.get("safe_to_deploy", False),
    }
    (output_dir / "batches_4_8_consolidation_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md = [
        "# Batches 4-8 Consolidation Summary",
        "",
        "Batches 4-8 consolidation refreshed source/sample/environment standardization after reviewed curation updates. Clear disease and health-state terms are routed into dedicated disease/health-state fields, while specimen materials, anatomical sites, environment terms, food terms, and generic clinical context are preserved in their appropriate fields. Ambiguous outbreak, surveillance, contaminated-food, carrier, colonized, and generic clinical terms remain admin-review unless explicit disease evidence is present.",
        "",
        "This consolidation does not infer disease from generic clinical context. Disease routing is conservative and evidence-based.",
        "",
        f"- Status: **{summary['status']}**",
        f"- Rows audited: {summary['total_rows_scanned']:,}",
        f"- Isolation_Source_SD coverage: {summary['isolation_source_sd_present_percent']}%",
        f"- Raw-present standardization: {summary['raw_present_standardization_percent']}%",
        f"- Hard leakage after: {summary['hard_leakage_after']}",
        f"- Broad leakage after: {summary['broad_leakage_after']}",
        f"- Controlled-category duplicate/conflict keys: {summary['controlled_category_duplicate_keys']}/{summary['controlled_category_conflict_keys']}",
        f"- Global Insights snapshot: `{summary['global_insights_snapshot_id']}`",
        "- Deployment: manual only; gate intentionally reports safe_to_deploy=false.",
    ]
    (output_dir / "batches_4_8_consolidation_summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--before-qa-summary", type=Path, default=DEFAULT_QA_ROOT / "20260612" / "source_sample_environment_qa_summary.json")
    parser.add_argument("--after-qa-summary", type=Path, default=None)
    parser.add_argument("--global-insights-summary", type=Path, default=None)
    parser.add_argument("--tests-passed", action="store_true")
    parser.add_argument("--fail-on-blockers", action="store_true")
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    before = load_json(args.before_qa_summary)
    after = load_json(args.after_qa_summary or latest_qa_summary())
    gi = load_json(args.global_insights_summary or (output_dir / "global_insights_regeneration_summary.json"))
    rule_metrics = controlled_category_audit(output_dir)
    disease_health_audit(output_dir)
    clinical_context_audit(output_dir)
    admin_review_remaining(output_dir)
    canonical_summary(output_dir, before, after)
    if args.global_insights_summary and args.global_insights_summary.exists():
        (output_dir / "global_insights_regeneration_summary.json").write_text(json.dumps(gi, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    elif not (output_dir / "global_insights_regeneration_summary.json").exists():
        (output_dir / "global_insights_regeneration_summary.json").write_text(json.dumps(gi, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    gate = deployment_gate(output_dir, rule_metrics, after, gi, args.tests_passed)
    summary_files(output_dir, before, after, rule_metrics, gi, gate)
    print(json.dumps(gate, sort_keys=True))
    return 1 if args.fail_on_blockers and gate.get("hard_failures") else 0


if __name__ == "__main__":
    raise SystemExit(main())
