#!/usr/bin/env python3
"""Dry-run reviewed Semantic Closure Phase 1 corrections only.

This tool reads canonical bacterial standardized payloads, applies a finite
reviewed Phase 1 allowlist to copied payloads, writes compact projected-change
artifacts, and never writes canonical metadata.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import connect
from tools.semantic_phase2a_dry_run import (
    COMPANION_FIELDS,
    LEGACY_COMPATIBILITY_FIELDS,
    PROTECTED_FIELDS,
    PROVENANCE_FIELD,
    RAW_EVIDENCE_FIELDS,
    REMOVAL_PROVENANCE_KEY,
    compact_json,
    has_clinical_subject_evidence,
    has_collection_device_evidence,
    has_human_evidence,
    norm,
    present,
    read_payload,
    write_tsv,
)

DEFAULT_SNAPSHOT_ID = "20260602T140414Z_genbank_bacteria_root"
DEFAULT_RULES = ROOT / "standardization" / "semantic_closure_phase1_rules.csv"
DEFAULT_OUTPUT_ROOT = ROOT / "standardization" / "review" / "semantic_closure_phase1_dry_run"
RULESET_VERSION = "semantic_closure_phase1_rules_v1"
METHOD = "semantic_closure_phase1_dry_run"

RULE_COLUMNS = [
    "rule_id",
    "current_field",
    "current_value",
    "clear_current_field",
    "destination_field",
    "destination_value",
    "destination_condition",
    "removal_confidence",
    "destination_confidence",
    "merge_policy",
    "evidence_requirement",
    "preserve_legacy_fields",
    "reviewer_status",
    "rationale",
    "source_audit_commit",
]

ACTIVE_DESTINATION_FIELDS = [
    "Sample_Collection_Device_SD",
    "Host_Health_State_SD",
    "Host_Study_Group_SD",
    "Sampling_Context_SD",
    "Host_Context_SD",
    "Environment_Broad_Scale_SD",
    "Environment_Medium_SD",
    "Environment_Local_Scale_SD",
]

PROTECTED_CHANGE_FIELDS = set(PROTECTED_FIELDS) | set(RAW_EVIDENCE_FIELDS) | set(LEGACY_COMPATIBILITY_FIELDS)

SOURCE_FIELD_ORDER = {
    "Isolation_Site_SD": 10,
    "Host_Health_State_SD": 20,
    "Environment_Broad_Scale_SD": 30,
    "Environment_Local_Scale_SD": 40,
    "Environment_Medium_SD": 50,
}


@dataclass(frozen=True)
class ClosureRule:
    rule_id: str
    current_field: str
    current_value: str
    clear_current_field: bool
    destination_field: str
    destination_value: str
    destination_condition: str
    removal_confidence: str
    destination_confidence: str
    merge_policy: str
    evidence_requirement: str
    preserve_legacy_fields: bool
    reviewer_status: str
    rationale: str
    source_audit_commit: str


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def git_commit() -> str:
    configured = str(os.environ.get("FETCHM_WEBAPP_GIT_COMMIT") or "").strip()
    if configured:
        return configured
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT.parent, text=True, capture_output=True, check=False)
    return result.stdout.strip() or "unknown"


def load_rules(path: Path) -> list[ClosureRule]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != RULE_COLUMNS:
            raise ValueError(f"Unexpected Semantic Closure Phase 1 rule columns: {reader.fieldnames}")
        rules = [
            ClosureRule(
                rule_id=row["rule_id"],
                current_field=row["current_field"],
                current_value=row["current_value"],
                clear_current_field=norm(row["clear_current_field"]) == "true",
                destination_field=row["destination_field"],
                destination_value=row["destination_value"],
                destination_condition=row["destination_condition"],
                removal_confidence=row["removal_confidence"],
                destination_confidence=row["destination_confidence"],
                merge_policy=row["merge_policy"],
                evidence_requirement=row["evidence_requirement"],
                preserve_legacy_fields=norm(row["preserve_legacy_fields"]) == "true",
                reviewer_status=row["reviewer_status"],
                rationale=row["rationale"],
                source_audit_commit=row["source_audit_commit"],
            )
            for row in reader
        ]
    validate_rules(rules)
    return rules


def validate_rules(rules: list[ClosureRule]) -> None:
    ids = [rule.rule_id for rule in rules]
    duplicates = [rule_id for rule_id, count in Counter(ids).items() if count > 1]
    if duplicates:
        raise ValueError(f"Duplicate Semantic Closure Phase 1 rule IDs: {duplicates}")
    for rule in rules:
        if rule.reviewer_status != "approved_semantic_closure_phase1_dry_run":
            raise ValueError(f"Rule {rule.rule_id} is not approved for Phase 1 dry-run")
        if rule.destination_field in PROTECTED_CHANGE_FIELDS:
            raise ValueError(f"Rule {rule.rule_id} attempts to write protected/legacy/raw field {rule.destination_field}")
        if not rule.preserve_legacy_fields:
            raise ValueError(f"Rule {rule.rule_id} does not preserve legacy fields")
    destinations: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for rule in rules:
        if rule.destination_field:
            destinations[(rule.current_field, norm(rule.current_value), rule.destination_field)].add(norm(rule.destination_value))
    conflicts = {key: values for key, values in destinations.items() if len(values) > 1}
    if conflicts:
        raise ValueError(f"Conflicting destination values: {conflicts}")


def rules_by_current(rules: list[ClosureRule]) -> dict[tuple[str, str], list[ClosureRule]]:
    grouped: dict[tuple[str, str], list[ClosureRule]] = defaultdict(list)
    for rule in rules:
        grouped[(rule.current_field, norm(rule.current_value))].append(rule)
    return dict(grouped)


def iter_records(snapshot_id: str, chunk_size: int = 5000) -> Iterator[dict[str, Any]]:
    query = """
        SELECT i.assembly_accession,
               COALESCE(m.organism_name, '') AS organism_name,
               COALESCE(m.biosample_accession, '') AS biosample_accession,
               s.standardized_payload
        FROM bacterial_inventory_membership AS i
        JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
        LEFT JOIN assembly_master AS m ON m.assembly_accession = i.assembly_accession
        WHERE i.snapshot_id = %s
        ORDER BY i.assembly_accession
    """
    with connect() as connection:
        with connection.cursor(name="semantic_closure_phase1_stream") as cursor:
            cursor.itersize = max(1, int(chunk_size))
            cursor.execute(query, (snapshot_id,))
            for accession, organism, biosample, payload in cursor:
                yield {
                    "assembly_accession": str(accession),
                    "organism": str(organism or ""),
                    "biosample": str(biosample or ""),
                    "payload": read_payload(payload),
                }


def condition_met(rule: ClosureRule, payload: dict[str, Any]) -> tuple[bool, str, str]:
    condition = norm(rule.destination_condition)
    if not rule.destination_field:
        return True, "clear_only_rule=true", ""
    if condition == "always":
        return True, "condition=always", ""
    if condition == "blank_or_same":
        current = str(payload.get(rule.destination_field) or "").strip()
        if not present(current):
            return True, "destination_blank=true", ""
        if norm(current) == norm(rule.destination_value):
            return True, "destination_same=true", ""
        return False, f"existing_different={current}", "existing_different"
    if condition == "collection_device_evidence":
        ok = has_collection_device_evidence(payload)
        return ok, "collection_device_evidence=" + str(ok).lower(), "conditional_assignment_skip"
    if condition == "clinical_subject_evidence":
        ok = has_clinical_subject_evidence(payload)
        return ok, "clinical_subject_evidence=" + str(ok).lower(), "conditional_assignment_skip"
    if condition == "human_host_evidence":
        ok = has_human_evidence(payload)
        return ok, "human_host_evidence=" + str(ok).lower(), "conditional_assignment_skip"
    return False, f"unknown_condition={rule.destination_condition}", "unknown_condition_failure"


def provenance_entry(rule: ClosureRule, evidence: str) -> dict[str, Any]:
    return {
        "rule_id": rule.rule_id,
        "source_current_field": rule.current_field,
        "source_current_value": rule.current_value,
        "method": METHOD,
        "confidence": rule.destination_confidence,
        "evidence": evidence,
        "evidence_requirement": rule.evidence_requirement,
        "ruleset_version": RULESET_VERSION,
        "source_audit_commit": rule.source_audit_commit,
    }


def add_provenance(payload: dict[str, Any], field: str, entry: dict[str, Any]) -> None:
    provenance = payload.get(PROVENANCE_FIELD)
    if not isinstance(provenance, dict):
        provenance = {}
    entries = provenance.setdefault(field, [])
    if entry not in entries:
        entries.append(entry)
    payload[PROVENANCE_FIELD] = provenance


def clear_field(payload: dict[str, Any], field: str) -> list[dict[str, str]]:
    cleared = []
    for target in [field, *COMPANION_FIELDS.get(field, [])]:
        if present(payload.get(target)):
            previous = str(payload.get(target) or "")
            payload[target] = ""
            cleared.append({"field": target, "previous_value": previous, "is_primary": str(target == field).lower()})
    return cleared


def set_destination(payload: dict[str, Any], rule: ClosureRule, evidence: str) -> tuple[str, str]:
    if not rule.destination_field:
        return "clear_only", "no destination"
    current = str(payload.get(rule.destination_field) or "").strip()
    if not present(current):
        payload[rule.destination_field] = rule.destination_value
        add_provenance(payload, rule.destination_field, provenance_entry(rule, evidence))
        return "applied", ""
    if norm(current) == norm(rule.destination_value):
        add_provenance(payload, rule.destination_field, provenance_entry(rule, evidence))
        return "already_same", "same destination value already present"
    return "existing_different", f"existing={current}; proposed={rule.destination_value}"


def allowed_changed_fields(rules: list[ClosureRule]) -> set[str]:
    allowed = {PROVENANCE_FIELD}
    for rule in rules:
        if rule.clear_current_field:
            allowed.add(rule.current_field)
            allowed.update(COMPANION_FIELDS.get(rule.current_field, []))
        if rule.destination_field:
            allowed.add(rule.destination_field)
    return allowed


def add_removal_provenance(after: dict[str, Any], rule: ClosureRule, cleared: list[dict[str, str]], outcomes: list[dict[str, Any]]) -> dict[str, Any]:
    primary = next((item for item in cleared if item["is_primary"] == "true"), None)
    if not primary:
        return {}
    event = {
        "rule_id": rule.rule_id,
        "cleared_field": rule.current_field,
        "previous_value": primary["previous_value"],
        "cleared_companion_fields": [item for item in cleared if item["is_primary"] != "true"],
        "removal_confidence": rule.removal_confidence,
        "reason": rule.rationale,
        "destination_outcomes": outcomes,
        "destination_status": "removal_only" if not any(outcome.get("status") in {"applied", "already_same"} for outcome in outcomes) else "destination_recorded",
        "method": METHOD,
        "ruleset_version": RULESET_VERSION,
        "source_audit_commit": rule.source_audit_commit,
    }
    add_provenance(after, REMOVAL_PROVENANCE_KEY, event)
    return event


def apply_rules_to_payload(payload: dict[str, Any], grouped_rules: dict[tuple[str, str], list[ClosureRule]]) -> dict[str, Any]:
    before = deepcopy(payload)
    after = deepcopy(payload)
    matched_rules: list[ClosureRule] = []
    outcomes: list[dict[str, Any]] = []
    clears: list[dict[str, str]] = []
    removal_events: list[dict[str, Any]] = []

    ordered_groups = sorted(
        grouped_rules.items(),
        key=lambda item: (SOURCE_FIELD_ORDER.get(item[0][0], 100), item[0][0], item[0][1]),
    )
    for (field, value), rules in ordered_groups:
        if norm(before.get(field)) != value:
            continue
        matched_rules.extend(rules)
        clear_rule = next((rule for rule in rules if rule.clear_current_field), None)
        key_outcomes: list[dict[str, Any]] = []
        destination_clear_allowed = clear_rule is not None and not clear_rule.destination_field

        for rule in rules:
            ok, evidence, failure_kind = condition_met(rule, after)
            if not ok:
                event = {
                    "rule_id": rule.rule_id,
                    "field": rule.destination_field,
                    "value": rule.destination_value,
                    "condition": rule.destination_condition,
                    "evidence": evidence,
                    "status": failure_kind,
                    "detail": failure_kind,
                }
                outcomes.append(event)
                key_outcomes.append(event)
                continue
            if rule.destination_field == rule.current_field and rule.clear_current_field:
                key_clears = clear_field(after, field)
                clears.extend(key_clears)
                status, detail = set_destination(after, rule, evidence)
                event = {"rule_id": rule.rule_id, "field": rule.destination_field, "value": rule.destination_value, "condition": rule.destination_condition, "evidence": evidence, "status": status, "detail": detail}
                outcomes.append(event)
                key_outcomes.append(event)
                destination_clear_allowed = True
                if key_clears:
                    removal_events.append(add_removal_provenance(after, rule, key_clears, key_outcomes))
                continue
            status, detail = set_destination(after, rule, evidence)
            event = {"rule_id": rule.rule_id, "field": rule.destination_field, "value": rule.destination_value, "condition": rule.destination_condition, "evidence": evidence, "status": status, "detail": detail}
            outcomes.append(event)
            key_outcomes.append(event)
            if rule.clear_current_field and status in {"applied", "already_same", "clear_only"}:
                destination_clear_allowed = True

        if clear_rule is not None and clear_rule.destination_field != clear_rule.current_field and destination_clear_allowed:
            key_clears = clear_field(after, field)
            clears.extend(key_clears)
            if key_clears:
                removal_events.append(add_removal_provenance(after, clear_rule, key_clears, key_outcomes))

    changed_fields = [field for field in sorted(set(before) | set(after)) if before.get(field, "") != after.get(field, "")]
    allowed = allowed_changed_fields(matched_rules)
    return {
        "before": before,
        "after": after,
        "matched_rules": matched_rules,
        "outcomes": outcomes,
        "clears": clears,
        "removal_events": removal_events,
        "changed_fields": changed_fields,
        "legacy_changed": [field for field in LEGACY_COMPATIBILITY_FIELDS if before.get(field, "") != after.get(field, "")],
        "protected_changed": [field for field in PROTECTED_FIELDS if before.get(field, "") != after.get(field, "")],
        "raw_changed": [field for field in RAW_EVIDENCE_FIELDS if before.get(field, "") != after.get(field, "")],
        "unexpected_changed": sorted(set(changed_fields) - allowed) if matched_rules else [],
    }


def event_row(record: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    return {
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "rule_id": event.get("rule_id", ""),
        "field": event.get("field", ""),
        "value": event.get("value", ""),
        "condition": event.get("condition", ""),
        "evidence": event.get("evidence", ""),
        "status": event.get("status", ""),
        "detail": event.get("detail", ""),
    }


def clear_row(record: dict[str, Any], clear: dict[str, str], rule_id: str) -> dict[str, Any]:
    return {
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "rule_id": rule_id,
        "field": clear.get("field", ""),
        "previous_value": clear.get("previous_value", ""),
        "is_primary": clear.get("is_primary", ""),
    }


def change_row(record: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    return {
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "changed_fields": ";".join(result["changed_fields"]),
        "matched_rules": ";".join(rule.rule_id for rule in result["matched_rules"]),
        "outcomes": compact_json(result["outcomes"]),
    }


def run_dry_run(snapshot_id: str, rules_path: Path, output_dir: Path, *, example_limit: int = 5000, chunk_size: int = 5000) -> dict[str, Any]:
    rules = load_rules(rules_path)
    grouped = rules_by_current(rules)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows_scanned = 0
    changed_accessions: set[str] = set()
    rule_counts: dict[str, Counter[str]] = {rule.rule_id: Counter() for rule in rules}
    field_change_counts: Counter[str] = Counter()
    new_axis_counts: Counter[str] = Counter()
    clear_counts: Counter[str] = Counter()
    provenance_counts: Counter[str] = Counter()
    legacy_counts: Counter[str] = Counter()
    protected_counts: Counter[str] = Counter()
    raw_counts: Counter[str] = Counter()
    unexpected_counts: Counter[str] = Counter()
    outcome_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    projected_changes: list[dict[str, Any]] = []
    projected_clears: list[dict[str, Any]] = []
    examples: list[dict[str, Any]] = []
    removal_without_provenance = 0
    second_pass_changes = 0

    for record in iter_records(snapshot_id, chunk_size=chunk_size):
        rows_scanned += 1
        result = apply_rules_to_payload(record["payload"], grouped)
        if not result["matched_rules"]:
            continue
        if result["changed_fields"]:
            changed_accessions.add(record["assembly_accession"])
            if len(projected_changes) < example_limit:
                projected_changes.append(change_row(record, result))
        for rule in result["matched_rules"]:
            rule_counts[rule.rule_id]["matched_rows"] += 1
        for field in result["changed_fields"]:
            field_change_counts[field] += 1
        for event in result["outcomes"]:
            status = str(event.get("status") or "")
            rule_id = str(event.get("rule_id") or "")
            rule_counts[rule_id][status] += 1
            if status == "applied":
                new_axis_counts[str(event.get("field") or "")] += 1
            if len(outcome_rows[status]) < example_limit:
                outcome_rows[status].append(event_row(record, event))
        primary_rule = result["matched_rules"][0].rule_id if result["matched_rules"] else ""
        for clear in result["clears"]:
            clear_counts[clear.get("field", "")] += 1
            if len(projected_clears) < example_limit:
                projected_clears.append(clear_row(record, clear, primary_rule))
        if result["clears"] and not result["removal_events"]:
            removal_without_provenance += 1
        for event in result["removal_events"]:
            if event:
                provenance_counts[str(event.get("cleared_field") or REMOVAL_PROVENANCE_KEY)] += 1
        for field in result["legacy_changed"]:
            legacy_counts[field] += 1
        for field in result["protected_changed"]:
            protected_counts[field] += 1
        for field in result["raw_changed"]:
            raw_counts[field] += 1
        for field in result["unexpected_changed"]:
            unexpected_counts[field] += 1
        if len(examples) < example_limit:
            examples.append(change_row(record, result))
        second = apply_rules_to_payload(result["after"], grouped)
        if second["changed_fields"]:
            second_pass_changes += 1

    rules_rows = [
        {
            "rule_id": rule.rule_id,
            "current_field": rule.current_field,
            "current_value": rule.current_value,
            "destination_field": rule.destination_field,
            "destination_value": rule.destination_value,
            "matched_rows": rule_counts[rule.rule_id].get("matched_rows", 0),
            "applied": rule_counts[rule.rule_id].get("applied", 0),
            "already_same": rule_counts[rule.rule_id].get("already_same", 0),
            "existing_different": rule_counts[rule.rule_id].get("existing_different", 0),
            "conditional_assignment_skip": rule_counts[rule.rule_id].get("conditional_assignment_skip", 0),
            "clear_only": rule_counts[rule.rule_id].get("clear_only", 0),
        }
        for rule in rules
    ]
    field_rows = [{"field": field, "projected_changed_rows": count} for field, count in sorted(field_change_counts.items())]
    new_axis_rows = [{"field": field, "projected_assignments": count} for field, count in sorted(new_axis_counts.items()) if field]
    provenance_rows = [{"field": field, "events": count} for field, count in sorted(provenance_counts.items())]
    legacy_rows = [
        {"field": field, "changed_rows": legacy_counts.get(field, 0), "status": "pass" if legacy_counts.get(field, 0) == 0 else "fail"}
        for field in LEGACY_COMPATIBILITY_FIELDS
    ]
    remaining_unresolved = [
        {"queue": "strict_field_violation_assignment_occurrences", "assignment_occurrences": 910373, "note": "from final_semantic_closure/20260701_audit; Phase 1 dry-run intentionally applies only reviewed allowlist"},
        {"queue": "ambiguous_or_review_assignment_occurrences", "assignment_occurrences": 4199315, "note": "not applied in Phase 1"},
        {"queue": "preserved_compatibility_or_conflict_assignment_occurrences", "assignment_occurrences": 190318, "note": "not applied in Phase 1"},
    ]

    write_tsv(output_dir / "reviewed_rules.tsv", RULE_COLUMNS, [rule.__dict__ for rule in rules])
    write_tsv(output_dir / "rule_level_before_after.tsv", list(rules_rows[0]) if rules_rows else ["rule_id"], rules_rows)
    write_tsv(output_dir / "projected_field_changes.tsv", ["field", "projected_changed_rows"], field_rows)
    write_tsv(output_dir / "projected_new_axis_assignments.tsv", ["field", "projected_assignments"], new_axis_rows)
    write_tsv(output_dir / "projected_clears.tsv", ["assembly_accession", "biosample", "organism", "rule_id", "field", "previous_value", "is_primary"], projected_clears)
    write_tsv(output_dir / "destination_conflicts.tsv", ["assembly_accession", "biosample", "organism", "rule_id", "field", "value", "condition", "evidence", "status", "detail"], outcome_rows.get("conflict", []))
    write_tsv(output_dir / "skipped_existing_different_destinations.tsv", ["assembly_accession", "biosample", "organism", "rule_id", "field", "value", "condition", "evidence", "status", "detail"], outcome_rows.get("existing_different", []))
    write_tsv(output_dir / "preserved_legacy_fields.tsv", ["field", "changed_rows", "status"], legacy_rows)
    write_tsv(output_dir / "provenance_summary.tsv", ["field", "events"], provenance_rows)
    write_tsv(output_dir / "representative_examples.tsv", ["assembly_accession", "biosample", "organism", "changed_fields", "matched_rules", "outcomes"], examples)
    write_tsv(output_dir / "remaining_unresolved_strict_signals.tsv", ["queue", "assignment_occurrences", "note"], remaining_unresolved)

    hard_failures = {
        "raw_changes": sum(raw_counts.values()),
        "legacy_compatibility_field_changes": sum(legacy_counts.values()),
        "host_taxonomy_changes": sum(protected_counts[field] for field in PROTECTED_FIELDS if field.startswith("Host_")),
        "geography_date_changes": sum(protected_counts[field] for field in ["Country", "Continent", "Subcontinent", "Collection Date"]),
        "destination_overwrite_conflicts": len(outcome_rows.get("conflict", [])),
        "outside_allowlist_changes": sum(unexpected_counts.values()),
        "missing_provenance": removal_without_provenance,
        "second_pass_changes": second_pass_changes,
    }
    promotion_gate = {
        "safe_to_apply": False,
        "reason": "manual review required before canonical write",
        "canonical_write_authorized": False,
        "global_insights_regeneration_authorized": False,
        "deployment_authorized": False,
    }
    idempotency = {"second_pass_changes": second_pass_changes, "status": "pass" if second_pass_changes == 0 else "fail"}
    total_destination_conflicts = sum(counts.get("conflict", 0) for counts in rule_counts.values())
    total_existing_different = sum(counts.get("existing_different", 0) for counts in rule_counts.values())
    total_conditional_skips = sum(counts.get("conditional_assignment_skip", 0) for counts in rule_counts.values())
    summary = {
        "generated_at": utc_now(),
        "phase": "semantic_closure_phase1_dry_run_only",
        "snapshot_id": snapshot_id,
        "implementation_commit": git_commit(),
        "rules_path": str(rules_path),
        "rules_count": len(rules),
        "canonical_rows_scanned": rows_scanned,
        "projected_rows_changed": len(changed_accessions),
        "projected_changed_field_assignments": sum(field_change_counts.values()),
        "projected_new_axis_assignments": sum(new_axis_counts.values()),
        "projected_clears": sum(clear_counts.values()),
        "destination_conflicts": total_destination_conflicts,
        "skipped_existing_different_destinations": total_existing_different,
        "conditional_assignment_skips": total_conditional_skips,
        "raw_changes": hard_failures["raw_changes"],
        "legacy_compatibility_field_changes": hard_failures["legacy_compatibility_field_changes"],
        "host_taxonomy_changes": hard_failures["host_taxonomy_changes"],
        "geography_date_changes": hard_failures["geography_date_changes"],
        "outside_allowlist_changes": hard_failures["outside_allowlist_changes"],
        "missing_provenance": hard_failures["missing_provenance"],
        "second_pass_changes": second_pass_changes,
        "hard_failures": hard_failures,
        "production_rules_changed": False,
        "canonical_write_run": False,
        "global_insights_regenerated": False,
        "deployment_run": False,
        "promotion_gate": promotion_gate,
    }
    (output_dir / "semantic_closure_phase1_dry_run_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / "idempotency_summary.json").write_text(json.dumps(idempotency, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / "promotion_gate.json").write_text(json.dumps(promotion_gate, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / "semantic_closure_phase1_dry_run_summary.md").write_text(summary_markdown(summary), encoding="utf-8")
    return summary


def summary_markdown(summary: dict[str, Any]) -> str:
    return (
        "# Semantic Closure Phase 1 Dry Run\n\n"
        f"- Snapshot: `{summary['snapshot_id']}`\n"
        f"- Rows scanned: {summary['canonical_rows_scanned']:,}\n"
        f"- Rules: {summary['rules_count']:,}\n"
        f"- Projected rows changed: {summary['projected_rows_changed']:,}\n"
        f"- Projected clears: {summary['projected_clears']:,}\n"
        f"- Projected new-axis assignments: {summary['projected_new_axis_assignments']:,}\n"
        f"- Destination conflicts: {summary['destination_conflicts']:,}\n"
        f"- Existing-different destinations skipped: {summary['skipped_existing_different_destinations']:,}\n"
        f"- Second-pass changes: {summary['second_pass_changes']:,}\n"
        "- Canonical write run: false\n"
        "- Global Insights regenerated: false\n"
        "- Deployment run: false\n\n"
        "## Promotion Gate\n\n"
        "`safe_to_apply = false` - manual review required before canonical write.\n"
    )


def default_output_dir() -> Path:
    return DEFAULT_OUTPUT_ROOT / datetime.now(timezone.utc).strftime("%Y%m%d")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--example-limit", type=int, default=5000)
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--fail-on-hard-errors", action="store_true")
    args = parser.parse_args(argv)
    output_dir = args.output_dir or default_output_dir()
    summary = run_dry_run(
        args.snapshot_id,
        args.rules,
        output_dir,
        example_limit=max(1, int(args.example_limit)),
        chunk_size=max(1, int(args.chunk_size)),
    )
    print(json.dumps({
        "snapshot_id": summary["snapshot_id"],
        "canonical_rows_scanned": summary["canonical_rows_scanned"],
        "projected_rows_changed": summary["projected_rows_changed"],
        "destination_conflicts": summary["destination_conflicts"],
        "second_pass_changes": summary["second_pass_changes"],
        "safe_to_apply": summary["promotion_gate"]["safe_to_apply"],
        "output_dir": str(output_dir),
    }, sort_keys=True))
    if args.fail_on_hard_errors and any(summary["hard_failures"].values()):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
