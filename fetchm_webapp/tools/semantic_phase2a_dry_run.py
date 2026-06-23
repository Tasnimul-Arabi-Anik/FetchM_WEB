#!/usr/bin/env python3
"""Dry-run reviewed Phase 2A semantic-axis corrections.

This tool reads canonical standardized bacterial metadata, applies only the
reviewed Phase 2A allowlist to copied standardized payloads, exports projected
changes and validation artifacts, and never writes canonical tables.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import connect

DEFAULT_SNAPSHOT_ID = "20260602T140414Z_genbank_bacteria_root"
DEFAULT_RULES = ROOT / "standardization" / "semantic_phase2a_confirmed_rules.csv"
DEFAULT_OUTPUT_ROOT = ROOT / "standardization" / "review" / "host_clinical_site_semantics_phase2a_dry_run"
RULESET_VERSION = "semantic_phase2a_confirmed_rules_v1"
PROVENANCE_FIELD = "Semantic_Axis_Provenance"

ADDITIVE_FIELDS = [
    "Sample_Material_SD",
    "Sampling_Context_SD",
    "Sample_Processing_SD",
    "Sample_Collection_Device_SD",
    "Sample_Collection_Method_SD",
    "Sample_Entity_SD",
    "Data_Product_SD",
    "Host_Anatomical_Material_SD",
    "Host_Study_Group_SD",
    "Host_Hospitalization_Status_SD",
    "Host_Care_Setting_SD",
    "Host_Vital_Status_SD",
    "Host_Colonization_Status_SD",
    "Host_Disease_Stage_SD",
    "Host_Disease_Outcome_SD",
    "Host_Exposure_Context_SD",
]

LEGACY_COMPATIBILITY_FIELDS = [
    "Sample_Type_SD",
    "Sample_Type_SD_Broad",
    "Isolation_Source_SD",
    "Isolation_Source_SD_Broad",
]

PROTECTED_FIELDS = [
    "Host_SD",
    "Host_TaxID",
    "Host_Rank",
    "Host_Superkingdom",
    "Host_Phylum",
    "Host_Class",
    "Host_Order",
    "Host_Family",
    "Host_Genus",
    "Host_Species",
    "Country",
    "Continent",
    "Subcontinent",
    "Collection Date",
]

RAW_EVIDENCE_FIELDS = [
    "Host",
    "Host_Original",
    "Host_Cleaned",
    "Isolation Source",
    "Isolation Site",
    "Sample Type",
    "Collection Device",
    "Collection Method",
    "Host Disease",
    "Host Health State",
    "BioSample Host",
    "BioSample Host Disease",
    "BioSample Host Health State",
    "BioSample Isolation Source",
    "BioSample Isolation Site",
    "BioSample Body Site",
    "BioSample Tissue",
    "BioSample Tissue Type",
    "BioSample Host Tissue Sampled",
    "BioSample Collection Device",
    "BioSample Collection Method",
]

COMPANION_FIELDS = {
    "Host_Health_State_SD": [
        "Host_Health_State_SD_Broad",
        "Host_Health_State_SD_Detail",
        "Host_Health_State_SD_Method",
        "Host_Health_State_Ontology_ID",
    ],
    "Host_Disease_SD": [
        "Host_Disease_SD_Broad",
        "Host_Disease_SD_Detail",
        "Host_Disease_SD_Method",
        "Host_Disease_Ontology_ID",
    ],
    "Isolation_Site_SD": [
        "Isolation_Site_SD_Broad",
        "Isolation_Site_SD_Detail",
        "Isolation_Site_SD_Method",
        "Isolation_Site_Ontology_ID",
    ],
}

REQUIRED_RULE_COLUMNS = [
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


@dataclass(frozen=True)
class Rule:
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


def norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def present(value: Any) -> bool:
    text = norm(value)
    return bool(text) and text not in {"na", "n/a", "none", "unknown", "missing", "null", "not applicable"}


def read_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {"_non_object_standardized_payload": parsed}
        except json.JSONDecodeError:
            return {"_malformed_standardized_payload": value}
    return value or {}


def write_tsv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def load_rules(path: Path) -> list[Rule]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != REQUIRED_RULE_COLUMNS:
            raise ValueError(f"Unexpected rule columns: {reader.fieldnames}")
        rules = [
            Rule(
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


def validate_rules(rules: list[Rule]) -> None:
    ids = [rule.rule_id for rule in rules]
    duplicates = [rule_id for rule_id, count in Counter(ids).items() if count > 1]
    if duplicates:
        raise ValueError(f"Duplicate Phase 2A rule IDs: {duplicates}")
    for rule in rules:
        if rule.reviewer_status != "approved_phase2a_dry_run":
            raise ValueError(f"Rule {rule.rule_id} is not approved for Phase 2A dry-run")
        if rule.destination_field in LEGACY_COMPATIBILITY_FIELDS:
            raise ValueError(f"Rule {rule.rule_id} attempts to write legacy field {rule.destination_field}")
    by_destination: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for rule in rules:
        key = (rule.current_field, norm(rule.current_value), rule.destination_field)
        by_destination[key].add(norm(rule.destination_value))
    conflicts = {key: values for key, values in by_destination.items() if len(values) > 1}
    if conflicts:
        raise ValueError(f"Conflicting Phase 2A destination values: {conflicts}")


def load_records(snapshot_id: str) -> list[dict[str, Any]]:
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT i.assembly_accession,
                   COALESCE(m.organism_name, '') AS organism_name,
                   COALESCE(m.biosample_accession, '') AS biosample_accession,
                   s.standardized_payload
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            LEFT JOIN assembly_master AS m ON m.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
            """,
            (snapshot_id,),
        ).fetchall()
    return [
        {
            "assembly_accession": str(accession),
            "organism": str(organism or ""),
            "biosample": str(biosample or ""),
            "payload": read_payload(payload),
        }
        for accession, organism, biosample, payload in rows
    ]


def raw_text_blob(payload: dict[str, Any]) -> str:
    return " | ".join(str(payload.get(field) or "") for field in RAW_EVIDENCE_FIELDS)


def has_human_evidence(payload: dict[str, Any]) -> bool:
    if norm(payload.get("Host_TaxID")) == "9606":
        return True
    host_values = [payload.get("Host_SD"), payload.get("Host"), payload.get("Host_Original"), payload.get("Host_Cleaned"), payload.get("BioSample Host")]
    for value in host_values:
        text = norm(value)
        if text in {"human", "homo sapiens", "h. sapiens", "homo sapiens (human)"}:
            return True
    return False


def has_nonhuman_host_evidence(payload: dict[str, Any]) -> bool:
    if present(payload.get("Host_SD")) and not has_human_evidence(payload):
        return True
    text = norm(payload.get("Host") or payload.get("Host_Original") or payload.get("BioSample Host"))
    return bool(text and text not in {"patient", "human", "homo sapiens", "unknown"})


def has_host_evidence(payload: dict[str, Any]) -> bool:
    if has_human_evidence(payload) or has_nonhuman_host_evidence(payload):
        return True
    for field in ["Host_Context_SD", "Isolation_Source_SD_Broad", "Sample_Type_SD_Broad"]:
        text = norm(payload.get(field))
        if "host-associated" in text or "clinical" in text:
            return True
    return False


def has_plant_material_evidence(payload: dict[str, Any]) -> bool:
    blob = norm(raw_text_blob(payload))
    return bool(re.search(r"\b(?:plant-associated material|plant material|plant tissue|leaf tissue|root tissue|stem tissue|plant sample)\b", blob))


def has_collection_device_evidence(payload: dict[str, Any]) -> bool:
    for field in ["Collection Device", "BioSample Collection Device"]:
        if "catheter" in norm(payload.get(field)):
            return True
    for field in ["Sample Type", "BioSample Sample Type"]:
        text = norm(payload.get(field))
        if "catheter" in text and re.search(r"\b(?:device|catheter specimen|catheter sample)\b", text):
            return True
    return False


def condition_met(rule: Rule, payload: dict[str, Any]) -> tuple[bool, str]:
    condition = norm(rule.destination_condition)
    if condition == "always":
        return True, "condition=always"
    if condition == "human_host_evidence":
        return has_human_evidence(payload), "human_host_evidence=" + str(has_human_evidence(payload)).lower()
    if condition == "host_evidence":
        return has_host_evidence(payload), "host_evidence=" + str(has_host_evidence(payload)).lower()
    if condition == "plant_material_evidence":
        return has_plant_material_evidence(payload), "plant_material_evidence=" + str(has_plant_material_evidence(payload)).lower()
    if condition == "collection_device_evidence":
        return has_collection_device_evidence(payload), "collection_device_evidence=" + str(has_collection_device_evidence(payload)).lower()
    if condition == "blank_or_same":
        return True, "merge_policy=set_when_blank_or_same"
    return False, f"unknown_condition={rule.destination_condition}"


def provenance_entry(rule: Rule, payload: dict[str, Any], evidence: str) -> dict[str, str]:
    return {
        "rule_id": rule.rule_id,
        "source_current_field": rule.current_field,
        "source_current_value": rule.current_value,
        "source_raw_field": matched_raw_field(payload, rule.current_value)[0],
        "source_raw_value": matched_raw_field(payload, rule.current_value)[1],
        "method": "semantic_phase2a_dry_run",
        "confidence": rule.destination_confidence,
        "evidence": evidence,
        "evidence_requirement": rule.evidence_requirement,
        "ruleset_version": RULESET_VERSION,
        "source_audit_commit": rule.source_audit_commit,
    }


def matched_raw_field(payload: dict[str, Any], value: str) -> tuple[str, str]:
    wanted = norm(value)
    for field in RAW_EVIDENCE_FIELDS:
        raw_value = payload.get(field)
        if not present(raw_value):
            continue
        text = norm(raw_value)
        if text == wanted or wanted in text or text in wanted:
            return field, str(raw_value)
    return "", ""


def add_provenance(payload: dict[str, Any], field: str, entry: dict[str, str]) -> None:
    provenance = payload.get(PROVENANCE_FIELD)
    if not isinstance(provenance, dict):
        provenance = {}
    entries = provenance.setdefault(field, [])
    if entry not in entries:
        entries.append(entry)
    payload[PROVENANCE_FIELD] = provenance


def clear_field(payload: dict[str, Any], field: str) -> list[str]:
    cleared = []
    for target in [field, *COMPANION_FIELDS.get(field, [])]:
        if present(payload.get(target)):
            payload[target] = ""
            cleared.append(target)
    return cleared


def set_destination(payload: dict[str, Any], rule: Rule, evidence: str) -> tuple[str, str]:
    current = str(payload.get(rule.destination_field) or "").strip()
    if not current:
        payload[rule.destination_field] = rule.destination_value
        add_provenance(payload, rule.destination_field, provenance_entry(rule, payload, evidence))
        return "applied", ""
    if norm(current) == norm(rule.destination_value):
        add_provenance(payload, rule.destination_field, provenance_entry(rule, payload, evidence))
        return "noop", "same destination value already present"
    if norm(rule.merge_policy) == "set_when_blank_or_same":
        return "noop", f"existing nonblank destination preserved per merge policy; existing={current}; proposed={rule.destination_value}"
    return "conflict", f"existing={current}; proposed={rule.destination_value}"


def apply_rules_to_payload(payload: dict[str, Any], rules_by_current: dict[tuple[str, str], list[Rule]]) -> dict[str, Any]:
    before = deepcopy(payload)
    after = deepcopy(payload)
    applied: list[dict[str, str]] = []
    conflicts: list[dict[str, str]] = []
    evidence_failures: list[dict[str, str]] = []
    noops: list[dict[str, str]] = []
    cleared_fields: list[str] = []
    matched_rules: list[Rule] = []

    for key, rules in rules_by_current.items():
        current_field, current_value = key
        if norm(after.get(current_field)) != current_value:
            continue
        matched_rules.extend(rules)
        if any(rule.clear_current_field for rule in rules):
            cleared_fields.extend(clear_field(after, current_field))
        for rule in rules:
            ok, evidence = condition_met(rule, before)
            if not ok:
                evidence_failures.append({
                    "rule_id": rule.rule_id,
                    "field": rule.destination_field,
                    "value": rule.destination_value,
                    "condition": rule.destination_condition,
                    "evidence": evidence,
                })
                continue
            status, detail = set_destination(after, rule, evidence)
            event = {
                "rule_id": rule.rule_id,
                "field": rule.destination_field,
                "value": rule.destination_value,
                "condition": rule.destination_condition,
                "evidence": evidence,
                "detail": detail,
            }
            if status == "applied":
                applied.append(event)
            elif status == "noop":
                noops.append(event)
            else:
                conflicts.append(event)

    changed_fields = [field for field in sorted(set(before) | set(after)) if before.get(field, "") != after.get(field, "")]
    legacy_changed = [field for field in LEGACY_COMPATIBILITY_FIELDS if before.get(field, "") != after.get(field, "")]
    protected_changed = [field for field in PROTECTED_FIELDS if before.get(field, "") != after.get(field, "")]
    raw_changed = [field for field in RAW_EVIDENCE_FIELDS if before.get(field, "") != after.get(field, "")]
    return {
        "before": before,
        "after": after,
        "matched_rules": matched_rules,
        "applied": applied,
        "conflicts": conflicts,
        "evidence_failures": evidence_failures,
        "noops": noops,
        "cleared_fields": sorted(set(cleared_fields)),
        "changed_fields": changed_fields,
        "legacy_changed": legacy_changed,
        "protected_changed": protected_changed,
        "raw_changed": raw_changed,
    }


def rules_by_current(rules: list[Rule]) -> dict[tuple[str, str], list[Rule]]:
    grouped: dict[tuple[str, str], list[Rule]] = defaultdict(list)
    for rule in rules:
        grouped[(rule.current_field, norm(rule.current_value))].append(rule)
    return dict(grouped)


def compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def run_dry_run(snapshot_id: str, rules_path: Path, output_dir: Path, validation_examples_per_rule: int) -> dict[str, Any]:
    rules = load_rules(rules_path)
    grouped = rules_by_current(rules)
    records = load_records(snapshot_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    rules_counter: dict[str, Counter[str]] = {rule.rule_id: Counter() for rule in rules}
    value_counter: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    row_changes: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    evidence_failures: list[dict[str, Any]] = []
    noops: list[dict[str, Any]] = []
    patient_audit: list[dict[str, Any]] = []
    catheter_audit: list[dict[str, Any]] = []
    plant_audit: list[dict[str, Any]] = []
    validation_examples: list[dict[str, Any]] = []
    examples_per_rule: Counter[str] = Counter()
    before_coverage: Counter[str] = Counter()
    after_coverage: Counter[str] = Counter()
    legacy_changed_counter: Counter[str] = Counter()
    protected_changed_counter: Counter[str] = Counter()
    raw_changed_counter: Counter[str] = Counter()
    affected_accessions: set[str] = set()

    for record in records:
        payload = record["payload"]
        for field in ADDITIVE_FIELDS:
            if present(payload.get(field)):
                before_coverage[field] += 1
        result = apply_rules_to_payload(payload, grouped)
        after = result["after"]
        for field in ADDITIVE_FIELDS:
            if present(after.get(field)):
                after_coverage[field] += 1
        if not result["matched_rules"]:
            continue
        changed = bool(result["changed_fields"])
        if changed:
            affected_accessions.add(record["assembly_accession"])
        for rule in result["matched_rules"]:
            rules_counter[rule.rule_id]["matched_rows"] += 1
            value_counter[(rule.current_field, rule.current_value)]["matched_rows"] += 1
        for event in result["applied"]:
            rules_counter[event["rule_id"]]["applied_assignments"] += 1
            value_counter[(next(rule.current_field for rule in rules if rule.rule_id == event["rule_id"]), next(rule.current_value for rule in rules if rule.rule_id == event["rule_id"]))]["applied_assignments"] += 1
        for event in result["conflicts"]:
            rules_counter[event["rule_id"]]["destination_conflicts"] += 1
            conflicts.append(context_event(record, payload, event))
        for event in result["evidence_failures"]:
            rules_counter[event["rule_id"]]["evidence_failures"] += 1
            evidence_failures.append(context_event(record, payload, event))
        for event in result["noops"]:
            rules_counter[event["rule_id"]]["noops"] += 1
            noops.append(context_event(record, payload, event))
        for field in result["legacy_changed"]:
            legacy_changed_counter[field] += 1
        for field in result["protected_changed"]:
            protected_changed_counter[field] += 1
        for field in result["raw_changed"]:
            raw_changed_counter[field] += 1
        if norm(payload.get("Host_Health_State_SD")) == "patient":
            patient_audit.append(patient_context_row(record, payload, result))
        if norm(payload.get("Isolation_Site_SD")) == "catheter":
            catheter_audit.append(context_summary_row(record, payload, result, "catheter"))
        if norm(payload.get("Isolation_Site_SD")) == "plant-associated material":
            plant_audit.append(context_summary_row(record, payload, result, "plant-associated material"))
        if changed or result["conflicts"] or result["evidence_failures"] or result["noops"]:
            row_changes.append(row_change(record, result))
        for rule in result["matched_rules"]:
            if examples_per_rule[rule.rule_id] < validation_examples_per_rule:
                validation_examples.append(validation_example(record, payload, result, rule))
                examples_per_rule[rule.rule_id] += 1

    rules_rows = [rule_summary_row(rule, rules_counter[rule.rule_id]) for rule in rules]
    value_rows = [
        {
            "current_field": field,
            "current_value": value,
            "matched_rows": counts.get("matched_rows", 0),
            "applied_assignments": counts.get("applied_assignments", 0),
            "destination_conflicts": sum(rules_counter[rule.rule_id].get("destination_conflicts", 0) for rule in rules if rule.current_field == field and rule.current_value == value),
            "evidence_failures": sum(rules_counter[rule.rule_id].get("evidence_failures", 0) for rule in rules if rule.current_field == field and rule.current_value == value),
            "noops": sum(rules_counter[rule.rule_id].get("noops", 0) for rule in rules if rule.current_field == field and rule.current_value == value),
        }
        for (field, value), counts in sorted(value_counter.items())
    ]
    coverage_rows = [
        {
            "field": field,
            "before_present_rows": before_coverage.get(field, 0),
            "after_present_rows": after_coverage.get(field, 0),
            "delta": after_coverage.get(field, 0) - before_coverage.get(field, 0),
        }
        for field in ADDITIVE_FIELDS
    ]
    legacy_rows = [
        {
            "field": field,
            "changed_rows": legacy_changed_counter.get(field, 0),
            "status": "pass" if legacy_changed_counter.get(field, 0) == 0 else "fail",
        }
        for field in LEGACY_COMPATIBILITY_FIELDS
    ]

    write_outputs(
        output_dir,
        rules_rows,
        value_rows,
        row_changes,
        conflicts,
        evidence_failures,
        noops,
        patient_audit,
        catheter_audit,
        plant_audit,
        legacy_rows,
        coverage_rows,
        validation_examples,
    )
    summary = {
        "generated_at": utc_now(),
        "phase": "phase2a_dry_run_only",
        "canonical_snapshot_id": snapshot_id,
        "canonical_rows_scanned": len(records),
        "rules_path": str(rules_path),
        "rules_count": len(rules),
        "unique_assemblies_projected_to_change": len(affected_accessions),
        "assignments_projected_to_clear": sum(1 for row in row_changes for field in str(row.get("cleared_fields", "")).split("|") if field),
        "new_axis_assignments_projected": sum(row["delta"] for row in coverage_rows if row["field"] in ADDITIVE_FIELDS),
        "destination_conflicts": len(conflicts),
        "evidence_failures": len(evidence_failures),
        "noop_rows": len(noops),
        "legacy_field_changes": sum(legacy_changed_counter.values()),
        "protected_field_changes": sum(protected_changed_counter.values()),
        "raw_field_changes": sum(raw_changed_counter.values()),
        "rows_outside_reviewed_allowlist_affected": 0,
        "production_rules_changed": False,
        "canonical_write_run": False,
        "global_insights_regenerated": False,
        "deployment_run": False,
        "hard_failures": hard_failures(legacy_changed_counter, protected_changed_counter, raw_changed_counter, conflicts),
    }
    (output_dir / "phase2a_dry_run_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_summary_md(output_dir / "phase2a_dry_run_summary.md", summary)
    if summary["hard_failures"]:
        raise SystemExit("Phase 2A dry-run hard failures: " + json.dumps(summary["hard_failures"], sort_keys=True))
    return summary


def context_event(record: dict[str, Any], payload: dict[str, Any], event: dict[str, str]) -> dict[str, Any]:
    return {
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "rule_id": event.get("rule_id", ""),
        "field": event.get("field", ""),
        "value": event.get("value", ""),
        "condition": event.get("condition", ""),
        "evidence": event.get("evidence", ""),
        "detail": event.get("detail", ""),
        "Host_SD": payload.get("Host_SD", ""),
        "Host_TaxID": payload.get("Host_TaxID", ""),
        "Host_Context_SD": payload.get("Host_Context_SD", ""),
        "Host_Health_State_SD": payload.get("Host_Health_State_SD", ""),
        "Host_Disease_SD": payload.get("Host_Disease_SD", ""),
        "Isolation_Site_SD": payload.get("Isolation_Site_SD", ""),
        "Isolation_Source_SD": payload.get("Isolation_Source_SD", ""),
        "Sample_Type_SD": payload.get("Sample_Type_SD", ""),
    }


def patient_context_row(record: dict[str, Any], payload: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    after = result["after"]
    return {
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "Host_SD": payload.get("Host_SD", ""),
        "Host_TaxID": payload.get("Host_TaxID", ""),
        "raw_host": payload.get("Host", "") or payload.get("Host_Original", ""),
        "human_evidence": str(has_human_evidence(payload)).lower(),
        "nonhuman_host_evidence": str(has_nonhuman_host_evidence(payload)).lower(),
        "before_Host_Health_State_SD": payload.get("Host_Health_State_SD", ""),
        "after_Host_Health_State_SD": after.get("Host_Health_State_SD", ""),
        "after_Host_Context_SD": after.get("Host_Context_SD", ""),
        "after_Sampling_Context_SD": after.get("Sampling_Context_SD", ""),
        "evidence_failures": compact_json(result["evidence_failures"]),
    }


def context_summary_row(record: dict[str, Any], payload: dict[str, Any], result: dict[str, Any], value: str) -> dict[str, Any]:
    after = result["after"]
    return {
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "review_value": value,
        "Host_SD": payload.get("Host_SD", ""),
        "Host_Context_SD": payload.get("Host_Context_SD", ""),
        "before_Isolation_Site_SD": payload.get("Isolation_Site_SD", ""),
        "after_Isolation_Site_SD": after.get("Isolation_Site_SD", ""),
        "after_Sample_Material_SD": after.get("Sample_Material_SD", ""),
        "after_Sample_Collection_Device_SD": after.get("Sample_Collection_Device_SD", ""),
        "after_Host_Context_SD": after.get("Host_Context_SD", ""),
        "after_Sampling_Context_SD": after.get("Sampling_Context_SD", ""),
        "evidence_failures": compact_json(result["evidence_failures"]),
        "conflicts": compact_json(result["conflicts"]),
    }


def row_change(record: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    before = result["before"]
    after = result["after"]
    changed = result["changed_fields"]
    return {
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "changed_fields": "|".join(changed),
        "cleared_fields": "|".join(result["cleared_fields"]),
        "rules_applied": "|".join(sorted({event["rule_id"] for event in result["applied"]})),
        "conflict_count": len(result["conflicts"]),
        "evidence_failure_count": len(result["evidence_failures"]),
        "noop_count": len(result["noops"]),
        "legacy_changed": "|".join(result["legacy_changed"]),
        "protected_changed": "|".join(result["protected_changed"]),
        "raw_changed": "|".join(result["raw_changed"]),
        "before_values": compact_json({field: before.get(field, "") for field in changed}),
        "after_values": compact_json({field: after.get(field, "") for field in changed}),
    }


def validation_example(record: dict[str, Any], payload: dict[str, Any], result: dict[str, Any], rule: Rule) -> dict[str, Any]:
    after = result["after"]
    fields = sorted(set([rule.current_field, rule.destination_field, *COMPANION_FIELDS.get(rule.current_field, [])]))
    return {
        "rule_id": rule.rule_id,
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "current_field": rule.current_field,
        "current_value": rule.current_value,
        "destination_field": rule.destination_field,
        "destination_value": rule.destination_value,
        "before_values": compact_json({field: payload.get(field, "") for field in fields}),
        "after_values": compact_json({field: after.get(field, "") for field in fields}),
        "provenance": compact_json(after.get(PROVENANCE_FIELD, {}).get(rule.destination_field, [])),
        "conflicts": compact_json(result["conflicts"]),
        "evidence_failures": compact_json(result["evidence_failures"]),
    }


def rule_summary_row(rule: Rule, counts: Counter[str]) -> dict[str, Any]:
    return {
        "rule_id": rule.rule_id,
        "current_field": rule.current_field,
        "current_value": rule.current_value,
        "clear_current_field": str(rule.clear_current_field).lower(),
        "destination_field": rule.destination_field,
        "destination_value": rule.destination_value,
        "destination_condition": rule.destination_condition,
        "matched_rows": counts.get("matched_rows", 0),
        "applied_assignments": counts.get("applied_assignments", 0),
        "destination_conflicts": counts.get("destination_conflicts", 0),
        "evidence_failures": counts.get("evidence_failures", 0),
        "noops": counts.get("noops", 0),
        "removal_confidence": rule.removal_confidence,
        "destination_confidence": rule.destination_confidence,
        "rationale": rule.rationale,
    }


def hard_failures(legacy: Counter[str], protected: Counter[str], raw: Counter[str], conflicts: list[dict[str, Any]]) -> list[str]:
    failures = []
    if sum(legacy.values()):
        failures.append("legacy compatibility field changed")
    if sum(protected.values()):
        failures.append("protected host/geography/date field changed")
    if sum(raw.values()):
        failures.append("raw evidence field changed")
    if conflicts:
        failures.append("destination overwrite conflict")
    return failures


def write_outputs(
    output_dir: Path,
    rules_rows: list[dict[str, Any]],
    value_rows: list[dict[str, Any]],
    row_changes: list[dict[str, Any]],
    conflicts: list[dict[str, Any]],
    evidence_failures: list[dict[str, Any]],
    noops: list[dict[str, Any]],
    patient_audit: list[dict[str, Any]],
    catheter_audit: list[dict[str, Any]],
    plant_audit: list[dict[str, Any]],
    legacy_rows: list[dict[str, Any]],
    coverage_rows: list[dict[str, Any]],
    validation_examples: list[dict[str, Any]],
) -> None:
    write_tsv(output_dir / "phase2a_rules_applied.tsv", list(rules_rows[0].keys()) if rules_rows else [], rules_rows)
    write_tsv(output_dir / "phase2a_value_level_before_after.tsv", list(value_rows[0].keys()) if value_rows else ["current_field", "current_value"], value_rows)
    write_tsv(output_dir / "phase2a_projected_row_changes.tsv", list(row_changes[0].keys()) if row_changes else ["assembly_accession"], row_changes)
    event_header = ["assembly_accession", "biosample", "organism", "rule_id", "field", "value", "condition", "evidence", "detail", "Host_SD", "Host_TaxID", "Host_Context_SD", "Host_Health_State_SD", "Host_Disease_SD", "Isolation_Site_SD", "Isolation_Source_SD", "Sample_Type_SD"]
    write_tsv(output_dir / "phase2a_destination_conflicts.tsv", event_header, conflicts)
    write_tsv(output_dir / "phase2a_evidence_failures.tsv", event_header, evidence_failures)
    write_tsv(output_dir / "phase2a_destination_noops.tsv", event_header, noops)
    write_tsv(output_dir / "phase2a_patient_host_context_audit.tsv", list(patient_audit[0].keys()) if patient_audit else ["assembly_accession"], patient_audit)
    write_tsv(output_dir / "phase2a_catheter_context_audit.tsv", list(catheter_audit[0].keys()) if catheter_audit else ["assembly_accession"], catheter_audit)
    write_tsv(output_dir / "phase2a_plant_material_context_audit.tsv", list(plant_audit[0].keys()) if plant_audit else ["assembly_accession"], plant_audit)
    write_tsv(output_dir / "phase2a_legacy_field_preservation.tsv", ["field", "changed_rows", "status"], legacy_rows)
    write_tsv(output_dir / "phase2a_projected_field_coverage.tsv", ["field", "before_present_rows", "after_present_rows", "delta"], coverage_rows)
    write_tsv(output_dir / "phase2a_validation_examples.tsv", list(validation_examples[0].keys()) if validation_examples else ["rule_id"], validation_examples)


def write_summary_md(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Phase 2A Semantic Axis Dry Run",
        "",
        f"Generated: {summary['generated_at']}",
        f"Snapshot: `{summary['canonical_snapshot_id']}`",
        "",
        "## Scope",
        "",
        "Dry-run only. No canonical table writes, Global Insights regeneration, deployment, host taxonomy changes, geography/date changes, or legacy source/sample compatibility-field changes were performed.",
        "",
        "## Metrics",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Canonical rows scanned | {summary['canonical_rows_scanned']:,} |",
        f"| Rules in reviewed allowlist | {summary['rules_count']:,} |",
        f"| Unique assemblies projected to change | {summary['unique_assemblies_projected_to_change']:,} |",
        f"| Assignments projected to clear | {summary['assignments_projected_to_clear']:,} |",
        f"| New-axis assignments projected | {summary['new_axis_assignments_projected']:,} |",
        f"| Destination conflicts | {summary['destination_conflicts']:,} |",
        f"| Evidence failures | {summary['evidence_failures']:,} |",
        f"| No-op rows | {summary['noop_rows']:,} |",
        f"| Legacy compatibility field changes | {summary['legacy_field_changes']:,} |",
        f"| Protected/raw field changes | {summary['protected_field_changes'] + summary['raw_field_changes']:,} |",
        "",
        "## Gate",
        "",
        "Pass" if not summary["hard_failures"] else "Fail: " + ", ".join(summary["hard_failures"]),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--validation-examples-per-rule", type=int, default=5)
    args = parser.parse_args()
    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    output_dir = args.output_dir or DEFAULT_OUTPUT_ROOT / date
    summary = run_dry_run(args.snapshot_id, args.rules, output_dir, args.validation_examples_per_rule)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
