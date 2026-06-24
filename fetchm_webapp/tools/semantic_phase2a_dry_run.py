#!/usr/bin/env python3
"""Dry-run reviewed Phase 2A semantic-axis corrections.

This tool reads canonical standardized bacterial metadata, applies only the
reviewed Phase 2A allowlist to copied standardized payloads, exports projected
changes and validation artifacts, and never writes canonical tables.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
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
RULESET_VERSION = "semantic_phase2a_confirmed_rules_v2"
PROVENANCE_FIELD = "Semantic_Axis_Provenance"
REMOVAL_PROVENANCE_KEY = "_cleared_strict_fields"

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

CONTEXT_EVIDENCE_FIELDS = [
    *RAW_EVIDENCE_FIELDS,
    "Host_SD",
    "Host_TaxID",
    "Host_Context_SD",
    "Sample_Type_SD",
    "Sample_Type_SD_Broad",
    "Isolation_Source_SD",
    "Isolation_Source_SD_Broad",
    "Environment_Medium_SD",
    "Environment_Broad_Scale_SD",
    "Environment_Local_Scale_SD",
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

# Mirrors the central app metadata missing-token policy without importing the
# Flask application stack into this standalone audit tool.
MISSING_VALUE_TOKENS = {
    "",
    "-",
    "na",
    "n a",
    "n/a",
    "nan",
    "none",
    "null",
    "missing",
    "mising",
    "misisng",
    "absent",
    "unknown",
    "not known",
    "not available",
    "unavailable",
    "not provided",
    "not collected",
    "not applicable",
    "not recorded",
    "not determined",
    "restricted access",
    "no host",
    "no data",
}

ENVIRONMENT_ONLY_RE = re.compile(
    r"\b(?:wastewater|waste water|sewage|sewer|effluent|influent|sludge|sink|drain|"
    r"surface|environmental|environment|water|soil|sediment|biofilm|hospital environment|"
    r"healthcare environment|facility surface)\b",
    re.I,
)
ENVIRONMENT_EXCLUSION_RE = re.compile(r"\b(?:wastewater|waste water|sewage|sink|drain|surface|environmental|environment)\b", re.I)
CLINICAL_SPECIMEN_RE = re.compile(
    r"\b(?:clinical sample|clinical specimen|patient sample|patient specimen|human specimen|"
    r"veterinary specimen|veterinary sample|blood|urine|feces|faeces|stool|swab|nasal|"
    r"rectal|wound|tissue|biopsy|aspirate|sputum|cerebrospinal fluid|csf|pus|milk|"
    r"serum|plasma|saliva|respiratory sample)\b",
    re.I,
)
PATIENT_OR_HUMAN_RE = re.compile(r"\b(?:patient|human|homo sapiens|h\. sapiens)\b", re.I)
ANIMAL_HOST_RE = re.compile(
    r"\b(?:aves|bird|bos|bovine|cow|cattle|calf|taurus|gallus|chicken|turkey|sus|pig|"
    r"swine|ovis|sheep|capra|goat|equus|horse|canis|dog|felis|cat|fish|oyster|shrimp|"
    r"shellfish|mouse|mus musculus|rattus|rat)\b",
    re.I,
)
PLANT_CONTEXT_RE = re.compile(
    r"\b(?:plant|plant-associated|plant associated|plant material|plant tissue|leaf|root|stem|"
    r"seed|fruit|flower|rhizosphere|phyllosphere|endosphere|viridiplantae|arabidopsis|"
    r"oryza|rice|zea|maize|corn|glycine|soybean|solanum|tomato|spinach|lettuce|citrus|"
    r"medicago|triticum|wheat|combretum|campylopus)\b",
    re.I,
)
PLANT_MATERIAL_RE = re.compile(
    r"\b(?:plant material|plant tissue|leaf tissue|root tissue|stem tissue|seed tissue|"
    r"fruit tissue|flower tissue|plant sample|leaf sample|root sample|stem sample)\b",
    re.I,
)


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


def normalize_lookup(value: Any) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"\[[A-Za-z]+:\d+\]", " ", text)
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def present(value: Any) -> bool:
    text = norm(value)
    normalized = normalize_lookup(value)
    if not text or normalized in MISSING_VALUE_TOKENS or text in MISSING_VALUE_TOKENS:
        return False
    missing_prefixes = (
        "missing",
        "no collected",
        "not collect",
        "not applicable",
        "not available",
        "not collected",
        "not provided",
        "not recorded",
        "not determined",
        "unavailable",
        "unidentified",
        "unknown",
    )
    if re.match(r"^\d+\s*(not applicable|not available|not collected|not provided|unknown)\b", normalized):
        return False
    return not normalized.startswith(missing_prefixes)


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
    blocked_destination_fields = set(LEGACY_COMPATIBILITY_FIELDS) | set(PROTECTED_FIELDS) | set(RAW_EVIDENCE_FIELDS)
    for rule in rules:
        if rule.reviewer_status != "approved_phase2a_dry_run":
            raise ValueError(f"Rule {rule.rule_id} is not approved for Phase 2A dry-run")
        if rule.destination_field in blocked_destination_fields:
            raise ValueError(f"Rule {rule.rule_id} attempts to write protected or legacy field {rule.destination_field}")
    by_destination: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for rule in rules:
        if not rule.destination_field:
            continue
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


def text_blob(payload: dict[str, Any], fields: Iterable[str]) -> str:
    return " | ".join(str(payload.get(field) or "") for field in fields if present(payload.get(field)))


def raw_text_blob(payload: dict[str, Any]) -> str:
    return text_blob(payload, RAW_EVIDENCE_FIELDS)


def standardized_context_blob(payload: dict[str, Any]) -> str:
    return text_blob(payload, CONTEXT_EVIDENCE_FIELDS)


def has_human_evidence(payload: dict[str, Any]) -> bool:
    if normalize_lookup(payload.get("Host_TaxID")) == "9606":
        return True
    for field in ["Host_SD", "Host", "Host_Original", "Host_Cleaned", "BioSample Host"]:
        text = normalize_lookup(payload.get(field))
        if text in {"human", "homo sapiens", "h sapiens", "homo sapiens human"}:
            return True
    return False


def environment_only_context(payload: dict[str, Any]) -> bool:
    blob = standardized_context_blob(payload)
    if not ENVIRONMENT_ONLY_RE.search(blob):
        return False
    return not bool(CLINICAL_SPECIMEN_RE.search(blob) or PATIENT_OR_HUMAN_RE.search(raw_text_blob(payload)))


def explicit_clinical_specimen_context(payload: dict[str, Any]) -> bool:
    blob = standardized_context_blob(payload)
    if not CLINICAL_SPECIMEN_RE.search(blob):
        return False
    if ENVIRONMENT_EXCLUSION_RE.search(blob) and not PATIENT_OR_HUMAN_RE.search(blob):
        return False
    return True


def has_nonhuman_host_evidence(payload: dict[str, Any]) -> bool:
    if has_human_evidence(payload):
        return False
    host_sd = normalize_lookup(payload.get("Host_SD"))
    host_taxid = normalize_lookup(payload.get("Host_TaxID"))
    if host_sd and present(host_sd) and host_taxid and host_taxid != "9606":
        if not ENVIRONMENT_EXCLUSION_RE.search(host_sd) and host_sd not in {"sample", "environmental", "environment", "patient"}:
            return True
    context = normalize_lookup(payload.get("Host_Context_SD"))
    if context in {"animal associated", "plant associated", "host associated"}:
        return True
    return False


def has_host_evidence(payload: dict[str, Any]) -> bool:
    if has_human_evidence(payload) or has_nonhuman_host_evidence(payload):
        return True
    for field in ["Host_Context_SD", "Isolation_Source_SD_Broad", "Sample_Type_SD_Broad"]:
        text = norm(payload.get(field))
        if "host-associated" in text or "clinical" in text:
            return True
    return False


def has_clinical_subject_evidence(payload: dict[str, Any]) -> bool:
    raw_host_blob = text_blob(payload, ["Host", "Host_Original", "Host_Cleaned", "BioSample Host"])
    if PATIENT_OR_HUMAN_RE.search(raw_host_blob):
        return True
    if environment_only_context(payload):
        return False
    raw_blob = raw_text_blob(payload)
    if re.search(r"\bpatient\b", raw_blob, re.I) and CLINICAL_SPECIMEN_RE.search(raw_blob):
        return True
    if has_human_evidence(payload) and explicit_clinical_specimen_context(payload):
        return True
    if has_nonhuman_host_evidence(payload) and explicit_clinical_specimen_context(payload):
        return True
    return False


def has_animal_host_evidence(payload: dict[str, Any]) -> bool:
    host_blob = text_blob(payload, ["Host_SD", "Host", "Host_Original", "Host_Cleaned", "BioSample Host"])
    return bool(ANIMAL_HOST_RE.search(host_blob))


def has_plant_host_evidence(payload: dict[str, Any]) -> bool:
    host_blob = text_blob(payload, ["Host_SD", "Host", "Host_Original", "Host_Cleaned", "BioSample Host"])
    if not host_blob or has_animal_host_evidence(payload) or has_human_evidence(payload):
        return False
    return bool(PLANT_CONTEXT_RE.search(host_blob))


def has_plant_context_evidence(payload: dict[str, Any]) -> bool:
    if has_animal_host_evidence(payload) or has_human_evidence(payload):
        raw_blob = raw_text_blob(payload)
        return bool(PLANT_CONTEXT_RE.search(raw_blob) and not ENVIRONMENT_EXCLUSION_RE.search(raw_blob))
    if has_plant_host_evidence(payload):
        return True
    raw_or_source_blob = text_blob(
        payload,
        [
            "Isolation Source",
            "Sample Type",
            "BioSample Isolation Source",
            "BioSample Tissue",
            "BioSample Tissue Type",
            "BioSample Host Tissue Sampled",
            "Isolation_Source_SD",
            "Environment_Local_Scale_SD",
            "Environment_Broad_Scale_SD",
            "Host_Context_SD",
        ],
    )
    if ENVIRONMENT_EXCLUSION_RE.search(raw_or_source_blob) and not PLANT_CONTEXT_RE.search(raw_text_blob(payload)):
        return False
    return bool(PLANT_CONTEXT_RE.search(raw_or_source_blob))


def has_plant_material_evidence(payload: dict[str, Any]) -> bool:
    blob = text_blob(
        payload,
        [
            "Isolation Source",
            "Sample Type",
            "BioSample Isolation Source",
            "BioSample Tissue",
            "BioSample Tissue Type",
            "BioSample Host Tissue Sampled",
            "Isolation_Source_SD",
            "Sample_Type_SD",
        ],
    )
    return bool(PLANT_MATERIAL_RE.search(blob))


def has_collection_device_evidence(payload: dict[str, Any]) -> bool:
    for field in ["Collection Device", "BioSample Collection Device"]:
        if "catheter" in norm(payload.get(field)):
            return True
    for field in ["Sample Type", "BioSample Sample Type"]:
        text = norm(payload.get(field))
        if "catheter" in text and re.search(r"\b(?:device|catheter specimen|catheter sample)\b", text):
            return True
    return False


def condition_met(rule: Rule, payload: dict[str, Any]) -> tuple[bool, str, str]:
    condition = norm(rule.destination_condition)
    if not rule.destination_field:
        return True, "clear_only_rule=true", ""
    if condition == "always":
        return True, "condition=always", ""
    if condition == "human_host_evidence":
        ok = has_human_evidence(payload)
        return ok, "human_host_evidence=" + str(ok).lower(), "conditional_assignment_skip"
    if condition == "clinical_subject_evidence":
        ok = has_clinical_subject_evidence(payload)
        return ok, "clinical_subject_evidence=" + str(ok).lower(), "conditional_assignment_skip"
    if condition == "host_evidence":
        ok = has_host_evidence(payload)
        return ok, "host_evidence=" + str(ok).lower(), "conditional_assignment_skip"
    if condition == "plant_context_evidence":
        ok = has_plant_context_evidence(payload)
        return ok, "plant_context_evidence=" + str(ok).lower(), "conditional_assignment_skip"
    if condition == "plant_material_evidence":
        ok = has_plant_material_evidence(payload)
        return ok, "plant_material_evidence=" + str(ok).lower(), "conditional_assignment_skip"
    if condition == "collection_device_evidence":
        ok = has_collection_device_evidence(payload)
        return ok, "collection_device_evidence=" + str(ok).lower(), "conditional_assignment_skip"
    if condition == "blank_or_same":
        return True, "merge_policy=set_when_blank_or_same", ""
    if condition == "destination_blank":
        ok = not present(payload.get(rule.destination_field))
        return ok, "destination_blank=" + str(ok).lower(), "conditional_assignment_skip"
    return False, f"unknown_condition={rule.destination_condition}", "unknown_condition_failure"


def matched_raw_field(payload: dict[str, Any], value: str) -> tuple[str, str]:
    wanted = normalize_lookup(value)
    for field in RAW_EVIDENCE_FIELDS:
        raw_value = payload.get(field)
        if not present(raw_value):
            continue
        text = normalize_lookup(raw_value)
        if text == wanted or wanted in text or text in wanted:
            return field, str(raw_value)
    return "", ""


def provenance_entry(rule: Rule, payload: dict[str, Any], evidence: str) -> dict[str, str]:
    source_field, source_value = matched_raw_field(payload, rule.current_value)
    return {
        "rule_id": rule.rule_id,
        "source_current_field": rule.current_field,
        "source_current_value": rule.current_value,
        "source_raw_field": source_field,
        "source_raw_value": source_value,
        "method": "semantic_phase2a_dry_run",
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


def set_destination(payload: dict[str, Any], rule: Rule, evidence: str) -> tuple[str, str]:
    if not rule.destination_field:
        return "clear_only", "no destination for clear-only rule"
    current = str(payload.get(rule.destination_field) or "").strip()
    if not current:
        payload[rule.destination_field] = rule.destination_value
        add_provenance(payload, rule.destination_field, provenance_entry(rule, payload, evidence))
        return "applied", ""
    if norm(current) == norm(rule.destination_value):
        add_provenance(payload, rule.destination_field, provenance_entry(rule, payload, evidence))
        return "already_same", "same destination value already present"
    if norm(rule.merge_policy) == "set_when_blank_or_same":
        return "existing_different", f"existing nonblank destination preserved per merge policy; existing={current}; proposed={rule.destination_value}"
    return "conflict", f"existing={current}; proposed={rule.destination_value}"


def rules_by_current(rules: list[Rule]) -> dict[tuple[str, str], list[Rule]]:
    grouped: dict[tuple[str, str], list[Rule]] = defaultdict(list)
    for rule in rules:
        grouped[(rule.current_field, norm(rule.current_value))].append(rule)
    return dict(grouped)


def compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def allowed_changed_fields(matched_rules: list[Rule]) -> set[str]:
    allowed = {PROVENANCE_FIELD}
    for rule in matched_rules:
        if rule.clear_current_field:
            allowed.add(rule.current_field)
            allowed.update(COMPANION_FIELDS.get(rule.current_field, []))
        if rule.destination_field:
            allowed.add(rule.destination_field)
    return allowed


def add_removal_provenance(after: dict[str, Any], rule: Rule, cleared: list[dict[str, str]], outcomes: list[dict[str, str]]) -> dict[str, Any]:
    primary = next((item for item in cleared if item["is_primary"] == "true"), None)
    if not primary:
        return {}
    source_field, source_value = matched_raw_field(after, rule.current_value)
    event = {
        "rule_id": rule.rule_id,
        "cleared_field": rule.current_field,
        "previous_value": primary["previous_value"],
        "cleared_companion_fields": [item for item in cleared if item["is_primary"] != "true"],
        "removal_confidence": rule.removal_confidence,
        "reason": rule.rationale,
        "source_raw_field": source_field,
        "source_raw_value": source_value,
        "destination_outcomes": outcomes,
        "destination_status": "removal_only" if not any(outcome.get("status") in {"applied", "already_same"} for outcome in outcomes) else "destination_recorded",
        "method": "semantic_phase2a_dry_run",
        "ruleset_version": RULESET_VERSION,
        "source_audit_commit": rule.source_audit_commit,
    }
    add_provenance(after, REMOVAL_PROVENANCE_KEY, event)
    return event


def apply_rules_to_payload(payload: dict[str, Any], rules_by_lookup: dict[tuple[str, str], list[Rule]]) -> dict[str, Any]:
    before = deepcopy(payload)
    after = deepcopy(payload)
    applied: list[dict[str, str]] = []
    conflicts: list[dict[str, str]] = []
    conditional_skips: list[dict[str, str]] = []
    required_failures: list[dict[str, str]] = []
    unknown_condition_failures: list[dict[str, str]] = []
    already_same: list[dict[str, str]] = []
    existing_different: list[dict[str, str]] = []
    clear_only: list[dict[str, str]] = []
    cleared_fields: list[dict[str, str]] = []
    removal_events: list[dict[str, Any]] = []
    matched_rules: list[Rule] = []

    for key, rules in rules_by_lookup.items():
        current_field, current_value = key
        if norm(after.get(current_field)) != current_value:
            continue
        matched_rules.extend(rules)
        key_cleared: list[dict[str, str]] = []
        clear_rule = next((rule for rule in rules if rule.clear_current_field), None)
        if clear_rule is not None:
            key_cleared = clear_field(after, current_field)
            cleared_fields.extend(key_cleared)
        key_outcomes: list[dict[str, str]] = []
        for rule in rules:
            ok, evidence, failure_kind = condition_met(rule, before)
            if not ok:
                event = {
                    "rule_id": rule.rule_id,
                    "field": rule.destination_field,
                    "value": rule.destination_value,
                    "condition": rule.destination_condition,
                    "evidence": evidence,
                    "detail": failure_kind,
                    "status": failure_kind,
                }
                key_outcomes.append(event)
                if failure_kind == "unknown_condition_failure":
                    unknown_condition_failures.append(event)
                elif failure_kind == "required_evidence_failure":
                    required_failures.append(event)
                else:
                    conditional_skips.append(event)
                continue
            status, detail = set_destination(after, rule, evidence)
            event = {
                "rule_id": rule.rule_id,
                "field": rule.destination_field,
                "value": rule.destination_value,
                "condition": rule.destination_condition,
                "evidence": evidence,
                "detail": detail,
                "status": status,
            }
            key_outcomes.append(event)
            if status == "applied":
                applied.append(event)
            elif status == "already_same":
                already_same.append(event)
            elif status == "existing_different":
                existing_different.append(event)
            elif status == "clear_only":
                clear_only.append(event)
            else:
                conflicts.append(event)
        if clear_rule is not None and key_cleared:
            removal_events.append(add_removal_provenance(after, clear_rule, key_cleared, key_outcomes))

    changed_fields = [field for field in sorted(set(before) | set(after)) if before.get(field, "") != after.get(field, "")]
    legacy_changed = [field for field in LEGACY_COMPATIBILITY_FIELDS if before.get(field, "") != after.get(field, "")]
    protected_changed = [field for field in PROTECTED_FIELDS if before.get(field, "") != after.get(field, "")]
    raw_changed = [field for field in RAW_EVIDENCE_FIELDS if before.get(field, "") != after.get(field, "")]
    unexpected_changed = sorted(set(changed_fields) - allowed_changed_fields(matched_rules)) if matched_rules else []
    return {
        "before": before,
        "after": after,
        "matched_rules": matched_rules,
        "applied": applied,
        "conflicts": conflicts,
        "conditional_skips": conditional_skips,
        "required_failures": required_failures,
        "unknown_condition_failures": unknown_condition_failures,
        "already_same": already_same,
        "existing_different": existing_different,
        "clear_only": clear_only,
        "removal_events": removal_events,
        "cleared_fields": cleared_fields,
        "changed_fields": changed_fields,
        "legacy_changed": legacy_changed,
        "protected_changed": protected_changed,
        "raw_changed": raw_changed,
        "unexpected_changed": unexpected_changed,
    }


def event_rule_lookup(rules: list[Rule]) -> dict[str, Rule]:
    return {rule.rule_id: rule for rule in rules}


def run_dry_run(snapshot_id: str, rules_path: Path, output_dir: Path, validation_examples_per_rule: int) -> dict[str, Any]:
    rules = load_rules(rules_path)
    grouped = rules_by_current(rules)
    rules_lookup = event_rule_lookup(rules)
    records = load_records(snapshot_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    rules_counter: dict[str, Counter[str]] = {rule.rule_id: Counter() for rule in rules}
    value_counter: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    row_changes: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    conditional_skips: list[dict[str, Any]] = []
    required_failures: list[dict[str, Any]] = []
    unknown_condition_failures: list[dict[str, Any]] = []
    already_same: list[dict[str, Any]] = []
    existing_different: list[dict[str, Any]] = []
    removal_rows: list[dict[str, Any]] = []
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
    unexpected_changed_counter: Counter[str] = Counter()
    affected_accessions: set[str] = set()
    patient_environment_context_additions = 0
    plant_context_without_evidence = 0
    catheter_production_changes = 0
    removal_without_provenance = 0
    primary_clears = 0
    companion_clears = 0

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

        patient_row_needed = norm(payload.get("Host_Health_State_SD")) == "patient"
        plant_row_needed = norm(payload.get("Isolation_Site_SD")) == "plant-associated material"
        catheter_row_needed = norm(payload.get("Isolation_Site_SD")) == "catheter"

        if patient_row_needed:
            patient_audit.append(patient_context_row(record, payload, result))
        if plant_row_needed:
            plant_audit.append(context_summary_row(record, payload, result, "plant-associated material"))
        if catheter_row_needed:
            catheter_audit.append(context_summary_row(record, payload, result, "catheter"))

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
            rule = rules_lookup[event["rule_id"]]
            value_counter[(rule.current_field, rule.current_value)]["applied_assignments"] += 1
        for event in result["conflicts"]:
            rules_counter[event["rule_id"]]["destination_conflicts"] += 1
            conflicts.append(context_event(record, payload, event))
        for event in result["conditional_skips"]:
            rules_counter[event["rule_id"]]["conditional_assignment_skips"] += 1
            conditional_skips.append(context_event(record, payload, event))
        for event in result["required_failures"]:
            rules_counter[event["rule_id"]]["required_evidence_failures"] += 1
            required_failures.append(context_event(record, payload, event))
        for event in result["unknown_condition_failures"]:
            rules_counter[event["rule_id"]]["unknown_condition_failures"] += 1
            unknown_condition_failures.append(context_event(record, payload, event))
        for event in result["already_same"]:
            rules_counter[event["rule_id"]]["destination_already_same"] += 1
            already_same.append(context_event(record, payload, event))
        for event in result["existing_different"]:
            rules_counter[event["rule_id"]]["destination_existing_different"] += 1
            existing_different.append(context_event(record, payload, event))
        for removal_event in result["removal_events"]:
            if removal_event:
                removal_rows.append(removal_context_row(record, payload, removal_event))
        for field_event in result["cleared_fields"]:
            if field_event.get("is_primary") == "true":
                primary_clears += 1
            else:
                companion_clears += 1
        for field in result["legacy_changed"]:
            legacy_changed_counter[field] += 1
        for field in result["protected_changed"]:
            protected_changed_counter[field] += 1
        for field in result["raw_changed"]:
            raw_changed_counter[field] += 1
        for field in result["unexpected_changed"]:
            unexpected_changed_counter[field] += 1
        if any(event["rule_id"] == "PH2A-HHS-PATIENT-SAMPLING" for event in result["applied"]) and environment_only_context(payload):
            patient_environment_context_additions += 1
        if any(event["rule_id"] in {"PH2A-SITE-PLANT-MATERIAL-CONTEXT", "PH2A-SITE-PLANT-SAMPLING"} for event in result["applied"]) and not has_plant_context_evidence(payload):
            plant_context_without_evidence += 1
        if catheter_row_needed and result["changed_fields"]:
            catheter_production_changes += 1
        if result["cleared_fields"] and not result["removal_events"]:
            removal_without_provenance += 1
        if changed or result["conflicts"] or result["conditional_skips"] or result["required_failures"] or result["unknown_condition_failures"] or result["already_same"] or result["existing_different"]:
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
            "conditional_assignment_skips": sum(rules_counter[rule.rule_id].get("conditional_assignment_skips", 0) for rule in rules if rule.current_field == field and rule.current_value == value),
            "required_evidence_failures": sum(rules_counter[rule.rule_id].get("required_evidence_failures", 0) for rule in rules if rule.current_field == field and rule.current_value == value),
            "unknown_condition_failures": sum(rules_counter[rule.rule_id].get("unknown_condition_failures", 0) for rule in rules if rule.current_field == field and rule.current_value == value),
            "destination_already_same": sum(rules_counter[rule.rule_id].get("destination_already_same", 0) for rule in rules if rule.current_field == field and rule.current_value == value),
            "destination_existing_different": sum(rules_counter[rule.rule_id].get("destination_existing_different", 0) for rule in rules if rule.current_field == field and rule.current_value == value),
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
        output_dir=output_dir,
        snapshot_id=snapshot_id,
        rules=rules,
        rules_rows=rules_rows,
        value_rows=value_rows,
        row_changes=row_changes,
        conflicts=conflicts,
        conditional_skips=conditional_skips,
        required_failures=required_failures,
        unknown_condition_failures=unknown_condition_failures,
        already_same=already_same,
        existing_different=existing_different,
        removal_rows=removal_rows,
        patient_audit=patient_audit,
        catheter_audit=catheter_audit,
        plant_audit=plant_audit,
        legacy_rows=legacy_rows,
        coverage_rows=coverage_rows,
        validation_examples=validation_examples,
    )
    summary = {
        "generated_at": utc_now(),
        "phase": "phase2a_hardened_dry_run_only",
        "canonical_snapshot_id": snapshot_id,
        "canonical_rows_scanned": len(records),
        "rules_path": str(rules_path),
        "rules_count": len(rules),
        "unique_assemblies_projected_to_change": len(affected_accessions),
        "primary_strict_field_assignments_cleared": primary_clears,
        "companion_fields_cleared": companion_clears,
        "assignments_projected_to_clear": primary_clears + companion_clears,
        "destination_assignments_applied": sum(counts.get("applied_assignments", 0) for counts in rules_counter.values()),
        "new_axis_assignments_projected": sum(row["delta"] for row in coverage_rows if row["field"] in ADDITIVE_FIELDS),
        "destination_conflicts": len(conflicts),
        "conditional_assignment_skips": len(conditional_skips),
        "required_evidence_failures": len(required_failures),
        "unknown_condition_failures": len(unknown_condition_failures),
        "destinations_already_same": len(already_same),
        "destination_existing_different_preserved": len(existing_different),
        "removal_only_corrections": sum(1 for row in removal_rows if row.get("destination_status") == "removal_only"),
        "legacy_field_changes": sum(legacy_changed_counter.values()),
        "protected_field_changes": sum(protected_changed_counter.values()),
        "raw_field_changes": sum(raw_changed_counter.values()),
        "rows_outside_reviewed_allowlist_affected": sum(unexpected_changed_counter.values()),
        "patient_environment_only_context_additions": patient_environment_context_additions,
        "plant_context_without_evidence": plant_context_without_evidence,
        "catheter_production_changes": catheter_production_changes,
        "removal_without_provenance": removal_without_provenance,
        "production_rules_changed": False,
        "canonical_write_run": False,
        "global_insights_regenerated": False,
        "deployment_run": False,
        "hard_failures": hard_failures(
            legacy_changed_counter,
            protected_changed_counter,
            raw_changed_counter,
            conflicts,
            required_failures,
            unknown_condition_failures,
            unexpected_changed_counter,
            patient_environment_context_additions,
            plant_context_without_evidence,
            catheter_production_changes,
            removal_without_provenance,
        ),
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
        "status": event.get("status", ""),
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


def removal_context_row(record: dict[str, Any], payload: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    return {
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "rule_id": event.get("rule_id", ""),
        "cleared_field": event.get("cleared_field", ""),
        "previous_value": event.get("previous_value", ""),
        "cleared_companion_fields": compact_json(event.get("cleared_companion_fields", [])),
        "removal_confidence": event.get("removal_confidence", ""),
        "destination_status": event.get("destination_status", ""),
        "destination_outcomes": compact_json(event.get("destination_outcomes", [])),
        "source_raw_field": event.get("source_raw_field", ""),
        "source_raw_value": event.get("source_raw_value", ""),
        "reason": event.get("reason", ""),
        "Host_SD": payload.get("Host_SD", ""),
        "Host_TaxID": payload.get("Host_TaxID", ""),
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
        "clinical_subject_evidence": str(has_clinical_subject_evidence(payload)).lower(),
        "environment_only_context": str(environment_only_context(payload)).lower(),
        "before_Host_Health_State_SD": payload.get("Host_Health_State_SD", ""),
        "after_Host_Health_State_SD": after.get("Host_Health_State_SD", ""),
        "after_Host_Context_SD": after.get("Host_Context_SD", ""),
        "after_Sampling_Context_SD": after.get("Sampling_Context_SD", ""),
        "conditional_assignment_skips": compact_json(result["conditional_skips"]),
        "removal_provenance": compact_json(after.get(PROVENANCE_FIELD, {}).get(REMOVAL_PROVENANCE_KEY, [])),
    }


def context_summary_row(record: dict[str, Any], payload: dict[str, Any], result: dict[str, Any], value: str) -> dict[str, Any]:
    after = result["after"]
    return {
        "assembly_accession": record["assembly_accession"],
        "biosample": record["biosample"],
        "organism": record["organism"],
        "review_value": value,
        "Host_SD": payload.get("Host_SD", ""),
        "Host_TaxID": payload.get("Host_TaxID", ""),
        "Host_Context_SD": payload.get("Host_Context_SD", ""),
        "plant_context_evidence": str(has_plant_context_evidence(payload)).lower(),
        "plant_material_evidence": str(has_plant_material_evidence(payload)).lower(),
        "collection_device_evidence": str(has_collection_device_evidence(payload)).lower(),
        "before_Isolation_Site_SD": payload.get("Isolation_Site_SD", ""),
        "after_Isolation_Site_SD": after.get("Isolation_Site_SD", ""),
        "after_Sample_Material_SD": after.get("Sample_Material_SD", ""),
        "after_Sample_Collection_Device_SD": after.get("Sample_Collection_Device_SD", ""),
        "after_Host_Context_SD": after.get("Host_Context_SD", ""),
        "after_Sampling_Context_SD": after.get("Sampling_Context_SD", ""),
        "conditional_assignment_skips": compact_json(result["conditional_skips"]),
        "conflicts": compact_json(result["conflicts"]),
        "removal_provenance": compact_json(after.get(PROVENANCE_FIELD, {}).get(REMOVAL_PROVENANCE_KEY, [])),
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
        "cleared_fields": "|".join(event["field"] for event in result["cleared_fields"]),
        "rules_applied": "|".join(sorted({event["rule_id"] for event in result["applied"]})),
        "conflict_count": len(result["conflicts"]),
        "conditional_skip_count": len(result["conditional_skips"]),
        "required_failure_count": len(result["required_failures"]),
        "unknown_condition_failure_count": len(result["unknown_condition_failures"]),
        "destination_already_same_count": len(result["already_same"]),
        "destination_existing_different_count": len(result["existing_different"]),
        "legacy_changed": "|".join(result["legacy_changed"]),
        "protected_changed": "|".join(result["protected_changed"]),
        "raw_changed": "|".join(result["raw_changed"]),
        "unexpected_changed": "|".join(result["unexpected_changed"]),
        "before_values": compact_json({field: before.get(field, "") for field in changed}),
        "after_values": compact_json({field: after.get(field, "") for field in changed}),
    }


def validation_example(record: dict[str, Any], payload: dict[str, Any], result: dict[str, Any], rule: Rule) -> dict[str, Any]:
    after = result["after"]
    fields = sorted(set([rule.current_field, rule.destination_field, *COMPANION_FIELDS.get(rule.current_field, [])]) - {""})
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
        "destination_provenance": compact_json(after.get(PROVENANCE_FIELD, {}).get(rule.destination_field, [])) if rule.destination_field else "",
        "removal_provenance": compact_json(after.get(PROVENANCE_FIELD, {}).get(REMOVAL_PROVENANCE_KEY, [])),
        "conflicts": compact_json(result["conflicts"]),
        "conditional_assignment_skips": compact_json(result["conditional_skips"]),
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
        "conditional_assignment_skips": counts.get("conditional_assignment_skips", 0),
        "required_evidence_failures": counts.get("required_evidence_failures", 0),
        "unknown_condition_failures": counts.get("unknown_condition_failures", 0),
        "destination_already_same": counts.get("destination_already_same", 0),
        "destination_existing_different": counts.get("destination_existing_different", 0),
        "removal_confidence": rule.removal_confidence,
        "destination_confidence": rule.destination_confidence,
        "rationale": rule.rationale,
    }


def hard_failures(
    legacy: Counter[str],
    protected: Counter[str],
    raw: Counter[str],
    conflicts: list[dict[str, Any]],
    required_failures: list[dict[str, Any]],
    unknown_condition_failures: list[dict[str, Any]],
    unexpected_changed: Counter[str],
    patient_environment_context_additions: int,
    plant_context_without_evidence: int,
    catheter_production_changes: int,
    removal_without_provenance: int,
) -> list[str]:
    failures = []
    if sum(legacy.values()):
        failures.append("legacy compatibility field changed")
    if sum(protected.values()):
        failures.append("protected host/geography/date field changed")
    if sum(raw.values()):
        failures.append("raw evidence field changed")
    if conflicts:
        failures.append("destination overwrite conflict")
    if required_failures:
        failures.append("required destination evidence missing")
    if unknown_condition_failures:
        failures.append("unknown destination condition")
    if sum(unexpected_changed.values()):
        failures.append("unexpected non-allowlisted field changed")
    if patient_environment_context_additions:
        failures.append("patient clinical context assigned to environment-only rows")
    if plant_context_without_evidence:
        failures.append("plant context assigned without plant evidence")
    if catheter_production_changes:
        failures.append("catheter changed in Phase 2A")
    if removal_without_provenance:
        failures.append("removal without provenance")
    return failures


def write_outputs(
    output_dir: Path,
    snapshot_id: str,
    rules: list[Rule],
    rules_rows: list[dict[str, Any]],
    value_rows: list[dict[str, Any]],
    row_changes: list[dict[str, Any]],
    conflicts: list[dict[str, Any]],
    conditional_skips: list[dict[str, Any]],
    required_failures: list[dict[str, Any]],
    unknown_condition_failures: list[dict[str, Any]],
    already_same: list[dict[str, Any]],
    existing_different: list[dict[str, Any]],
    removal_rows: list[dict[str, Any]],
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
    event_header = ["assembly_accession", "biosample", "organism", "rule_id", "field", "value", "condition", "evidence", "status", "detail", "Host_SD", "Host_TaxID", "Host_Context_SD", "Host_Health_State_SD", "Host_Disease_SD", "Isolation_Site_SD", "Isolation_Source_SD", "Sample_Type_SD"]
    write_tsv(output_dir / "phase2a_destination_conflicts.tsv", event_header, conflicts)
    write_tsv(output_dir / "phase2a_conditional_assignment_skips.tsv", event_header, conditional_skips)
    write_tsv(output_dir / "phase2a_required_evidence_failures.tsv", event_header, required_failures)
    write_tsv(output_dir / "phase2a_unknown_condition_failures.tsv", event_header, unknown_condition_failures)
    write_tsv(output_dir / "phase2a_destination_already_same.tsv", event_header, already_same)
    write_tsv(output_dir / "phase2a_destination_existing_different.tsv", event_header, existing_different)
    write_tsv(output_dir / "phase2a_removal_provenance.tsv", list(removal_rows[0].keys()) if removal_rows else ["assembly_accession"], removal_rows)
    write_tsv(output_dir / "phase2a_patient_host_context_audit.tsv", list(patient_audit[0].keys()) if patient_audit else ["assembly_accession"], patient_audit)
    write_tsv(output_dir / "phase2a_catheter_context_audit.tsv", list(catheter_audit[0].keys()) if catheter_audit else ["assembly_accession"], catheter_audit)
    write_tsv(output_dir / "phase2a_plant_material_context_audit.tsv", list(plant_audit[0].keys()) if plant_audit else ["assembly_accession"], plant_audit)
    write_tsv(output_dir / "phase2a_legacy_field_preservation.tsv", ["field", "changed_rows", "status"], legacy_rows)
    write_tsv(output_dir / "phase2a_projected_field_coverage.tsv", ["field", "before_present_rows", "after_present_rows", "delta"], coverage_rows)
    write_tsv(output_dir / "phase2a_validation_examples.tsv", list(validation_examples[0].keys()) if validation_examples else ["rule_id"], validation_examples)
    write_tsv(
        output_dir / "phase2a_existing_different_summary.tsv",
        ["rule_id", "current_field", "current_value", "destination_field", "proposed_value", "existing_value", "affected_rows", "interpretation", "review_priority"],
        existing_different_summary_rows(rules, existing_different),
    )
    write_tsv(
        output_dir / "phase2a_removal_only_summary.tsv",
        ["rule_id", "current_field", "current_value", "destination_status", "reason", "affected_rows"],
        removal_only_summary_rows(rules, removal_rows),
    )
    write_tsv(
        output_dir / "phase2a_provenance_completeness_summary.tsv",
        ["metric", "count"],
        provenance_completeness_rows(removal_rows),
    )
    write_full_artifact_manifest(output_dir, snapshot_id)


def existing_different_summary_rows(rules: list[Rule], existing_different: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_rule = {rule.rule_id: rule for rule in rules}
    counts: Counter[tuple[str, str]] = Counter()
    for row in existing_different:
        counts[(row.get("rule_id", ""), parse_existing_value(row.get("detail", "")))] += 1
    summary_rows = []
    for (rule_id, existing_value), count in sorted(counts.items()):
        rule = by_rule.get(rule_id)
        if not rule:
            continue
        summary_rows.append({
            "rule_id": rule.rule_id,
            "current_field": rule.current_field,
            "current_value": rule.current_value,
            "destination_field": rule.destination_field,
            "proposed_value": rule.destination_value,
            "existing_value": existing_value,
            "affected_rows": count,
            "interpretation": existing_different_interpretation(rule, existing_value),
            "review_priority": "medium",
        })
    return summary_rows


def parse_existing_value(detail: str) -> str:
    match = re.search(r"existing=([^;]+)", detail or "")
    return match.group(1).strip() if match else ""


def existing_different_interpretation(rule: Rule, existing_value: str) -> str:
    existing = norm(existing_value)
    if rule.rule_id == "PH2A-HD-HEALTHY-NO-DISEASE":
        if existing == "healthy/control":
            return "compatible composite; study-group decomposition deferred"
        if existing == "healthy/no disease reported":
            return "semantically equivalent legacy value; normalization deferred"
        return "genuine semantic disagreement"
    if rule.rule_id == "PH2A-HD-DISEASED":
        return "genuine semantic disagreement"
    if rule.rule_id == "PH2A-SITE-MANURE-MEDIUM":
        return "benign more-specific value preserved"
    return "manual review"


def removal_only_summary_rows(rules: list[Rule], removal_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_rule = {rule.rule_id: rule for rule in rules}
    counts: Counter[tuple[str, str]] = Counter()
    for row in removal_rows:
        if row.get("destination_status") == "removal_only":
            counts[(row.get("rule_id", ""), row.get("reason", ""))] += 1
    summary_rows = []
    for (rule_id, reason), count in sorted(counts.items()):
        rule = by_rule.get(rule_id)
        summary_rows.append({
            "rule_id": rule_id,
            "current_field": rule.current_field if rule else "",
            "current_value": rule.current_value if rule else "",
            "destination_status": "removal_only",
            "reason": reason,
            "affected_rows": count,
        })
    return summary_rows


def provenance_completeness_rows(removal_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(removal_rows)
    exact_raw = 0
    standardized = 0
    no_usable = 0
    for row in removal_rows:
        has_raw = present(row.get("source_raw_field")) and present(row.get("source_raw_value"))
        has_standardized = present(row.get("previous_value")) or present(row.get("cleared_companion_fields"))
        if has_raw:
            exact_raw += 1
        elif has_standardized:
            standardized += 1
        else:
            no_usable += 1
    return [
        {"metric": "total_removals", "count": total},
        {"metric": "removals_with_exact_raw_evidence", "count": exact_raw},
        {"metric": "removals_with_no_raw_match_but_standardized_provenance", "count": standardized},
        {"metric": "removals_with_no_usable_provenance", "count": no_usable},
    ]


def write_full_artifact_manifest(output_dir: Path, snapshot_id: str) -> None:
    row_path = output_dir / "phase2a_projected_row_changes.tsv"
    compressed_path = output_dir / "phase2a_projected_row_changes.tsv.gz"
    if row_path.exists():
        with row_path.open("rb") as src, compressed_path.open("wb") as raw_dst:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw_dst, mtime=0) as dst:
                for chunk in iter(lambda: src.read(1024 * 1024), b""):
                    dst.write(chunk)
    manifest = {
        "artifact_filename": row_path.name,
        "canonical_snapshot_id": snapshot_id,
        "compressed_bytes": compressed_path.stat().st_size if compressed_path.exists() else 0,
        "compressed_filename": compressed_path.name,
        "compressed_sha256": sha256_file(compressed_path) if compressed_path.exists() else "",
        "external_archive_uri": "TBD: attach compressed artifact to GitHub release or manuscript data archive",
        "generator_commit": os.environ.get("FETCHM_PHASE2A_GENERATOR_COMMIT", "working_tree"),
        "git_tracking_policy": "Do not commit full row-change TSV or compressed artifact to normal Git history.",
        "local_compressed_artifact_path": str(compressed_path),
        "ruleset_version": RULESET_VERSION,
        "uncompressed_bytes": row_path.stat().st_size if row_path.exists() else 0,
        "uncompressed_row_count_excluding_header": count_data_rows(row_path) if row_path.exists() else 0,
        "uncompressed_sha256": sha256_file(row_path) if row_path.exists() else "",
    }
    (output_dir / "phase2a_full_artifact_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def count_data_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        count = sum(1 for _ in handle)
    return max(0, count - 1)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_summary_md(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Phase 2A Hardened Semantic Axis Dry Run",
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
        f"| Primary strict-field assignments cleared | {summary['primary_strict_field_assignments_cleared']:,} |",
        f"| Companion fields cleared | {summary['companion_fields_cleared']:,} |",
        f"| New-axis assignments projected | {summary['new_axis_assignments_projected']:,} |",
        f"| Destination assignments applied | {summary['destination_assignments_applied']:,} |",
        f"| Destinations already same | {summary['destinations_already_same']:,} |",
        f"| Existing-different destinations preserved | {summary['destination_existing_different_preserved']:,} |",
        f"| Conditional assignment skips | {summary['conditional_assignment_skips']:,} |",
        f"| Required evidence failures | {summary['required_evidence_failures']:,} |",
        f"| Unknown-condition failures | {summary['unknown_condition_failures']:,} |",
        f"| Destination conflicts | {summary['destination_conflicts']:,} |",
        f"| Removal-only corrections | {summary['removal_only_corrections']:,} |",
        f"| Legacy compatibility field changes | {summary['legacy_field_changes']:,} |",
        f"| Protected/raw field changes | {summary['protected_field_changes'] + summary['raw_field_changes']:,} |",
        f"| Rows outside reviewed allowlist affected | {summary['rows_outside_reviewed_allowlist_affected']:,} |",
        "",
        "## Gate",
        "",
        "Dry-run mutation-safety gate: pass" if not summary["hard_failures"] else "Dry-run mutation-safety gate: fail: " + ", ".join(summary["hard_failures"]),
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
