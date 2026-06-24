#!/usr/bin/env python3
"""Apply reviewed source/environment semantic completion rules.

The tool composes the accepted Phase 2A host/clinical/site transformer with a
small deterministic source/environment/material enrichment layer. Dry-run is the
default. The apply mode updates only changed canonical standardized payloads and
is blocked when the mutation-safety gate reports hard failures.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import Jsonb, connect
from tools.semantic_phase2a_dry_run import (
    ADDITIVE_FIELDS as PHASE2A_ADDITIVE_FIELDS,
    COMPANION_FIELDS,
    DEFAULT_RULES as DEFAULT_PHASE2A_RULES,
    DEFAULT_SNAPSHOT_ID,
    LEGACY_COMPATIBILITY_FIELDS,
    PROTECTED_FIELDS,
    PROVENANCE_FIELD,
    RAW_EVIDENCE_FIELDS,
    REMOVAL_PROVENANCE_KEY,
    apply_rules_to_payload,
    environment_only_context,
    compact_json,
    load_rules,
    norm,
    normalize_lookup,
    present,
    read_payload,
    rules_by_current,
    write_tsv,
)

RULESET_VERSION = "source_environment_semantic_completion_v1"
DEFAULT_OUTPUT_ROOT = ROOT / "standardization" / "review" / "source_environment_semantic_completion"

ACTIVE_SEMANTIC_FIELDS = [
    "Sample_Material_SD",
    "Sampling_Context_SD",
    "Host_Anatomical_Material_SD",
    "Host_Hospitalization_Status_SD",
    "Host_Vital_Status_SD",
    "Host_Colonization_Status_SD",
    "Host_Disease_Stage_SD",
    "Host_Exposure_Context_SD",
    PROVENANCE_FIELD,
]

COMPLETION_SOURCE_FIELDS = [
    "Sample_Type_SD",
    "Sample_Type_SD_Broad",
    "Isolation_Source_SD",
    "Isolation_Source_SD_Broad",
    "Environment_Medium_SD",
    "Environment_Local_Scale_SD",
    "Environment_Broad_Scale_SD",
    "Isolation_Site_SD",
    "Isolation Source",
    "Isolation Site",
    "Sample Type",
    "Environment Medium",
    "Environment Local Scale",
    "Environment Broad Scale",
    "BioSample Isolation Source",
    "BioSample Isolation Site",
    "BioSample Sample Type",
    "BioSample Source Name",
    "BioSample Tissue",
    "BioSample Tissue Type",
    "BioSample Host Tissue Sampled",
    "BioSample Environmental Medium",
    "BioSample Environmental Local Scale",
    "BioSample Environmental Broad Scale",
    "BioSample Env Medium",
    "BioSample Env Local Scale",
    "BioSample Env Broad Scale",
]

ALL_RAW_OR_PROTECTED_FIELDS = set(RAW_EVIDENCE_FIELDS) | set(PROTECTED_FIELDS)
LEGACY_FIELDS = set(LEGACY_COMPATIBILITY_FIELDS)

STRICT_SITE_FORBIDDEN = {
    "cerebrospinal fluid",
    "manure",
    "pus",
    "plant-associated material",
}

ENVIRONMENT_ONLY_RE = re.compile(
    r"\b(?:wastewater|waste water|sewage|sewer|effluent|influent|sludge|sink|drain|"
    r"surface|environmental|environment|water|soil|sediment|biofilm|hospital environment|"
    r"healthcare environment|facility surface)\b",
    re.I,
)
HOST_BIOFILM_RE = re.compile(r"\b(?:dental|oral|tooth|teeth|gingival|skin|wound|catheter|device|host|patient)\b", re.I)
PLANT_EVIDENCE_RE = re.compile(
    r"\b(?:plant|plant-associated|plant associated|leaf|root|stem|seed|fruit|flower|rhizosphere|"
    r"phyllosphere|endosphere|plant tissue|arabidopsis|oryza|rice|zea|maize|corn|soybean|"
    r"lettuce|spinach|wheat|triticum|tomato|citrus)\b",
    re.I,
)
PLANT_MATERIAL_RE = re.compile(r"\b(?:leaf|root|stem|seed|fruit|flower|plant tissue|plant material|plant sample)\b", re.I)
ANIMAL_OR_HUMAN_RE = re.compile(r"\b(?:human|homo sapiens|patient|aves|bos|bovine|cow|cattle|chicken|gallus|pig|swine|fish|oyster|mouse|rat|dog|cat)\b", re.I)
CONTRADICTORY_FOOD_ONLY_RE = re.compile(r"\b(?:food|meat|egg|lettuce|oyster|fish|chicken|beef|pork|market)\b", re.I)


@dataclass(frozen=True)
class CompletionRule:
    rule_id: str
    source_values: tuple[str, ...]
    destination_field: str
    destination_value: str
    category: str
    confidence: str
    condition: str = "blank_or_same"
    source_fields: tuple[str, ...] = tuple(COMPLETION_SOURCE_FIELDS)
    rationale: str = "reviewed deterministic source/environment completion"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def git_commit() -> str:
    configured = str(os.environ.get("FETCHM_WEBAPP_GIT_COMMIT") or "").strip()
    if configured:
        return configured
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT.parent, text=True, capture_output=True, check=False)
    return result.stdout.strip() or "unknown"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_text(payload: dict[str, Any], fields: Iterable[str] = COMPLETION_SOURCE_FIELDS) -> str:
    return " | ".join(str(payload.get(field) or "") for field in fields if present(payload.get(field)))


def normalized_values(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(normalize_lookup(value) for value in values)


def exact_source_match(payload: dict[str, Any], rule: CompletionRule) -> tuple[str, str]:
    wanted = set(normalized_values(rule.source_values))
    for field in rule.source_fields:
        value = payload.get(field)
        if not present(value):
            continue
        normalized = normalize_lookup(value)
        if normalized in wanted:
            return field, str(value)
    return "", ""


def phrase_source_match(payload: dict[str, Any], phrases: Iterable[str], fields: Iterable[str] = COMPLETION_SOURCE_FIELDS) -> tuple[str, str]:
    wanted = set(normalized_values(phrases))
    for field in fields:
        value = payload.get(field)
        if not present(value):
            continue
        normalized = normalize_lookup(value)
        if normalized in wanted:
            return field, str(value)
    return "", ""


def has_host_evidence(payload: dict[str, Any]) -> bool:
    host_sd = normalize_lookup(payload.get("Host_SD"))
    host_taxid = normalize_lookup(payload.get("Host_TaxID"))
    host_context = normalize_lookup(payload.get("Host_Context_SD"))
    if host_sd and host_sd not in {"environment", "environmental", "sample", "unknown", "absent"}:
        return True
    if host_taxid:
        return True
    return host_context in {"human associated", "animal associated", "host associated", "clinical host associated material"}


def has_plant_context_evidence(payload: dict[str, Any], matched_field: str = "", matched_value: str = "") -> bool:
    blob = source_text(payload, ["Host_SD", "Host", "Host_Original", "Host_Cleaned", "BioSample Host", *COMPLETION_SOURCE_FIELDS])
    if matched_value:
        blob = f"{blob} | {matched_value}"
    if ANIMAL_OR_HUMAN_RE.search(blob) and not PLANT_EVIDENCE_RE.search(blob):
        return False
    return bool(PLANT_EVIDENCE_RE.search(blob))


def has_plant_material_evidence(payload: dict[str, Any], matched_value: str = "") -> bool:
    blob = source_text(payload, COMPLETION_SOURCE_FIELDS)
    if matched_value:
        blob = f"{blob} | {matched_value}"
    return bool(PLANT_MATERIAL_RE.search(blob))


def condition_met(rule: CompletionRule, payload: dict[str, Any], source_field: str, source_value: str) -> tuple[bool, str, str]:
    destination = rule.destination_field
    current = str(payload.get(destination) or "").strip()
    condition = norm(rule.condition)
    if condition == "blank_or_same":
        if not current:
            return True, "destination_blank=true", ""
        if norm(current) == norm(rule.destination_value):
            return True, "destination_same=true", ""
        return False, f"existing_different={current}", "preserved_conflict"
    if condition == "destination_blank":
        if not current:
            return True, "destination_blank=true", ""
        return False, f"destination_blank=false; existing={current}", "preserved_conflict"
    if condition == "environmental_biofilm":
        blob = source_text(payload)
        if HOST_BIOFILM_RE.search(blob):
            return False, "host_or_device_biofilm_context=true", "ambiguous"
        return condition_met(CompletionRule(**{**rule.__dict__, "condition": "blank_or_same"}), payload, source_field, source_value)
    if condition == "plant_context":
        if not has_plant_context_evidence(payload, source_field, source_value):
            return False, "plant_context_evidence=false", "ambiguous"
        return condition_met(CompletionRule(**{**rule.__dict__, "condition": "blank_or_same"}), payload, source_field, source_value)
    if condition == "plant_material":
        if not has_plant_material_evidence(payload, source_value):
            return False, "plant_material_evidence=false", "ambiguous"
        return condition_met(CompletionRule(**{**rule.__dict__, "condition": "blank_or_same"}), payload, source_field, source_value)
    if condition == "host_material":
        if not has_host_evidence(payload):
            return False, "host_evidence=false", "conditional_assignment_skip"
        return condition_met(CompletionRule(**{**rule.__dict__, "condition": "blank_or_same"}), payload, source_field, source_value)
    return False, f"unknown_condition={rule.condition}", "unknown_condition_failure"


def completion_provenance(rule: CompletionRule, source_field: str, source_value: str, evidence: str) -> dict[str, str]:
    return {
        "rule_id": rule.rule_id,
        "source_current_field": source_field,
        "source_current_value": source_value,
        "source_raw_field": source_field if source_field not in LEGACY_FIELDS else "",
        "source_raw_value": source_value if source_field not in LEGACY_FIELDS else "",
        "method": "source_environment_semantic_completion",
        "confidence": rule.confidence,
        "evidence": evidence,
        "ruleset_version": RULESET_VERSION,
    }


def add_provenance(payload: dict[str, Any], field: str, entry: dict[str, Any]) -> None:
    provenance = payload.get(PROVENANCE_FIELD)
    if not isinstance(provenance, dict):
        provenance = {}
    entries = provenance.setdefault(field, [])
    if entry not in entries:
        entries.append(entry)
    payload[PROVENANCE_FIELD] = provenance


def has_rule_provenance(payload: dict[str, Any], field: str, rule_id: str) -> bool:
    provenance = payload.get(PROVENANCE_FIELD)
    if not isinstance(provenance, dict):
        return False
    entries = provenance.get(field)
    if not isinstance(entries, list):
        return False
    return any(isinstance(entry, dict) and entry.get("rule_id") == rule_id for entry in entries)


def set_destination(payload: dict[str, Any], rule: CompletionRule, source_field: str, source_value: str, evidence: str) -> tuple[str, str]:
    current = str(payload.get(rule.destination_field) or "").strip()
    if not current:
        payload[rule.destination_field] = rule.destination_value
        add_provenance(payload, rule.destination_field, completion_provenance(rule, source_field, source_value, evidence))
        return "applied", ""
    if norm(current) == norm(rule.destination_value):
        if not has_rule_provenance(payload, rule.destination_field, rule.rule_id):
            add_provenance(payload, rule.destination_field, completion_provenance(rule, source_field, source_value, evidence))
        return "already_present", "same destination value already present"
    return "preserved_conflict", f"existing={current}; proposed={rule.destination_value}"


def rules() -> list[CompletionRule]:
    source_standardized = ("Sample_Type_SD", "Sample_Type_SD_Broad", "Isolation_Source_SD", "Isolation_Source_SD_Broad")
    source_all = tuple(COMPLETION_SOURCE_FIELDS)
    return [
        CompletionRule("SEC-MEDIUM-WATER", ("water",), "Environment_Medium_SD", "water", "environment_medium", "high", source_fields=source_all),
        CompletionRule("SEC-MEDIUM-SALINE-WATER", ("saline water",), "Environment_Medium_SD", "saline water", "environment_medium", "high", source_fields=source_all),
        CompletionRule("SEC-MEDIUM-WASTEWATER", ("wastewater", "waste water", "hospital wastewater"), "Environment_Medium_SD", "wastewater", "environment_medium", "high", source_fields=source_all),
        CompletionRule("SEC-MEDIUM-SEWAGE", ("sewage", "hospital sewage"), "Environment_Medium_SD", "sewage", "environment_medium", "high", source_fields=source_all),
        CompletionRule("SEC-MEDIUM-BIOFILM", ("biofilm",), "Environment_Medium_SD", "biofilm", "environment_medium", "high", condition="environmental_biofilm", source_fields=source_all),
        CompletionRule("SEC-MEDIUM-MANURE", ("manure", "manure/fecal material"), "Environment_Medium_SD", "agricultural organic material", "environment_medium", "medium", condition="destination_blank", source_fields=source_all),
        CompletionRule("SEC-LOCAL-STREAM", ("stream",), "Environment_Local_Scale_SD", "stream", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-COLD-SEEP", ("cold seep",), "Environment_Local_Scale_SD", "cold seep", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-DRAIN", ("drain",), "Environment_Local_Scale_SD", "drain", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-SINK", ("sink",), "Environment_Local_Scale_SD", "sink", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-ANAEROBIC-DIGESTER", ("anaerobic digester",), "Environment_Local_Scale_SD", "anaerobic digester", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-GLACIER", ("glacier",), "Environment_Local_Scale_SD", "glacier", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-ESTUARY", ("estuary",), "Environment_Local_Scale_SD", "estuary", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-CLEANROOM-FLOOR", ("cleanroom floor",), "Environment_Local_Scale_SD", "cleanroom floor", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-CAVE-BIOFILM", ("cave biofilm",), "Environment_Local_Scale_SD", "cave", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-MEDIUM-CAVE-BIOFILM", ("cave biofilm",), "Environment_Medium_SD", "biofilm", "environment_medium", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-FARM", ("farm", "dairy farm"), "Environment_Local_Scale_SD", "farm", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-BROAD-FARM", ("farm", "dairy farm"), "Environment_Broad_Scale_SD", "agricultural environment", "environment_broad_scale", "high", source_fields=source_all),
        CompletionRule("SEC-LOCAL-RHIZOSPHERE", ("rhizosphere",), "Environment_Local_Scale_SD", "rhizosphere", "environment_local_scale", "high", source_fields=source_all),
        CompletionRule("SEC-CONTEXT-RHIZOSPHERE", ("rhizosphere",), "Host_Context_SD", "plant-associated", "sampling_context", "medium", condition="plant_context", source_fields=source_all),
        CompletionRule("SEC-BROAD-TERRESTRIAL", ("terrestrial environment",), "Environment_Broad_Scale_SD", "terrestrial environment", "environment_broad_scale", "high", source_fields=source_all),
        CompletionRule("SEC-BROAD-BUILT", ("built environment",), "Environment_Broad_Scale_SD", "built environment", "environment_broad_scale", "high", source_fields=source_all),
        CompletionRule("SEC-BROAD-AGRICULTURAL", ("agricultural environment",), "Environment_Broad_Scale_SD", "agricultural environment", "environment_broad_scale", "high", source_fields=source_all),
        CompletionRule("SEC-BROAD-HEALTHCARE", ("healthcare-associated environment", "healthcare associated environment", "hospital environment"), "Environment_Broad_Scale_SD", "healthcare-associated environment", "environment_broad_scale", "high", source_fields=source_all),
        CompletionRule("SEC-BROAD-LABORATORY", ("laboratory environment",), "Environment_Broad_Scale_SD", "laboratory environment", "environment_broad_scale", "high", source_fields=source_all),
        CompletionRule("SEC-CONTEXT-LABORATORY", ("laboratory",), "Sampling_Context_SD", "laboratory", "sampling_context", "medium", source_fields=source_standardized),
        CompletionRule("SEC-MATERIAL-BLOOD", ("blood",), "Sample_Material_SD", "blood", "sample_material", "high", source_fields=source_standardized),
        CompletionRule("SEC-ANAT-MATERIAL-BLOOD", ("blood",), "Host_Anatomical_Material_SD", "blood", "anatomical_material", "medium", condition="host_material", source_fields=source_standardized),
        CompletionRule("SEC-MATERIAL-URINE", ("urine",), "Sample_Material_SD", "urine", "sample_material", "high", source_fields=source_standardized),
        CompletionRule("SEC-MATERIAL-FECES", ("feces/stool", "feces", "faeces", "stool"), "Sample_Material_SD", "feces/stool", "sample_material", "high", source_fields=source_standardized),
        CompletionRule("SEC-ANAT-MATERIAL-FECES", ("feces/stool", "feces", "faeces", "stool"), "Host_Anatomical_Material_SD", "feces/stool", "anatomical_material", "medium", condition="host_material", source_fields=source_standardized),
        CompletionRule("SEC-MATERIAL-MILK", ("milk",), "Sample_Material_SD", "milk", "sample_material", "high", source_fields=source_standardized),
        CompletionRule("SEC-MATERIAL-TISSUE", ("tissue",), "Sample_Material_SD", "tissue", "sample_material", "medium", source_fields=source_standardized),
        CompletionRule("SEC-MATERIAL-SPUTUM", ("sputum",), "Sample_Material_SD", "sputum", "sample_material", "high", source_fields=source_standardized),
        CompletionRule("SEC-MATERIAL-BODY-FLUID", ("body fluid", "fluid"), "Sample_Material_SD", "body fluid", "sample_material", "medium", source_fields=source_standardized),
        CompletionRule("SEC-MATERIAL-BAL", ("bronchoalveolar lavage", "bronchoalveolar lavage fluid"), "Sample_Material_SD", "bronchoalveolar lavage fluid", "sample_material", "high", source_fields=source_standardized),
    ]


def validate_completion_rules(completion_rules: list[CompletionRule]) -> None:
    ids = [rule.rule_id for rule in completion_rules]
    duplicates = [rule_id for rule_id, count in Counter(ids).items() if count > 1]
    if duplicates:
        raise ValueError(f"Duplicate source/environment completion rule IDs: {duplicates}")
    blocked = ALL_RAW_OR_PROTECTED_FIELDS | LEGACY_FIELDS
    for rule in completion_rules:
        if rule.destination_field in blocked:
            raise ValueError(f"Rule {rule.rule_id} writes blocked field {rule.destination_field}")
    conflicts: dict[tuple[tuple[str, ...], str], set[str]] = defaultdict(set)
    for rule in completion_rules:
        conflicts[(normalized_values(rule.source_values), rule.destination_field)].add(norm(rule.destination_value))
    bad = {key: values for key, values in conflicts.items() if len(values) > 1}
    if bad:
        raise ValueError(f"Conflicting source/environment completion rules: {bad}")


def phase2a_values_by_field(phase2a_lookup: dict[tuple[str, str], list[Any]]) -> dict[str, set[str]]:
    values: dict[str, set[str]] = defaultdict(set)
    for field, value in phase2a_lookup:
        values[field].add(value)
    return values


def phase2a_has_match(payload: dict[str, Any], values_by_field: dict[str, set[str]]) -> bool:
    for field, values in values_by_field.items():
        if norm(payload.get(field)) in values:
            return True
    return False


def empty_phase2a_result(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "before": payload,
        "after": payload,
        "matched_rules": [],
        "applied": [],
        "conflicts": [],
        "conditional_skips": [],
        "required_failures": [],
        "unknown_condition_failures": [],
        "already_same": [],
        "existing_different": [],
        "clear_only": [],
        "removal_events": [],
        "cleared_fields": [],
        "changed_fields": [],
        "legacy_changed": [],
        "protected_changed": [],
        "raw_changed": [],
        "unexpected_changed": [],
    }


def completion_rule_index(completion_rules: list[CompletionRule]) -> dict[str, dict[str, list[CompletionRule]]]:
    index: dict[str, dict[str, list[CompletionRule]]] = defaultdict(lambda: defaultdict(list))
    for rule in completion_rules:
        for field in rule.source_fields:
            for value in normalized_values(rule.source_values):
                index[field][value].append(rule)
    return index


def allowed_changed_fields(phase2a_result: dict[str, Any], completion_events: list[dict[str, Any]]) -> set[str]:
    allowed = {PROVENANCE_FIELD}
    allowed.update(phase2a_result.get("changed_fields", []))
    for event in completion_events:
        allowed.add(str(event.get("destination_field") or ""))
    return {field for field in allowed if field}


def apply_completion_rules(payload: dict[str, Any], completion_rules: list[CompletionRule] | dict[str, dict[str, list[CompletionRule]]]) -> dict[str, Any]:
    after = deepcopy(payload)
    events: list[dict[str, Any]] = []
    index = completion_rule_index(completion_rules) if isinstance(completion_rules, list) else completion_rules
    seen_rules: set[str] = set()
    for source_field in COMPLETION_SOURCE_FIELDS:
        value = after.get(source_field)
        if not present(value):
            continue
        matched_rules = index.get(source_field, {}).get(normalize_lookup(value), [])
        for rule in matched_rules:
            if rule.rule_id in seen_rules:
                continue
            seen_rules.add(rule.rule_id)
            source_value = str(value)
            ok, evidence, skip_kind = condition_met(rule, after, source_field, source_value)
            event = {
                "rule_id": rule.rule_id,
                "source_field": source_field,
                "source_value": source_value,
                "destination_field": rule.destination_field,
                "destination_value": rule.destination_value,
                "category": rule.category,
                "confidence": rule.confidence,
                "evidence": evidence,
                "status": "",
                "detail": "",
            }
            if not ok:
                event["status"] = skip_kind or "skipped"
                event["detail"] = evidence
                events.append(event)
                continue
            status, detail = set_destination(after, rule, source_field, source_value, evidence)
            event["status"] = status
            event["detail"] = detail
            events.append(event)
    changed_fields = [field for field in sorted(set(payload) | set(after)) if payload.get(field, "") != after.get(field, "")]
    return {"after": after, "events": events, "changed_fields": changed_fields}


def apply_semantic_completion(
    payload: dict[str, Any],
    phase2a_lookup: dict[tuple[str, str], list[Any]],
    completion_rules: list[CompletionRule] | dict[str, dict[str, list[CompletionRule]]],
    phase2a_trigger_values: dict[str, set[str]] | None = None,
) -> dict[str, Any]:
    before = deepcopy(payload)
    trigger_values = phase2a_trigger_values or phase2a_values_by_field(phase2a_lookup)
    if phase2a_has_match(before, trigger_values):
        phase2a = apply_rules_to_payload(before, phase2a_lookup)
    else:
        phase2a = empty_phase2a_result(before)
    completion = apply_completion_rules(phase2a["after"], completion_rules)
    after = completion["after"]
    changed_fields = [field for field in sorted(set(before) | set(after)) if before.get(field, "") != after.get(field, "")]
    legacy_changed = [field for field in LEGACY_COMPATIBILITY_FIELDS if before.get(field, "") != after.get(field, "")]
    protected_changed = [field for field in PROTECTED_FIELDS if before.get(field, "") != after.get(field, "")]
    raw_changed = [field for field in RAW_EVIDENCE_FIELDS if before.get(field, "") != after.get(field, "")]
    allowed = allowed_changed_fields(phase2a, completion["events"])
    unexpected_changed = sorted(set(changed_fields) - allowed)
    return {
        "before": before,
        "after": after,
        "changed_fields": changed_fields,
        "phase2a": phase2a,
        "completion_events": completion["events"],
        "completion_changed_fields": completion["changed_fields"],
        "legacy_changed": legacy_changed,
        "protected_changed": protected_changed,
        "raw_changed": raw_changed,
        "unexpected_changed": unexpected_changed,
    }


def event_context(record: dict[str, Any], payload: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    return {
        "assembly_accession": record.get("assembly_accession", ""),
        "biosample": record.get("biosample", ""),
        "organism": record.get("organism", ""),
        "rule_id": event.get("rule_id", ""),
        "source_field": event.get("source_field", ""),
        "source_value": event.get("source_value", ""),
        "destination_field": event.get("destination_field", ""),
        "destination_value": event.get("destination_value", ""),
        "category": event.get("category", ""),
        "status": event.get("status", ""),
        "detail": event.get("detail", ""),
        "Host_SD": payload.get("Host_SD", ""),
        "Host_Context_SD": payload.get("Host_Context_SD", ""),
        "Isolation_Source_SD": payload.get("Isolation_Source_SD", ""),
        "Sample_Type_SD": payload.get("Sample_Type_SD", ""),
        "Environment_Medium_SD": payload.get("Environment_Medium_SD", ""),
        "Environment_Local_Scale_SD": payload.get("Environment_Local_Scale_SD", ""),
        "Environment_Broad_Scale_SD": payload.get("Environment_Broad_Scale_SD", ""),
        "Isolation_Site_SD": payload.get("Isolation_Site_SD", ""),
    }


def rule_summary_rows(completion_rules: list[CompletionRule], counter: dict[str, Counter[str]]) -> list[dict[str, Any]]:
    rows = []
    for rule in completion_rules:
        counts = counter[rule.rule_id]
        rows.append({
            "rule_id": rule.rule_id,
            "source_values": "|".join(rule.source_values),
            "destination_field": rule.destination_field,
            "destination_value": rule.destination_value,
            "category": rule.category,
            "condition": rule.condition,
            "matched_rows": counts.get("matched", 0),
            "applied_rows": counts.get("applied", 0),
            "already_present_rows": counts.get("already_present", 0),
            "preserved_conflict_rows": counts.get("preserved_conflict", 0),
            "ambiguous_rows": counts.get("ambiguous", 0),
            "conditional_skip_rows": counts.get("conditional_assignment_skip", 0),
            "unknown_condition_failure_rows": counts.get("unknown_condition_failure", 0),
        })
    return rows


def phase2a_counts(result: dict[str, Any]) -> Counter[str]:
    phase2a = result["phase2a"]
    counts = Counter()
    if phase2a["changed_fields"]:
        counts["changed_rows"] += 1
    counts["primary_strict_clears"] += sum(1 for item in phase2a["cleared_fields"] if item.get("is_primary") == "true")
    counts["companion_clears"] += sum(1 for item in phase2a["cleared_fields"] if item.get("is_primary") != "true")
    counts["destination_conflicts"] += len(phase2a["conflicts"])
    counts["required_evidence_failures"] += len(phase2a["required_failures"])
    counts["unknown_condition_failures"] += len(phase2a["unknown_condition_failures"])
    counts["removal_without_provenance"] += int(bool(phase2a["cleared_fields"] and not phase2a["removal_events"]))
    return counts


def hard_failures(summary: dict[str, Any]) -> list[str]:
    failures = []
    checks = {
        "legacy_field_changes": "legacy compatibility field changed",
        "raw_field_changes": "raw evidence field changed",
        "protected_field_changes": "protected host/geography/date field changed",
        "outside_allowlist_field_changes": "outside-allowlist field changed",
        "destination_conflicts": "destination overwrite conflict",
        "required_evidence_failures": "required evidence failure",
        "unknown_condition_failures": "unknown condition failure",
        "missing_removal_provenance": "removal without provenance",
        "catheter_changes": "catheter changed",
        "plant_context_without_evidence": "plant context without evidence",
        "patient_environment_only_additions": "patient context added to environment-only row",
    }
    for key, message in checks.items():
        if int(summary.get(key) or 0):
            failures.append(message)
    if int(summary.get("strict_site_forbidden_remaining", 0)):
        failures.append("forbidden non-site value remains in Isolation_Site_SD")
    return failures


def write_summary_md(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Source/Environment Semantic Completion",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Snapshot: `{summary['canonical_snapshot_id']}`",
        f"- Rows scanned: {summary['canonical_rows_scanned']:,}",
        f"- Assemblies changed/projected: {summary['unique_assemblies_changed']:,}",
        f"- Phase 2A primary strict-field clears: {summary['phase2a_primary_strict_field_clears']:,}",
        f"- Environment/material assignments applied/projected: {summary['completion_assignments_applied']:,}",
        f"- Legacy field changes: {summary['legacy_field_changes']:,}",
        f"- Raw/protected changes: {summary['raw_field_changes']:,} / {summary['protected_field_changes']:,}",
        f"- Destination conflicts: {summary['destination_conflicts']:,}",
        f"- Strict site forbidden values remaining: {summary['strict_site_forbidden_remaining']:,}",
        f"- Canonical write run: `{str(summary['canonical_write_run']).lower()}`",
        f"- Global Insights regenerated: `{str(summary['global_insights_regenerated']).lower()}`",
        f"- Deployment run: `{str(summary['deployment_run']).lower()}`",
        "",
        "## Hard Failures",
        "",
    ]
    failures = summary.get("hard_failures") or []
    if failures:
        lines.extend(f"- {failure}" for failure in failures)
    else:
        lines.append("- none")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def scan_forbidden_sites(records: list[dict[str, Any]], transformed: dict[str, dict[str, Any]] | None = None) -> int:
    count = 0
    for record in records:
        payload = transformed.get(record["assembly_accession"], record["payload"]) if transformed is not None else record["payload"]
        if norm(payload.get("Isolation_Site_SD")) in STRICT_SITE_FORBIDDEN:
            count += 1
    return count


def output_dir_for(base: Path, label: str | None) -> Path:
    stamp = label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return base / stamp


def iter_records(snapshot_id: str, batch_size: int = 5000) -> Iterable[dict[str, Any]]:
    with connect() as connection:
        with connection.cursor(name="source_environment_semantic_completion_stream") as cursor:
            cursor.execute(
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
            )
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for accession, organism, biosample, payload in rows:
                    yield {
                        "assembly_accession": str(accession),
                        "organism": str(organism or ""),
                        "biosample": str(biosample or ""),
                        "payload": read_payload(payload),
                    }


def append_limited(rows: list[dict[str, Any]], row: dict[str, Any], limit: int = 10000) -> None:
    if len(rows) < limit:
        rows.append(row)


def backup_table_name() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"assembly_standardization_backup_source_env_{timestamp}"


def initialize_backup_table(connection: Any, snapshot_id: str, backup_table: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {backup_table} AS
        SELECT s.*
        FROM assembly_standardization AS s
        JOIN bacterial_inventory_membership AS i USING (assembly_accession)
        WHERE i.snapshot_id = %s AND false
        """,
        (snapshot_id,),
    )


def write_change_batch(connection: Any, backup_table: str, snapshot_id: str, rows: list[tuple[str, dict[str, Any]]]) -> None:
    if not rows:
        return
    now = datetime.now(timezone.utc)
    with connection.cursor() as cursor:
        for accession, payload in rows:
            cursor.execute(f"INSERT INTO {backup_table} SELECT s.* FROM assembly_standardization AS s WHERE s.assembly_accession = %s", (accession,))
            cursor.execute(
                """
                UPDATE assembly_standardization
                SET standardized_payload = %s,
                    status = 'source_environment_semantic_completion',
                    updated_at = %s
                WHERE assembly_accession = %s
                  AND EXISTS (
                    SELECT 1 FROM bacterial_inventory_membership
                    WHERE snapshot_id = %s AND assembly_accession = %s
                  )
                """,
                (Jsonb(payload), now, accession, snapshot_id, accession),
            )


def write_apply_manifest(output_dir: Path, snapshot_id: str, backup_table: str, changed_rows: int) -> None:
    manifest = {
        "backup_table": backup_table,
        "changed_rows_backed_up": changed_rows,
        "snapshot_id": snapshot_id,
        "applied_at": utc_now(),
        "ruleset_version": RULESET_VERSION,
    }
    (output_dir / "source_environment_completion_apply_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(snapshot_id: str, output_dir: Path, *, apply: bool = False, phase2a_rules_path: Path = DEFAULT_PHASE2A_RULES) -> dict[str, Any]:
    phase2a_rules = load_rules(phase2a_rules_path)
    phase2a_lookup = rules_by_current(phase2a_rules)
    completion_rules = rules()
    validate_completion_rules(completion_rules)
    phase2a_trigger_values = phase2a_values_by_field(phase2a_lookup)
    completion_index = completion_rule_index(completion_rules)
    output_dir.mkdir(parents=True, exist_ok=True)

    affected_count = 0
    write_batch: list[tuple[str, dict[str, Any]]] = []
    backup_table = backup_table_name() if apply else ""
    write_context = None
    write_connection = None
    write_transaction = None
    rule_counter: dict[str, Counter[str]] = defaultdict(Counter)
    category_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    conflicts: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    strict_rows: list[dict[str, Any]] = []
    legacy_changed = Counter()
    protected_changed = Counter()
    raw_changed = Counter()
    unexpected_changed = Counter()
    phase2a_total = Counter()
    provenance_total = Counter()
    catheter_changes = 0
    plant_context_without_evidence = 0
    patient_environment_only_additions = 0
    strict_site_remaining = 0
    records_scanned = 0

    before_coverage = Counter()
    after_coverage = Counter()
    coverage_fields = [
        *ACTIVE_SEMANTIC_FIELDS[:-1],
        "Environment_Medium_SD",
        "Environment_Local_Scale_SD",
        "Environment_Broad_Scale_SD",
        "Host_Production_Context_SD",
    ]

    try:
        if apply:
            write_context = connect()
            write_connection = write_context.__enter__()
            write_transaction = write_connection.transaction()
            write_transaction.__enter__()
            initialize_backup_table(write_connection, snapshot_id, backup_table)

        for record in iter_records(snapshot_id):
            records_scanned += 1
            payload = record["payload"]
            for field in coverage_fields:
                if present(payload.get(field)):
                    before_coverage[field] += 1
            result = apply_semantic_completion(payload, phase2a_lookup, completion_index, phase2a_trigger_values)
            after = result["after"]
            for field in coverage_fields:
                if present(after.get(field)):
                    after_coverage[field] += 1

            phase2a_total.update(phase2a_counts(result))
            if norm(payload.get("Isolation_Site_SD")) in STRICT_SITE_FORBIDDEN:
                append_limited(strict_rows, {
                    "assembly_accession": record["assembly_accession"],
                    "biosample": record["biosample"],
                    "organism": record["organism"],
                    "before_Isolation_Site_SD": payload.get("Isolation_Site_SD", ""),
                    "after_Isolation_Site_SD": after.get("Isolation_Site_SD", ""),
                    "after_Sample_Material_SD": after.get("Sample_Material_SD", ""),
                    "after_Environment_Medium_SD": after.get("Environment_Medium_SD", ""),
                    "after_Host_Context_SD": after.get("Host_Context_SD", ""),
                    "after_Sampling_Context_SD": after.get("Sampling_Context_SD", ""),
                })
            if norm(after.get("Isolation_Site_SD")) in STRICT_SITE_FORBIDDEN:
                strict_site_remaining += 1
            if result["changed_fields"]:
                affected_count += 1
                if apply:
                    write_batch.append((record["assembly_accession"], after))
                    if len(write_batch) >= 1000:
                        write_change_batch(write_connection, backup_table, snapshot_id, write_batch)
                        write_batch.clear()
            for field in result["legacy_changed"]:
                legacy_changed[field] += 1
            for field in result["protected_changed"]:
                protected_changed[field] += 1
            for field in result["raw_changed"]:
                raw_changed[field] += 1
            for field in result["unexpected_changed"]:
                unexpected_changed[field] += 1
            for event in result["completion_events"]:
                status = str(event.get("status") or "")
                rule_counter[event["rule_id"]]["matched"] += 1
                rule_counter[event["rule_id"]][status] += 1
                row = event_context(record, payload, event)
                if status == "preserved_conflict":
                    append_limited(conflicts, row)
                elif status in {"ambiguous", "conditional_assignment_skip"}:
                    append_limited(ambiguous, row)
                elif status in {"applied", "already_present"}:
                    append_limited(category_rows[event["category"]], row)
            if norm(payload.get("Isolation_Site_SD")) == "catheter" and result["changed_fields"]:
                catheter_changes += 1
            if after.get("Host_Context_SD") == "plant-associated" and not has_plant_context_evidence(payload):
                plant_context_without_evidence += 1
            if (
                any(event.get("rule_id") == "PH2A-HHS-PATIENT-SAMPLING" for event in result["phase2a"].get("applied", []))
                and environment_only_context(payload)
            ):
                patient_environment_only_additions += 1
            provenance = after.get(PROVENANCE_FIELD)
            if isinstance(provenance, dict):
                for field, entries in provenance.items():
                    if isinstance(entries, list):
                        provenance_total["provenance_entries"] += len(entries)
                        if field == REMOVAL_PROVENANCE_KEY:
                            provenance_total["removal_provenance_entries"] += len(entries)
                        else:
                            provenance_total["destination_provenance_entries"] += len(entries)

        rules_rows = rule_summary_rows(completion_rules, rule_counter)
        coverage_rows = [
            {
                "field": field,
                "before_present_rows": before_coverage.get(field, 0),
                "after_present_rows": after_coverage.get(field, 0),
                "delta": after_coverage.get(field, 0) - before_coverage.get(field, 0),
            }
            for field in coverage_fields
        ]
        legacy_rows = [
            {"field": field, "changed_rows": legacy_changed.get(field, 0), "status": "pass" if not legacy_changed.get(field, 0) else "fail"}
            for field in LEGACY_COMPATIBILITY_FIELDS
        ]
        provenance_rows = [
            {"metric": "destination_provenance_entries", "count": provenance_total.get("destination_provenance_entries", 0)},
            {"metric": "removal_provenance_entries", "count": provenance_total.get("removal_provenance_entries", 0)},
            {"metric": "missing_removal_provenance", "count": phase2a_total.get("removal_without_provenance", 0)},
        ]

        summary = {
            "generated_at": utc_now(),
            "git_commit": git_commit(),
            "canonical_snapshot_id": snapshot_id,
            "canonical_rows_scanned": records_scanned,
            "phase2a_rules_path": str(phase2a_rules_path),
            "phase2a_rules_count": len(phase2a_rules),
            "completion_rules_count": len(completion_rules),
            "unique_assemblies_changed": affected_count,
            "phase2a_primary_strict_field_clears": phase2a_total.get("primary_strict_clears", 0),
            "phase2a_companion_field_clears": phase2a_total.get("companion_clears", 0),
            "completion_assignments_applied": sum(count.get("applied", 0) for count in rule_counter.values()),
            "completion_destinations_already_present": sum(count.get("already_present", 0) for count in rule_counter.values()),
            "completion_preserved_destination_conflicts": sum(count.get("preserved_conflict", 0) for count in rule_counter.values()),
            "ambiguous_values_retained": sum(count.get("ambiguous", 0) + count.get("conditional_assignment_skip", 0) for count in rule_counter.values()),
            "legacy_field_changes": sum(legacy_changed.values()),
            "raw_field_changes": sum(raw_changed.values()),
            "protected_field_changes": sum(protected_changed.values()),
            "outside_allowlist_field_changes": sum(unexpected_changed.values()),
            "destination_conflicts": phase2a_total.get("destination_conflicts", 0),
            "required_evidence_failures": phase2a_total.get("required_evidence_failures", 0),
            "unknown_condition_failures": phase2a_total.get("unknown_condition_failures", 0),
            "missing_removal_provenance": phase2a_total.get("removal_without_provenance", 0),
            "strict_site_forbidden_remaining": strict_site_remaining,
            "catheter_changes": catheter_changes,
            "plant_context_without_evidence": plant_context_without_evidence,
            "patient_environment_only_additions": patient_environment_only_additions,
            "canonical_write_run": bool(apply),
            "global_insights_regenerated": False,
            "deployment_run": False,
        }
        summary["hard_failures"] = hard_failures(summary)

        write_tsv(output_dir / "rule_level_before_after.tsv", list(rules_rows[0].keys()) if rules_rows else ["rule_id"], rules_rows)
        write_tsv(output_dir / "environment_medium_additions.tsv", list(category_rows["environment_medium"][0].keys()) if category_rows["environment_medium"] else ["assembly_accession"], category_rows["environment_medium"])
        write_tsv(output_dir / "environment_local_scale_additions.tsv", list(category_rows["environment_local_scale"][0].keys()) if category_rows["environment_local_scale"] else ["assembly_accession"], category_rows["environment_local_scale"])
        write_tsv(output_dir / "environment_broad_scale_additions.tsv", list(category_rows["environment_broad_scale"][0].keys()) if category_rows["environment_broad_scale"] else ["assembly_accession"], category_rows["environment_broad_scale"])
        write_tsv(output_dir / "sample_material_additions.tsv", list(category_rows["sample_material"][0].keys()) if category_rows["sample_material"] else ["assembly_accession"], category_rows["sample_material"])
        write_tsv(output_dir / "preserved_destination_conflicts.tsv", list(conflicts[0].keys()) if conflicts else ["assembly_accession"], conflicts)
        write_tsv(output_dir / "ambiguous_values_retained.tsv", list(ambiguous[0].keys()) if ambiguous else ["assembly_accession"], ambiguous)
        write_tsv(output_dir / "strict_site_corrections.tsv", list(strict_rows[0].keys()) if strict_rows else ["assembly_accession"], strict_rows)
        write_tsv(output_dir / "legacy_field_preservation.tsv", ["field", "changed_rows", "status"], legacy_rows)
        write_tsv(output_dir / "provenance_completeness.tsv", ["metric", "count"], provenance_rows)
        write_tsv(output_dir / "field_coverage_before_after.tsv", ["field", "before_present_rows", "after_present_rows", "delta"], coverage_rows)
        (output_dir / "source_environment_completion_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        write_summary_md(output_dir / "source_environment_completion_summary.md", summary)

        if summary["hard_failures"]:
            if apply:
                raise RuntimeError("Source/environment completion gate failed; rolling back canonical write: " + json.dumps(summary["hard_failures"], sort_keys=True))
            return summary
        if apply:
            write_change_batch(write_connection, backup_table, snapshot_id, write_batch)
            write_batch.clear()
            write_transaction.__exit__(None, None, None)
            write_context.__exit__(None, None, None)
            write_transaction = None
            write_context = None
            write_apply_manifest(output_dir, snapshot_id, backup_table, affected_count)
        return summary
    except Exception as exc:
        if write_transaction is not None:
            write_transaction.__exit__(type(exc), exc, exc.__traceback__)
        if write_context is not None:
            write_context.__exit__(type(exc), exc, exc.__traceback__)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--phase2a-rules", type=Path, default=DEFAULT_PHASE2A_RULES)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--output-label", default="")
    parser.add_argument("--apply", action="store_true", help="Apply changes transactionally after a clean dry-run gate.")
    args = parser.parse_args()
    output_dir = args.output_dir or output_dir_for(DEFAULT_OUTPUT_ROOT, args.output_label or None)
    summary = run(args.snapshot_id, output_dir, apply=args.apply, phase2a_rules_path=args.phase2a_rules)
    print(json.dumps(summary, sort_keys=True))
    if summary.get("hard_failures"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
