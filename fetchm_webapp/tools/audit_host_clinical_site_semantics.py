#!/usr/bin/env python3
"""Audit host, clinical, sample, site, and environment semantic field purity.

Phase 1 audit only: this script reads canonical bacterial standardized metadata,
classifies distinct standardized values, exports incompatibility queues, and does
not modify rules or canonical metadata.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import connect

DEFAULT_SNAPSHOT_ID = "20260602T140414Z_genbank_bacteria_root"
DEFAULT_OUTPUT_ROOT = ROOT / "standardization" / "review" / "host_clinical_site_semantics_audit"

AUDIT_FIELDS = [
    "Host_SD",
    "Host_Context_SD",
    "Host_Disease_SD",
    "Host_Health_State_SD",
    "Host_Production_Context_SD",
    "Host_Anatomical_Site_SD",
    "Sample_Type_SD",
    "Sample_Type_SD_Broad",
    "Isolation_Source_SD",
    "Isolation_Source_SD_Broad",
    "Isolation_Site_SD",
    "Environment_Medium_SD",
    "Environment_Medium_SD_Broad",
    "Environment_Broad_Scale_SD",
    "Environment_Local_Scale_SD",
]

CONTEXT_FIELDS = [
    "Host",
    "Host_SD",
    "Host_Context_SD",
    "Host_Disease_SD",
    "Host_Health_State_SD",
    "Host_Production_Context_SD",
    "Host_Anatomical_Site_SD",
    "Isolation Source",
    "Isolation_Source_SD",
    "Isolation_Source_SD_Broad",
    "Sample Type",
    "Sample_Type_SD",
    "Sample_Type_SD_Broad",
    "Isolation_Site_SD",
    "Environment_Medium_SD",
    "Environment_Medium_SD_Broad",
    "Environment_Broad_Scale_SD",
    "Environment_Local_Scale_SD",
    "Collection Date",
    "Country",
]

RAW_CONTEXT_FIELDS = [
    "Host",
    "Isolation Source",
    "Sample Type",
    "Host Disease",
    "Host Health State",
    "BioSample Host Disease",
    "BioSample Isolation Source",
    "BioSample ENV Package",
    "BioSample ENV Broad Scale",
    "BioSample ENV Local Scale",
    "BioSample ENV Medium",
    "BioSample ENV Material",
]

SEMANTIC_CLASSES = [
    "host_taxon",
    "host_context",
    "disease",
    "health_state",
    "study_group",
    "care_setting",
    "vital_status",
    "colonization_status",
    "disease_stage",
    "sample_material",
    "collection_device",
    "sample_processing",
    "anatomical_site",
    "environment_medium",
    "environment_local_scale",
    "environment_broad_scale",
    "source_context",
    "food_commodity",
    "metadata_descriptor",
    "unresolved",
]

FIELD_CONTRACT = {
    "Host_SD": {"host_taxon"},
    "Host_Context_SD": {"host_context"},
    "Host_Disease_SD": {"disease"},
    "Host_Health_State_SD": {"health_state"},
    "Host_Production_Context_SD": {"host_context"},
    "Host_Anatomical_Site_SD": {"anatomical_site"},
    "Sample_Type_SD": {"sample_material", "food_commodity"},
    "Sample_Type_SD_Broad": {"sample_material", "food_commodity"},
    "Isolation_Source_SD": {"source_context", "food_commodity"},
    "Isolation_Source_SD_Broad": {"source_context", "food_commodity", "environment_broad_scale"},
    "Isolation_Site_SD": {"anatomical_site"},
    "Environment_Medium_SD": {"environment_medium"},
    "Environment_Medium_SD_Broad": {"environment_medium"},
    "Environment_Broad_Scale_SD": {"environment_broad_scale"},
    "Environment_Local_Scale_SD": {"environment_local_scale"},
}

INCOMPATIBILITY_FILES = {
    "non_health_values_in_host_health_state": "non_health_values_in_host_health_state.tsv",
    "non_disease_values_in_host_disease": "non_disease_values_in_host_disease.tsv",
    "non_material_values_in_sample_type": "non_material_values_in_sample_type.tsv",
    "non_site_values_in_isolation_site": "non_site_values_in_isolation_site.tsv",
    "material_values_in_isolation_source": "material_values_in_isolation_source.tsv",
    "environment_values_in_sample_type": "environment_values_in_sample_type.tsv",
    "processing_values_in_sample_type": "processing_values_in_sample_type.tsv",
    "care_setting_values_in_health_state": "care_setting_values_in_health_state.tsv",
    "study_group_values_in_health_state": "study_group_values_in_health_state.tsv",
    "vital_status_values_in_health_state": "vital_status_values_in_health_state.tsv",
    "colonization_values_in_health_state": "colonization_values_in_health_state.tsv",
    "disease_stage_values_in_health_state": "disease_stage_values_in_health_state.tsv",
    "conflicting_anatomical_site_fields": "conflicting_anatomical_site_fields.tsv",
    "ambiguous_broad_source_categories": "ambiguous_broad_source_categories.tsv",
}

TARGET_VALUES = {
    "patient",
    "clinical patient",
    "patient sample",
    "diseased",
    "hospitalized",
    "non-hospitalized",
    "non hospitalized",
    "control",
    "healthy control",
    "healthy/no disease reported",
    "carrier",
    "colonized",
    "alive",
    "dead",
    "convalescent",
    "exacerbation",
    "specific pathogen free",
    "pus",
    "manure",
    "cerebrospinal fluid",
    "bronchoalveolar lavage fluid",
    "gut content",
    "gastric biopsy",
    "tracheal aspirate",
    "abscess",
    "catheter",
    "sink",
    "drain",
    "clinical sample",
    "clinical material",
    "clinical/host-associated material",
    "clinical fluid/material",
    "metagenomic assembly",
    "culture",
    "enrichment culture",
    "dna extract",
    "swab",
}

EXACT_CLASS_OVERRIDES = {
    "patient": ("host_context", "Patient is a clinical human subject/context, not a health state."),
    "patient/clinical": ("source_context", "Clinical patient aggregate; should become host/sample context, not a material."),
    "clinical patient": ("host_context", "Clinical human subject/context; not health state."),
    "patient sample": ("source_context", "Clinical sample context without physical material."),
    "clinical sample": ("source_context", "Clinical sampling context, not a physical material."),
    "clinical material": ("source_context", "Clinical material context is broad and should be split when possible."),
    "clinical/host-associated material": ("source_context", "Broad clinical or host-associated source context."),
    "host-associated context": ("source_context", "Generic host-associated source context."),
    "clinical fluid/material": ("sample_material", "Broad clinical fluid or purulent material class."),
    "clinical fluid material": ("sample_material", "Broad clinical fluid or purulent material class."),
    "food/meat": ("food_commodity", "Food commodity material/context."),
    "food/dairy": ("food_commodity", "Food commodity material/context."),
    "food/produce": ("food_commodity", "Food commodity material/context."),
    "food/plant product": ("food_commodity", "Food commodity material/context."),
    "aquatic food product": ("food_commodity", "Food commodity material/context."),
    "sterile body site": ("anatomical_site", "Anatomical site class, not a specimen material."),
    "organ/tissue site": ("anatomical_site", "Anatomical site class, not a specimen material."),
    "tonsil/oropharyngeal site": ("anatomical_site", "Anatomical site class."),
    "cubital fossa": ("anatomical_site", "Anatomical site class."),
    "phyllosphere": ("anatomical_site", "Plant surface/anatomical context."),
    "endosphere": ("anatomical_site", "Plant internal/anatomical context."),
    "leaf tissue": ("anatomical_site", "Plant part/tissue context."),
    "healthy": ("health_state", "Health state."),
    "diseased": ("health_state", "Generic health state; not a named disease."),
    "healthy/no disease reported": ("health_state", "Health state/no disease report, not a disease."),
    "healthy control": ("study_group", "Compound healthy plus control; needs split health-state + study-group."),
    "healthy/control": ("study_group", "Study group/control signal; needs split if health is explicit."),
    "control": ("study_group", "Control group, not necessarily healthy."),
    "hospitalized": ("care_setting", "Hospitalization/care setting."),
    "non-hospitalized": ("care_setting", "Hospitalization/care setting."),
    "non hospitalized": ("care_setting", "Hospitalization/care setting."),
    "inpatient": ("care_setting", "Care setting."),
    "outpatient": ("care_setting", "Care setting."),
    "carrier": ("colonization_status", "Carrier/colonization status."),
    "colonized": ("colonization_status", "Colonization status."),
    "colonization": ("colonization_status", "Colonization status."),
    "alive": ("vital_status", "Vital status."),
    "dead": ("vital_status", "Vital status."),
    "deceased": ("vital_status", "Vital status."),
    "convalescent": ("disease_stage", "Disease stage."),
    "exacerbation": ("disease_stage", "Disease stage."),
    "specific pathogen free": ("host_context", "Husbandry/production context, not health state."),
    "pus": ("sample_material", "Purulent specimen material, not anatomical site."),
    "purulent material": ("sample_material", "Purulent specimen material."),
    "abscess": ("sample_material", "Abscess material unless explicit site wording exists."),
    "cerebrospinal fluid": ("sample_material", "Clinical fluid specimen."),
    "bronchoalveolar lavage fluid": ("sample_material", "Lavage fluid specimen."),
    "tracheal aspirate": ("sample_material", "Aspirate specimen."),
    "tracheal aspirate/secretion": ("sample_material", "Respiratory aspirate/secretion material."),
    "gastric biopsy": ("sample_material", "Biopsy material with gastrointestinal site context."),
    "biopsy": ("sample_material", "Biopsy material."),
    "gut content": ("sample_material", "Gut/intestinal contents are specimen material."),
    "intestinal content": ("sample_material", "Intestinal contents are specimen material."),
    "manure": ("sample_material", "Agricultural fecal material; not anatomical site."),
    "catheter": ("collection_device", "Medical device/collection device, not anatomical site."),
    "swab": ("collection_device", "Collection device; may remain compatibility sample label."),
    "sponge": ("collection_device", "Collection device."),
    "filter": ("collection_device", "Collection device."),
    "sink": ("environment_local_scale", "Built-environment local feature."),
    "drain": ("environment_local_scale", "Built-environment local feature."),
    "farm": ("environment_local_scale", "Agricultural local feature."),
    "river": ("environment_local_scale", "Freshwater local feature."),
    "pond": ("environment_local_scale", "Freshwater local feature."),
    "groundwater": ("environment_medium", "Environmental medium."),
    "culture": ("sample_processing", "Culture/process context unless explicitly submitted material."),
    "enrichment culture": ("sample_processing", "Enrichment process context."),
    "metagenomic assembly": ("sample_processing", "Assembly/processing state, not physical specimen."),
    "metagenome assembly": ("sample_processing", "Assembly/processing state, not physical specimen."),
    "dna extract": ("sample_processing", "Molecular extract/processing state."),
    "molecular extract": ("sample_processing", "Molecular extract/processing state."),
    "single cell": ("sample_processing", "Single-cell preparation."),
}

PATTERNS = [
    ("metadata_descriptor", re.compile(r"\b(?:metadata descriptor|non-source|unknown|missing|not applicable|uncategorized|sample|specimen)\b", re.I), "Generic descriptor or missing value."),
    ("sample_processing", re.compile(r"\b(?:culture|enrichment|metagenom|assembly|dna extract|rna extract|extract|single[- ]?cell|wgs|sequencing)\b", re.I), "Processing or molecular preparation term."),
    ("collection_device", re.compile(r"\b(?:swab|sponge|catheter|filter|trap)\b", re.I), "Collection device term."),
    ("care_setting", re.compile(r"\b(?:hospitali[sz]ed|non[- ]?hospitali[sz]ed|inpatient|outpatient|icu|intensive care|nursing home|long[- ]term care)\b", re.I), "Care setting term."),
    ("study_group", re.compile(r"\b(?:control|case|contact|exposed)\b", re.I), "Study group term."),
    ("colonization_status", re.compile(r"\b(?:carrier|carriage|colonized|colonised|colonization|colonisation)\b", re.I), "Colonization or carriage term."),
    ("vital_status", re.compile(r"\b(?:alive|dead|deceased)\b", re.I), "Vital status term."),
    ("disease_stage", re.compile(r"\b(?:convalescent|exacerbation|acute|chronic)\b", re.I), "Disease stage term."),
    ("health_state", re.compile(r"\b(?:healthy|diseased|asymptomatic|symptomatic|sick)\b", re.I), "Host health state."),
    ("disease", re.compile(r"\b(?:pneumonia|sepsis|bacteremia|bacteraemia|diarrh|infection|mastitis|meningitis|tuberculosis|cystic fibrosis|gastroenteritis|colitis|osteomyelitis|leukemia|listeriosis|salmonellosis|campylobacteriosis|pertussis|gastritis|neoplasm|cancer|otitis|obesity|legionellosis|buruli ulcer|tularemia|yersiniosis|syphilis|brucellosis|melioidosis|diphtheria|leptospirosis|diabetes|scleroderma|endocarditis|glaucoma|bronchiectasis|abortion|adenocarcinoma|lymphoma|injury|dental caries|disorder|disease)\b", re.I), "Disease or disease class."),
    ("sample_material", re.compile(r"\b(?:blood|urine|feces|faeces|stool|sputum|saliva|tissue|fluid|pus|aspirate|biopsy|lavage|csf|cerebrospinal|bile|abscess|content|contents|manure|milk)\b", re.I), "Physical specimen/material."),
    ("anatomical_site", re.compile(r"\b(?:skin|wound|rectum|rectal|perianal|nasal|nasopharynx|oropharynx|throat|oral|mouth|lung|bronch|trachea|respiratory tract|bladder|urogenital|vagina|cervix|uterus|gastrointestinal tract|colon|stomach|liver|kidney|brain|spleen|bone|eye|ear|placenta|lymph node|heart|breast|cloaca|root|leaf|stem)\b", re.I), "Anatomical site or plant part."),
    ("environment_medium", re.compile(r"\b(?:soil|water|wastewater|sewage|sediment|sludge|biofilm|air|dust|compost|groundwater|seawater|freshwater|manure)\b", re.I), "Environmental medium."),
    ("environment_local_scale", re.compile(r"\b(?:sink|drain|farm|river|lake|pond|canal|stream|facility|hospital|laboratory|market|abattoir|slaughterhouse|reservoir|hot spring|cave|cleanroom)\b", re.I), "Local environmental/facility feature."),
    ("environment_broad_scale", re.compile(r"\b(?:built environment|healthcare-associated environment|agricultural environment|freshwater environment|marine environment|terrestrial environment|host-associated environment|plant-associated environment)\b", re.I), "Broad environmental setting."),
    ("food_commodity", re.compile(r"\b(?:food|meat|beef|pork|poultry|chicken|turkey|seafood|oyster|shrimp|shellfish|dairy|cheese|yogurt|produce|vegetable|fruit|egg)\b", re.I), "Food or animal commodity."),
    ("host_context", re.compile(r"\b(?:human|patient|animal|poultry|cattle|cow|swine|pig|fish|plant|host-associated|plant-associated|animal-associated)\b", re.I), "Host-associated context."),
    ("source_context", re.compile(r"\b(?:clinical|host-associated|environmental material|source context|surveillance|outbreak)\b", re.I), "Broad source/sample context."),
]

BANANA_BLOOD_DISEASE_RE = re.compile(r"\bbanana blood disease\b", re.I)


@dataclass(frozen=True)
class ClassifiedValue:
    semantic_class: str
    reason: str
    normalized_value: str


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", normalize(value)).strip("_") or "value"


def is_present(value: Any) -> bool:
    text = normalize(value)
    return bool(text) and text not in {"-", "--", "na", "n/a", "none", "not applicable", "unknown", "missing", "null"}


def classify_value(value: Any) -> ClassifiedValue:
    text = normalize(value)
    if not text:
        return ClassifiedValue("unresolved", "Blank or missing value.", text)
    if text in EXACT_CLASS_OVERRIDES:
        klass, reason = EXACT_CLASS_OVERRIDES[text]
        return ClassifiedValue(klass, reason, text)
    if BANANA_BLOOD_DISEASE_RE.search(text):
        return ClassifiedValue("disease", "Compound organism/disease name; must not be classified from the token 'blood'.", text)
    for semantic_class, pattern, reason in PATTERNS:
        if pattern.search(text):
            return ClassifiedValue(semantic_class, reason, text)
    if re.search(r"^[A-Z][a-z]+(?: [a-z]+)?$", str(value or "").strip()):
        return ClassifiedValue("host_taxon", "Looks like a taxonomic label; verify against Host_SD context.", text)
    return ClassifiedValue("unresolved", "No deterministic semantic class assigned by audit rules.", text)


def field_expected_classes(field: str) -> set[str]:
    return FIELD_CONTRACT.get(field, set())


def git_commit() -> str:
    import os

    configured = str(os.environ.get("FETCHM_WEBAPP_GIT_COMMIT") or "").strip()
    if configured:
        return configured
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT.parent, capture_output=True, text=True, check=False)
    return result.stdout.strip() or "unknown"


def read_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value)
    return value or {}


def write_tsv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


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
            ORDER BY i.assembly_accession
            """,
            (snapshot_id,),
        ).fetchall()
    records = []
    for accession, organism, biosample, payload in rows:
        records.append({
            "assembly_accession": str(accession),
            "organism": str(organism or ""),
            "biosample": str(biosample or ""),
            "payload": read_payload(payload),
        })
    return records


def distinct_values(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counters: dict[str, Counter[str]] = {field: Counter() for field in AUDIT_FIELDS}
    for record in records:
        payload = record["payload"]
        for field in AUDIT_FIELDS:
            value = payload.get(field)
            if is_present(value):
                counters[field][str(value).strip()] += 1
    rows = []
    for field, counter in counters.items():
        for value, count in counter.most_common():
            classified = classify_value(value)
            expected = field_expected_classes(field)
            compatible = classified.semantic_class in expected if expected else True
            rows.append({
                "field": field,
                "standardized_value": value,
                "semantic_class": classified.semantic_class,
                "compatible_with_field_contract": str(compatible).lower(),
                "row_count": count,
                "reason": classified.reason,
                "expected_semantic_classes": "|".join(sorted(expected)),
            })
    return rows


def field_count_rows(value_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(value_rows, key=lambda row: (row["field"], -int(row["row_count"]), row["standardized_value"]))


def add_flag(flags: dict[str, list[dict[str, Any]]], bucket: str, row: dict[str, Any], reason: str, proposed_destination: str) -> None:
    flags[bucket].append({
        "field": row["field"],
        "standardized_value": row["standardized_value"],
        "semantic_class": row["semantic_class"],
        "row_count": row["row_count"],
        "reason": reason,
        "proposed_destination": proposed_destination,
    })


def incompatibility_rows(value_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    flags: dict[str, list[dict[str, Any]]] = {key: [] for key in INCOMPATIBILITY_FILES}
    for row in value_rows:
        field = row["field"]
        klass = row["semantic_class"]
        value = normalize(row["standardized_value"])
        if field == "Host_Health_State_SD" and klass != "health_state":
            add_flag(flags, "non_health_values_in_host_health_state", row, "Host_Health_State_SD should contain strict health states only.", recommended_destination(klass))
        if field == "Host_Disease_SD" and klass != "disease":
            add_flag(flags, "non_disease_values_in_host_disease", row, "Host_Disease_SD should contain named diseases or defensible disease classes only.", recommended_destination(klass))
        if field in {"Sample_Type_SD", "Sample_Type_SD_Broad"} and klass not in {"sample_material", "food_commodity"}:
            add_flag(flags, "non_material_values_in_sample_type", row, "Sample_Type fields should contain physical specimen/material values only.", recommended_destination(klass))
        if field == "Sample_Type_SD" and klass in {"environment_medium", "environment_local_scale", "environment_broad_scale"}:
            add_flag(flags, "environment_values_in_sample_type", row, "Environmental terms should not be physical sample type values.", recommended_destination(klass))
        if field == "Sample_Type_SD" and klass == "sample_processing":
            add_flag(flags, "processing_values_in_sample_type", row, "Processing/preparation terms should move to Sample_Processing_SD.", "Sample_Processing_SD")
        if field == "Isolation_Site_SD" and klass != "anatomical_site":
            add_flag(flags, "non_site_values_in_isolation_site", row, "Isolation_Site_SD should contain anatomical sites or plant parts only.", recommended_destination(klass))
        if field in {"Isolation_Source_SD", "Isolation_Source_SD_Broad"} and klass == "sample_material":
            add_flag(flags, "material_values_in_isolation_source", row, "Physical specimen/material values should not be represented as strict source context.", "Sample_Type_SD")
        if field == "Host_Health_State_SD" and klass == "care_setting":
            add_flag(flags, "care_setting_values_in_health_state", row, "Care setting should move to Host_Care_Setting_SD.", "Host_Care_Setting_SD")
        if field == "Host_Health_State_SD" and klass == "study_group":
            add_flag(flags, "study_group_values_in_health_state", row, "Study group should move to Host_Study_Group_SD.", "Host_Study_Group_SD")
        if field == "Host_Health_State_SD" and klass == "vital_status":
            add_flag(flags, "vital_status_values_in_health_state", row, "Vital status should move to Host_Vital_Status_SD.", "Host_Vital_Status_SD")
        if field == "Host_Health_State_SD" and klass == "colonization_status":
            add_flag(flags, "colonization_values_in_health_state", row, "Colonization/carrier status should move to Host_Colonization_Status_SD.", "Host_Colonization_Status_SD")
        if field == "Host_Health_State_SD" and klass == "disease_stage":
            add_flag(flags, "disease_stage_values_in_health_state", row, "Disease stage should move to Host_Disease_Stage_SD.", "Host_Disease_Stage_SD")
        if field in {"Isolation_Site_SD", "Host_Anatomical_Site_SD"} and klass in {"sample_material", "environment_medium", "environment_local_scale", "source_context"}:
            add_flag(flags, "conflicting_anatomical_site_fields", row, "Site field contains non-anatomical context/material.", recommended_destination(klass))
        if field == "Isolation_Source_SD_Broad" and klass not in {"source_context", "food_commodity", "environment_broad_scale"}:
            add_flag(flags, "ambiguous_broad_source_categories", row, "Broad source category mixes non-source semantic classes; document or migrate.", recommended_destination(klass))
        if value in {"banana blood disease"} and field == "Sample_Type_SD":
            add_flag(flags, "non_material_values_in_sample_type", row, "Compound disease name false positive; token blood should not create blood sample type.", "Host_Disease_SD")
    return {key: sorted(rows, key=lambda r: (-int(r["row_count"]), r["field"], r["standardized_value"])) for key, rows in flags.items()}


def recommended_destination(semantic_class: str) -> str:
    return {
        "host_taxon": "Host_SD",
        "host_context": "Host_Context_SD",
        "disease": "Host_Disease_SD",
        "health_state": "Host_Health_State_SD",
        "study_group": "Host_Study_Group_SD",
        "care_setting": "Host_Care_Setting_SD",
        "vital_status": "Host_Vital_Status_SD",
        "colonization_status": "Host_Colonization_Status_SD",
        "disease_stage": "Host_Disease_Stage_SD",
        "sample_material": "Sample_Type_SD",
        "collection_device": "Sample_Collection_Device_SD",
        "sample_processing": "Sample_Processing_SD",
        "anatomical_site": "Host_Anatomical_Site_SD",
        "environment_medium": "Environment_Medium_SD",
        "environment_local_scale": "Environment_Local_Scale_SD",
        "environment_broad_scale": "Environment_Broad_Scale_SD",
        "source_context": "Isolation_Source_SD",
        "food_commodity": "Isolation_Source_SD",
        "metadata_descriptor": "suppress/provenance only",
        "unresolved": "admin_review",
    }.get(semantic_class, "admin_review")


def raw_match_context(payload: dict[str, Any], value: str) -> tuple[str, str]:
    normalized_value = normalize(value)
    for field in RAW_CONTEXT_FIELDS:
        raw_value = payload.get(field)
        if not is_present(raw_value):
            continue
        text = normalize(raw_value)
        if normalized_value and (normalized_value == text or normalized_value in text or text in normalized_value):
            return field, str(raw_value)
    for field in RAW_CONTEXT_FIELDS:
        raw_value = payload.get(field)
        if is_present(raw_value):
            return field, str(raw_value)
    return "", ""


def example_rows(records: list[dict[str, Any]], flagged_rows: list[dict[str, Any]], per_value: int) -> list[dict[str, Any]]:
    wanted = {(row["field"], row["standardized_value"]): row for row in flagged_rows}
    remaining = {key: per_value for key in wanted}
    examples: list[dict[str, Any]] = []
    for record in records:
        payload = record["payload"]
        for key, flag in list(wanted.items()):
            if remaining.get(key, 0) <= 0:
                continue
            field, value = key
            if str(payload.get(field) or "").strip() != value:
                continue
            raw_field, raw_value = raw_match_context(payload, value)
            example = {
                "field": field,
                "standardized_value": value,
                "semantic_class": flag["semantic_class"],
                "proposed_destination": flag["proposed_destination"],
                "reason": flag["reason"],
                "assembly_accession": record["assembly_accession"],
                "biosample": record["biosample"],
                "organism": record["organism"],
                "raw_attribute_name": raw_field,
                "raw_value": raw_value,
            }
            for context_field in CONTEXT_FIELDS:
                example[context_field] = payload.get(context_field, "")
            examples.append(example)
            remaining[key] -= 1
    return examples


def target_value_examples(records: list[dict[str, Any]], value_rows: list[dict[str, Any]], per_value: int) -> list[dict[str, Any]]:
    indexed_rows = {(row["field"], row["standardized_value"]): row for row in value_rows}
    target_slugs = {slug(value) for value in TARGET_VALUES}
    candidates = []
    for row in value_rows:
        normalized = normalize(row["standardized_value"])
        if slug(normalized) in target_slugs or normalized in TARGET_VALUES:
            candidates.append({
                "field": row["field"],
                "standardized_value": row["standardized_value"],
                "semantic_class": row["semantic_class"],
                "proposed_destination": recommended_destination(row["semantic_class"]),
                "reason": row["reason"],
            })
    return example_rows(records, candidates, per_value)


def migration_decisions(flags: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for bucket, rows in flags.items():
        for row in rows:
            key = (row["field"], row["standardized_value"], row["semantic_class"])
            existing = merged.setdefault(key, {
                "current_field": row["field"],
                "standardized_value": row["standardized_value"],
                "semantic_class": row["semantic_class"],
                "affected_rows": row["row_count"],
                "proposed_destination": row["proposed_destination"],
                "decision": "review_required",
                "confidence": "audit_suggested",
                "rationale": row["reason"],
                "source_flags": set(),
            })
            existing["source_flags"].add(bucket)
    rows = []
    for row in merged.values():
        row = dict(row)
        row["source_flags"] = "|".join(sorted(row["source_flags"]))
        rows.append(row)
    return sorted(rows, key=lambda r: (-int(r["affected_rows"]), r["current_field"], r["standardized_value"]))


def proposed_new_fields() -> list[dict[str, Any]]:
    return [
        {"field": "Host_Study_Group_SD", "purpose": "Study-group assignment independent of disease and health state.", "examples": "case; control; exposed; contact", "release_policy": "derived/backward-compatible"},
        {"field": "Host_Care_Setting_SD", "purpose": "Care setting or hospitalization context.", "examples": "hospitalized; not hospitalized; inpatient; outpatient; ICU", "release_policy": "derived/backward-compatible"},
        {"field": "Host_Vital_Status_SD", "purpose": "Vital status, separate from health state.", "examples": "alive; deceased", "release_policy": "derived/backward-compatible"},
        {"field": "Host_Colonization_Status_SD", "purpose": "Carrier or colonization state, separate from disease.", "examples": "carrier; colonized; not colonized", "release_policy": "derived/backward-compatible"},
        {"field": "Host_Disease_Stage_SD", "purpose": "Stage/phase of disease rather than disease identity.", "examples": "acute; chronic; convalescent; exacerbation", "release_policy": "derived/backward-compatible"},
        {"field": "Sample_Context_SD", "purpose": "Sampling context when physical material is unspecified.", "examples": "clinical; environmental; food; surveillance; laboratory", "release_policy": "derived/backward-compatible"},
        {"field": "Sample_Processing_SD", "purpose": "Preparation or processing state.", "examples": "culture; enrichment culture; metagenomic assembly; DNA extract", "release_policy": "derived/backward-compatible"},
        {"field": "Sample_Collection_Device_SD", "purpose": "Device used to collect a specimen.", "examples": "swab; sponge; catheter; filter", "release_policy": "derived/backward-compatible"},
    ]


def field_contract_rows() -> list[dict[str, Any]]:
    descriptions = {
        "Host_SD": "Biological host taxon only.",
        "Host_Context_SD": "Broad host-associated context where taxon is unavailable or source-derived.",
        "Host_Disease_SD": "Named disease or defensible disease class only.",
        "Host_Health_State_SD": "Strict health state only: healthy, diseased, asymptomatic, symptomatic, unknown.",
        "Host_Production_Context_SD": "Production/husbandry context when applicable.",
        "Host_Anatomical_Site_SD": "Host anatomical part or plant part.",
        "Sample_Type_SD": "Physical specimen or material only.",
        "Sample_Type_SD_Broad": "Broad physical specimen/material class.",
        "Isolation_Source_SD": "Source context, not physical specimen itself.",
        "Isolation_Source_SD_Broad": "Broad source-context umbrella; legacy broad categories need explicit caveat.",
        "Isolation_Site_SD": "Anatomical isolation site or plant part only.",
        "Environment_Medium_SD": "Physical environmental medium only.",
        "Environment_Medium_SD_Broad": "Broad environmental medium class only.",
        "Environment_Broad_Scale_SD": "Broad environmental setting.",
        "Environment_Local_Scale_SD": "Local environmental or facility feature.",
    }
    return [
        {
            "field": field,
            "strict_meaning": descriptions.get(field, ""),
            "allowed_semantic_classes": "|".join(sorted(classes)),
            "phase1_action": "audit_only",
        }
        for field, classes in FIELD_CONTRACT.items()
    ]


def proposed_regression_tests(flags: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    examples = [
        ("patient", "Host_Context_SD=human-associated; Sample_Context_SD=clinical; no Host_Health_State_SD"),
        ("clinical patient", "human/clinical context; no Host_Health_State_SD=patient"),
        ("control", "Host_Study_Group_SD=control; do not infer healthy"),
        ("healthy control", "Host_Health_State_SD=healthy + Host_Study_Group_SD=control"),
        ("hospitalized", "Host_Care_Setting_SD=hospitalized"),
        ("carrier", "Host_Colonization_Status_SD=carrier; not Host_Disease_SD"),
        ("colonized", "Host_Colonization_Status_SD=colonized; not Host_Disease_SD"),
        ("dead", "Host_Vital_Status_SD=deceased"),
        ("convalescent", "Host_Disease_Stage_SD=convalescent"),
        ("pus", "Sample_Type_SD=pus/purulent material; no Isolation_Site_SD unless wound/abscess site explicit"),
        ("manure", "sample/environment fecal material; no Isolation_Site_SD"),
        ("cerebrospinal fluid", "Sample_Type_SD=cerebrospinal fluid; no site unless explicit CNS site"),
        ("bronchoalveolar lavage fluid", "Sample_Type_SD=lavage fluid + respiratory site"),
        ("gastric biopsy", "Sample_Type_SD=biopsy/tissue + gastrointestinal site"),
        ("sink", "Environment_Local_Scale_SD=sink; no Sample_Type_SD"),
        ("drain", "Environment_Local_Scale_SD=drain; no Sample_Type_SD"),
        ("metagenomic assembly", "Sample_Processing_SD=metagenomic assembly; no Sample_Type_SD material"),
        ("DNA extract", "Sample_Processing_SD=DNA extract"),
        ("banana blood disease", "Host_Disease_SD disease context; must not create blood sample type"),
    ]
    return [
        {"raw_value": raw_value, "expected_behavior": expected, "phase": "phase3_after_audit_approval"}
        for raw_value, expected in examples
    ]


def write_summary_md(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Host, Clinical, And Site Semantics Audit",
        "",
        f"Snapshot ID: `{summary['snapshot_id']}`",
        f"Generated: {summary['generated_at']}",
        f"Git commit: `{summary['git_commit']}`",
        "",
        "## Scope",
        "",
        "Phase 1 audit only. No production standardization rules, canonical metadata, Global Insights, or deployment state were changed.",
        "",
        "## Dataset",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Canonical rows audited | {summary['rows_audited']:,} |",
        f"| Distinct standardized field values classified | {summary['distinct_values_classified']:,} |",
        f"| Incompatible value-field pairs | {summary['incompatible_value_field_pairs']:,} |",
        f"| Affected rows represented by incompatibility flags | {summary['incompatibility_affected_rows']:,} |",
        "",
        "## Top Incompatibility Queues",
        "",
        "| Queue | Values | Affected rows |",
        "| --- | ---: | ---: |",
    ]
    for row in summary["incompatibility_queue_counts"]:
        lines.append(f"| `{row['queue']}` | {row['distinct_values']:,} | {row['affected_rows']:,} |")
    lines.extend([
        "",
        "## Interpretation",
        "",
        "The existing release gate remains useful for source leakage and broad-vocabulary control, but this audit evaluates a different question: whether host clinical status, specimen material, source context, site, and environment fields are semantically orthogonal.",
        "",
        "Large queues should be reviewed before rule changes. Some rows are legacy compatibility labels or broad context umbrellas, not necessarily immediate errors.",
        "",
        "## Recommended Next Step",
        "",
        "Review `recommended_migration_decisions.tsv` and approve a small high-confidence correction batch before changing production rules.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def generate_audit(snapshot_id: str, output_dir: Path, example_limit: int) -> dict[str, Any]:
    records = load_records(snapshot_id)
    value_rows = distinct_values(records)
    flags = incompatibility_rows(value_rows)
    flagged_rows = [row for rows in flags.values() for row in rows]
    unique_flagged = {(row["field"], row["standardized_value"]): row for row in flagged_rows}
    examples = example_rows(records, list(unique_flagged.values()), example_limit)
    target_examples = target_value_examples(records, value_rows, example_limit)
    all_examples = examples + [row for row in target_examples if (row["field"], row["standardized_value"], row["assembly_accession"]) not in {(e["field"], e["standardized_value"], e["assembly_accession"]) for e in examples}]
    migration_rows = migration_decisions(flags)
    queue_counts = [
        {
            "queue": queue,
            "distinct_values": len(rows),
            "affected_rows": sum(int(row["row_count"]) for row in rows),
        }
        for queue, rows in flags.items()
    ]
    queue_counts.sort(key=lambda row: (-row["affected_rows"], row["queue"]))
    summary = {
        "generated_at": utc_now(),
        "snapshot_id": snapshot_id,
        "git_commit": git_commit(),
        "phase": "audit_only",
        "production_rules_changed": False,
        "canonical_refresh_run": False,
        "global_insights_regenerated": False,
        "deployment_run": False,
        "rows_audited": len(records),
        "distinct_values_classified": len(value_rows),
        "incompatible_value_field_pairs": len(unique_flagged),
        "incompatibility_affected_rows": sum(int(row["affected_rows"]) for row in migration_rows),
        "incompatibility_queue_counts": queue_counts,
        "semantic_classes": SEMANTIC_CLASSES,
        "audit_note": "Counts are audit triage signals, not production-rule changes. Review before migration.",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    write_tsv(output_dir / "semantic_field_contract.tsv", ["field", "strict_meaning", "allowed_semantic_classes", "phase1_action"], field_contract_rows())
    write_tsv(output_dir / "all_standardized_values_classified.tsv", ["field", "standardized_value", "semantic_class", "compatible_with_field_contract", "row_count", "reason", "expected_semantic_classes"], field_count_rows(value_rows))
    for queue, filename in INCOMPATIBILITY_FILES.items():
        write_tsv(output_dir / filename, ["field", "standardized_value", "semantic_class", "row_count", "reason", "proposed_destination"], flags[queue])
    example_fields = ["field", "standardized_value", "semantic_class", "proposed_destination", "reason", "assembly_accession", "biosample", "organism", "raw_attribute_name", "raw_value"] + CONTEXT_FIELDS
    write_tsv(output_dir / "high_impact_examples.tsv", example_fields, all_examples)
    write_tsv(output_dir / "recommended_migration_decisions.tsv", ["current_field", "standardized_value", "semantic_class", "affected_rows", "proposed_destination", "decision", "confidence", "rationale", "source_flags"], migration_rows)
    write_tsv(output_dir / "proposed_new_fields.tsv", ["field", "purpose", "examples", "release_policy"], proposed_new_fields())
    write_tsv(output_dir / "proposed_regression_tests.tsv", ["raw_value", "expected_behavior", "phase"], proposed_regression_tests(flags))
    (output_dir / "audit_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_summary_md(output_dir / "audit_summary.md", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--example-limit", type=int, default=5)
    args = parser.parse_args()
    if args.example_limit < 1:
        parser.error("--example-limit must be positive")
    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    output_dir = args.output_dir or DEFAULT_OUTPUT_ROOT / date
    summary = generate_audit(args.snapshot_id, output_dir, args.example_limit)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
