#!/usr/bin/env python3
"""Audit host, clinical, sample, site, and environment semantic field usage.

Phase 1 audit only. The script reads canonical bacterial standardized metadata,
classifies standardized values into compositional semantic components, writes
review artifacts, and does not modify production rules or canonical metadata.
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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dataset_production_store import connect

DEFAULT_SNAPSHOT_ID = "20260602T140414Z_genbank_bacteria_root"
DEFAULT_OUTPUT_ROOT = ROOT / "standardization" / "review" / "host_clinical_site_semantics_audit"
CONTROLLED_CATEGORIES = ROOT / "standardization" / "controlled_categories.csv"
APPROVED_BROAD_CATEGORIES = ROOT / "standardization" / "approved_broad_categories.csv"

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
    "production_context",
    "disease",
    "health_state",
    "study_group",
    "hospitalization_status",
    "care_setting",
    "vital_status",
    "colonization_status",
    "disease_stage",
    "disease_outcome",
    "exposure_context",
    "sampling_context",
    "sample_material",
    "sample_entity",
    "data_product",
    "collection_device",
    "collection_method",
    "sample_processing",
    "anatomical_material",
    "anatomical_site",
    "environment_medium",
    "environment_local_scale",
    "environment_broad_scale",
    "source_context",
    "food_commodity",
    "metadata_descriptor",
    "unresolved",
]

# Existing public/download fields are treated as compatibility fields unless noted.
# Strict derived fields are proposed in artifacts; they are not written by this script.
FIELD_CONTRACT = {
    "Host_SD": {"host_taxon"},
    "Host_Context_SD": {"host_context", "production_context", "exposure_context"},
    "Host_Disease_SD": {"disease"},
    "Host_Health_State_SD": {"health_state"},
    "Host_Production_Context_SD": {"production_context"},
    "Host_Anatomical_Site_SD": {"anatomical_site"},
    "Sample_Type_SD": {"sample_material", "sample_entity", "data_product", "food_commodity", "collection_device", "sampling_context", "sample_processing"},
    "Sample_Type_SD_Broad": {"sample_material", "sample_entity", "data_product", "food_commodity", "collection_device", "sampling_context", "sample_processing", "source_context"},
    "Isolation_Source_SD": {"source_context", "food_commodity", "sample_material", "anatomical_material", "environment_medium", "environment_local_scale", "environment_broad_scale", "host_context"},
    "Isolation_Source_SD_Broad": {"source_context", "food_commodity", "sample_material", "anatomical_material", "environment_medium", "environment_broad_scale", "host_context"},
    "Isolation_Site_SD": {"anatomical_site"},
    "Environment_Medium_SD": {"environment_medium"},
    "Environment_Medium_SD_Broad": {"environment_medium"},
    "Environment_Broad_Scale_SD": {"environment_broad_scale"},
    "Environment_Local_Scale_SD": {"environment_local_scale", "care_setting"},
}

STRICT_FIELDS = {"Host_Disease_SD", "Host_Health_State_SD", "Host_Anatomical_Site_SD", "Isolation_Site_SD", "Environment_Medium_SD", "Environment_Broad_Scale_SD", "Environment_Local_Scale_SD"}
LEGACY_COMPATIBILITY_FIELDS = {"Sample_Type_SD", "Sample_Type_SD_Broad", "Isolation_Source_SD", "Isolation_Source_SD_Broad"}
CONFIRMED_STRICT_FIX_FIELDS = {"Host_Health_State_SD", "Host_Disease_SD", "Isolation_Site_SD"}

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
    "patient", "clinical patient", "patient sample", "diseased", "hospitalized",
    "non-hospitalized", "control", "healthy control", "healthy/no disease reported",
    "carrier", "colonized", "alive", "dead", "convalescent", "exacerbation",
    "specific pathogen free", "pus", "manure", "cerebrospinal fluid",
    "bronchoalveolar lavage fluid", "gut content", "gastric biopsy", "tracheal aspirate",
    "abscess", "catheter", "sink", "drain", "clinical sample", "clinical material",
    "clinical/host-associated material", "clinical fluid/material", "metagenomic assembly",
    "culture", "enrichment culture", "dna extract", "swab",
}

EXACT_METADATA_DESCRIPTORS = {
    "", "sample", "specimen", "other", "unknown", "uncategorized", "missing",
    "not applicable", "n/a", "na", "none", "null", "metadata descriptor/non-source",
    "metadata descriptor / non-source",
}

# Exact values that are intrinsically compositional or need contextual handling.
EXACT_COMPONENTS: dict[str, tuple[tuple[str, str], ...]] = {
    "patient": (("host_context", "patient/human clinical subject"),),
    "clinical patient": (("host_context", "human clinical subject"), ("sampling_context", "clinical")),
    "patient sample": (("host_context", "patient/human clinical subject"), ("sampling_context", "clinical")),
    "patient/clinical": (("host_context", "patient/human clinical subject"), ("sampling_context", "clinical")),
    "clinical": (("sampling_context", "clinical"),),
    "clinical sample": (("sampling_context", "clinical"),),
    "clinical material": (("sampling_context", "clinical"),),
    "clinical/host-associated material": (("source_context", "clinical or host-associated source context"),),
    "host-associated context": (("source_context", "host-associated source context"),),
    "clinical fluid/material": (("sample_material", "clinical fluid or purulent material"),),
    "clinical fluid material": (("sample_material", "clinical fluid or purulent material"),),
    "healthy": (("health_state", "healthy"),),
    "diseased": (("health_state", "diseased"),),
    "healthy/no disease reported": (("health_state", "healthy/no disease reported"),),
    "control": (("study_group", "control group"),),
    "healthy control": (("health_state", "healthy"), ("study_group", "control group")),
    "healthy/control": (("health_state", "healthy"), ("study_group", "control group")),
    "hospitalized": (("hospitalization_status", "hospitalized"),),
    "non-hospitalized": (("hospitalization_status", "not hospitalized"),),
    "non hospitalized": (("hospitalization_status", "not hospitalized"),),
    "not hospitalized": (("hospitalization_status", "not hospitalized"),),
    "inpatient": (("care_setting", "inpatient"),),
    "outpatient": (("care_setting", "outpatient"),),
    "carrier": (("colonization_status", "carrier"),),
    "carriage": (("colonization_status", "carrier"),),
    "colonized": (("colonization_status", "colonized"),),
    "alive": (("vital_status", "alive"),),
    "dead": (("vital_status", "deceased"),),
    "deceased": (("vital_status", "deceased"),),
    "convalescent": (("disease_stage", "convalescent"),),
    "exacerbation": (("disease_stage", "exacerbation"),),
    "specific pathogen free": (("production_context", "specific pathogen free"),),
    "broiler": (("production_context", "broiler production context"), ("host_context", "poultry")),
    "dairy cow": (("production_context", "dairy production context"), ("host_context", "cattle")),
    "wild caught": (("production_context", "wild caught"),),
    "farmed": (("production_context", "farmed"),),
    "laboratory-reared": (("production_context", "laboratory reared"),),
    "pus": (("sample_material", "pus/purulent material"), ("anatomical_material", "host anatomical material")),
    "purulent material": (("sample_material", "pus/purulent material"), ("anatomical_material", "host anatomical material")),
    "abscess": (("sample_material", "abscess material"), ("anatomical_site", "abscess/lesion site"), ("disease", "disease manifestation")),
    "cerebrospinal fluid": (("sample_material", "cerebrospinal fluid"), ("anatomical_material", "host anatomical material")),
    "bronchoalveolar lavage fluid": (("sample_material", "lavage fluid"), ("collection_method", "bronchoalveolar lavage"), ("anatomical_site", "lower respiratory tract")),
    "tracheal aspirate": (("sample_material", "aspirate"), ("collection_method", "aspiration"), ("anatomical_site", "trachea/lower respiratory tract")),
    "tracheal aspirate/secretion": (("sample_material", "aspirate/secretion"), ("collection_method", "aspiration"), ("anatomical_site", "trachea/lower respiratory tract")),
    "gastric biopsy": (("sample_material", "biopsy/tissue"), ("collection_method", "biopsy"), ("anatomical_site", "stomach/gastrointestinal tract")),
    "biopsy": (("sample_material", "biopsy/tissue"), ("collection_method", "biopsy")),
    "gut content": (("sample_material", "gut/intestinal content"), ("anatomical_material", "host anatomical material"), ("anatomical_site", "gastrointestinal tract")),
    "intestinal content": (("sample_material", "intestinal content"), ("anatomical_material", "host anatomical material"), ("anatomical_site", "gastrointestinal tract")),
    "manure": (("sample_material", "manure/fecal material"), ("environment_medium", "agricultural organic material")),
    "swab": (("sample_material", "swab specimen"), ("collection_device", "swab")),
    "sponge": (("collection_device", "sponge"),),
    "catheter": (("collection_device", "catheter"), ("source_context", "medical device")),
    "sink": (("environment_local_scale", "sink"),),
    "drain": (("environment_local_scale", "drain"),),
    "farm": (("environment_local_scale", "farm"),),
    "river": (("environment_local_scale", "river"),),
    "pond": (("environment_local_scale", "pond"),),
    "groundwater": (("environment_medium", "groundwater"),),
    "culture": (("sample_entity", "culture entity"), ("sample_processing", "cultured")),
    "cell culture": (("sample_entity", "cell culture"), ("sample_processing", "cultured")),
    "pure/single culture": (("sample_entity", "culture entity"), ("sample_processing", "pure/single culture")),
    "mixed culture": (("sample_entity", "mixed culture"), ("sample_processing", "mixed culture")),
    "microbial culture": (("sample_entity", "microbial culture"), ("sample_processing", "cultured")),
    "microbial isolate": (("sample_entity", "microbial isolate"), ("sample_processing", "isolated")),
    "bacterial culture": (("sample_entity", "bacterial culture"), ("sample_processing", "cultured")),
    "enrichment culture": (("sample_entity", "enrichment culture"), ("sample_processing", "enrichment")),
    "metagenomic assembly": (("data_product", "metagenomic assembly data product"), ("sample_processing", "assembly/sequence-derived")),
    "metagenome assembly": (("data_product", "metagenomic assembly data product"), ("sample_processing", "assembly/sequence-derived")),
    "dna extract": (("sample_material", "molecular extract"), ("sample_processing", "DNA extraction")),
    "molecular extract": (("sample_material", "molecular extract"), ("sample_processing", "molecular extraction")),
    "single cell": (("sample_entity", "single cell"), ("sample_processing", "single-cell preparation")),
    "food contact surface": (("food_commodity", "food production/contact context"), ("environment_local_scale", "contact surface")),
    "freshwater fish product": (("food_commodity", "fish product"),),
    "dairy farm": (("production_context", "dairy production setting"), ("host_context", "cattle/livestock"), ("environment_local_scale", "farm")),
    "non-food-contact surface": (("environment_local_scale", "non-food-contact surface"),),
    "non-food contact surface": (("environment_local_scale", "non-food-contact surface"),),
    "laboratory culture": (("sample_entity", "culture entity"), ("sample_processing", "cultured"), ("sampling_context", "laboratory")),
    "skin/body-surface swab": (("sample_material", "swab specimen"), ("collection_device", "swab"), ("anatomical_site", "skin/body surface")),
    "skin/body surface swab": (("sample_material", "swab specimen"), ("collection_device", "swab"), ("anatomical_site", "skin/body surface")),
    "rhizosphere": (("environment_local_scale", "rhizosphere"), ("host_context", "plant-associated")),
    "phyllosphere": (("environment_local_scale", "phyllosphere"), ("host_context", "plant-associated")),
    "endosphere": (("environment_local_scale", "endosphere/plant internal compartment"), ("host_context", "plant-associated")),
    "plant-associated material": (("source_context", "plant-associated material"), ("host_context", "plant-associated"), ("sampling_context", "plant-associated"), ("sample_material", "plant material")),
    "exposure/contact context": (("exposure_context", "exposure/contact context"),),
    "household contact": (("exposure_context", "household contact"),),
    "close contact": (("exposure_context", "close contact"),),
    "recovered": (("disease_outcome", "recovered"),),
    "survived": (("disease_outcome", "survived"),),
    "fatal outcome": (("disease_outcome", "fatal outcome"),),
    "ascites": (("sample_material", "ascitic fluid"), ("anatomical_material", "body fluid")),
    "dental plaque": (("sample_material", "dental plaque/biofilm"), ("environment_medium", "biofilm"), ("anatomical_site", "oral cavity/tooth surface")),
    "gill": (("anatomical_site", "gill"),),
    "bloodstream": (("anatomical_site", "bloodstream/vascular compartment"),),
    "secretion": (("sample_material", "secretion"), ("anatomical_material", "host anatomical material")),
    "cold seep": (("environment_local_scale", "cold seep"),),
    "estuary": (("environment_local_scale", "estuary"),),
    "glacier": (("environment_local_scale", "glacier"),),
    "anaerobic digester": (("environment_local_scale", "anaerobic digester"),),
    "deciduous forest": (("environment_broad_scale", "forest/terrestrial environment"), ("environment_local_scale", "deciduous forest")),
    "produced fluids from fractured shale": (("environment_medium", "produced shale fluid"), ("environment_local_scale", "fractured shale")),
    "produced shale fluids": (("environment_medium", "produced shale fluid"), ("environment_local_scale", "fractured shale")),
    "ice cream": (("food_commodity", "ice cream/dairy food"),),
    "ready-to-eat product": (("food_commodity", "ready-to-eat food product"),),
    "ready to eat product": (("food_commodity", "ready-to-eat food product"),),
    "kimchi": (("food_commodity", "fermented vegetable food"),),
    "prawn product": (("food_commodity", "seafood/prawn product"),),
    "catfish product": (("food_commodity", "fish product"),),
    "environmental/geologic material": (("source_context", "environmental or geologic source context"),),
    "banana blood disease": (("disease", "banana blood disease"),),
}

PATTERNS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    ("food_commodity", re.compile(r"\b(?:food|meat|beef|pork|seafood|cheese|yogurt|produce|ready[- ]?to[- ]?eat|ice cream|kimchi|(?:fish|prawn|catfish|shrimp|oyster|shellfish|dairy|egg|vegetable|fruit) product|food contact surface)\b", re.I), "explicit food or commodity context"),
    ("data_product", re.compile(r"\b(?:metagenomic assembly|metagenome assembly|metagenome[- ]assembled genome|sequence assembly)\b", re.I), "sequence-derived data product"),
    ("sample_entity", re.compile(r"\b(?:cell culture|mixed culture|pure culture|single culture|microbial isolate|isolate|whole organism|single[- ]?cell)\b", re.I), "sample entity or biological material form"),
    ("sample_processing", re.compile(r"\b(?:enrichment|cultured|culture|metagenom|assembly|dna extract|rna extract|extract|wgs|sequencing)\b", re.I), "processing or molecular preparation"),
    ("collection_device", re.compile(r"\b(?:swab|sponge|catheter|filter|trap)\b", re.I), "collection device"),
    ("collection_method", re.compile(r"\b(?:lavage|aspirate|aspiration|biopsy|wash)\b", re.I), "collection method"),
    ("hospitalization_status", re.compile(r"\b(?:hospitali[sz]ed|non[- ]?hospitali[sz]ed|not hospitali[sz]ed)\b", re.I), "hospitalization status"),
    ("care_setting", re.compile(r"\b(?:inpatient|outpatient|icu|intensive care|acute care hospital|chronic care facility|nursing home|long[- ]term care)\b", re.I), "care setting"),
    ("colonization_status", re.compile(r"\b(?:carrier|carriage|colonized|colonised|colonization|colonisation)\b", re.I), "colonization or carriage status"),
    ("vital_status", re.compile(r"\b(?:alive|dead|deceased)\b", re.I), "vital status"),
    ("disease_stage", re.compile(r"\b(?:convalescent|exacerbation)\b", re.I), "disease stage"),
    ("health_state", re.compile(r"\b(?:healthy|diseased|asymptomatic|symptomatic|sick)\b", re.I), "host health state"),
    ("disease", re.compile(r"\b(?:pneumonia|sepsis|bacteremia|bacteraemia|diarrh|infection|mastitis|meningitis|tuberculosis|cystic fibrosis|gastroenteritis|colitis|osteomyelitis|leukemia|listeriosis|salmonellosis|campylobacteriosis|pertussis|gastritis|neoplasm|cancer|otitis|obesity|legionellosis|buruli ulcer|tularemia|yersiniosis|syphilis|brucellosis|melioidosis|diphtheria|leptospirosis|diabetes|scleroderma|endocarditis|glaucoma|bronchiectasis|abortion|adenocarcinoma|lymphoma|injury|dental caries|disorder|disease)\b", re.I), "disease or disease class"),
    ("sample_material", re.compile(r"\b(?:blood|urine|feces|faeces|stool|sputum|saliva|mucus|tissue|fluid|pus|aspirate|biopsy|lavage|csf|cerebrospinal|bile|abscess|content|contents|manure|milk)\b", re.I), "physical specimen/material"),
    ("anatomical_material", re.compile(r"\b(?:blood|feces|faeces|stool|mucus|saliva|pus|cerebrospinal fluid|gut content|intestinal content)\b", re.I), "host anatomical material"),
    ("anatomical_site", re.compile(r"\b(?:skin|wound|rectum|rectal|perianal|nasal|nasopharynx|oropharynx|throat|oral|mouth|lung|bronch|trachea|respiratory tract|bladder|urogenital|vagina|cervix|uterus|gastrointestinal tract|colon|stomach|liver|kidney|brain|spleen|bone|eye|ear|placenta|lymph node|heart|breast|cloaca|gill|root|leaf|stem|sterile body site|organ/tissue site|tonsil/oropharyngeal site|cubital fossa)\b", re.I), "anatomical site or plant part"),
    ("environment_medium", re.compile(r"\b(?:soil|water|wastewater|sewage|sediment|sludge|biofilm|air|dust|compost|groundwater|seawater|freshwater|saline water|hospital wastewater|hospital sewage|manure)\b", re.I), "environmental medium"),
    ("environment_local_scale", re.compile(r"\b(?:sink|drain|farm|river|lake|pond|canal|stream|facility|hospital|laboratory|market|abattoir|slaughterhouse|reservoir|hot spring|cold seep|estuary|glacier|anaerobic digester|deciduous forest|fractured shale|cave|cleanroom|surface|floor|rhizosphere|phyllosphere|endosphere)\b", re.I), "local environmental or facility feature"),
    ("environment_broad_scale", re.compile(r"\b(?:built environment|healthcare-associated environment|agricultural environment|freshwater environment|marine environment|terrestrial environment|host-associated environment|plant-associated environment|laboratory environment)\b", re.I), "broad environmental setting"),
    ("production_context", re.compile(r"\b(?:specific pathogen free|broiler|dairy cow|wild caught|farmed|laboratory[- ]reared|lab[- ]reared)\b", re.I), "production or husbandry context"),
    ("host_context", re.compile(r"\b(?:human|patient|animal|poultry|cattle|cow|swine|pig|fish|plant|host-associated|plant-associated|animal-associated)\b", re.I), "host-associated context"),
    ("exposure_context", re.compile(r"\b(?:exposure/contact context|household contact|close contact|contact case|exposed contact)\b", re.I), "exposure/contact context"),
    ("disease_outcome", re.compile(r"\b(?:recovered|survived|fatal outcome|died of disease|disease outcome)\b", re.I), "disease outcome"),
    ("sampling_context", re.compile(r"\b(?:clinical sample|clinical material|clinical specimen|environmental sample|respiratory sample|cloacal sample|surveillance|laboratory sample|patient sample)\b", re.I), "sampling context"),
    ("source_context", re.compile(r"\b(?:clinical|host-associated|environmental material|source context|outbreak)\b", re.I), "broad source context"),
)

STUDY_GROUP_RE = re.compile(r"\b(?:control|case|household contact|close contact|contact case|exposed contact|exposed)\b", re.I)
ACUTE_CHRONIC_STAGE_RE = re.compile(r"\b(?:acute|chronic)\b", re.I)
CARE_EXCLUSION_RE = re.compile(r"\b(?:acute care|chronic care|hospital|facility|ward|unit)\b", re.I)
GENERIC_METADATA_RE = re.compile(r"^(?:sample|specimen|other|unknown|uncategorized|missing|not applicable|n/a|na|none|null)$", re.I)


@dataclass(frozen=True)
class ClassifiedValue:
    primary_semantic_class: str
    secondary_semantic_classes: tuple[str, ...]
    semantic_components: tuple[str, ...]
    confidence: str
    reason: str
    normalized_value: str

    @property
    def semantic_class(self) -> str:
        return self.primary_semantic_class

    @property
    def all_classes(self) -> tuple[str, ...]:
        return (self.primary_semantic_class, *self.secondary_semantic_classes)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", normalize(value)).strip("_") or "value"


def is_present(value: Any) -> bool:
    text = normalize(value)
    return bool(text) and text not in {"-", "--", "na", "n/a", "none", "not applicable", "unknown", "missing", "null"}


def add_component(components: list[tuple[str, str]], semantic_class: str, label: str) -> None:
    item = (semantic_class, label)
    if item not in components:
        components.append(item)


def semantic_priority_for_field(field: str) -> list[str]:
    field_priorities = {
        "Host_SD": ["host_taxon"],
        "Host_Context_SD": ["host_context", "production_context", "exposure_context"],
        "Host_Disease_SD": ["disease", "health_state"],
        "Host_Health_State_SD": ["health_state", "study_group", "hospitalization_status", "care_setting", "colonization_status"],
        "Host_Production_Context_SD": ["production_context"],
        "Host_Anatomical_Site_SD": ["anatomical_site", "anatomical_material"],
        "Sample_Type_SD": ["sample_material", "sample_entity", "data_product", "food_commodity", "collection_device", "sampling_context", "sample_processing"],
        "Sample_Type_SD_Broad": ["sample_material", "sample_entity", "data_product", "food_commodity", "collection_device", "sampling_context", "sample_processing", "source_context"],
        "Isolation_Source_SD": ["source_context", "sample_material", "environment_medium", "environment_local_scale", "food_commodity", "anatomical_site", "host_context"],
        "Isolation_Source_SD_Broad": ["source_context", "environment_broad_scale", "environment_medium", "sample_material", "food_commodity", "host_context"],
        "Isolation_Site_SD": ["anatomical_site", "anatomical_material", "sample_material"],
        "Environment_Medium_SD": ["environment_medium"],
        "Environment_Medium_SD_Broad": ["environment_medium"],
        "Environment_Broad_Scale_SD": ["environment_broad_scale"],
        "Environment_Local_Scale_SD": ["environment_local_scale", "care_setting"],
    }
    return field_priorities.get(field, []) + [
        "sample_material", "sample_entity", "data_product", "source_context", "food_commodity", "environment_medium",
        "environment_local_scale", "anatomical_site", "host_context", "disease", "health_state",
        "sampling_context", "collection_device", "collection_method", "sample_processing",
        "production_context", "unresolved",
    ]


def classify_value(value: Any, field: str = "", raw_attribute: str = "") -> ClassifiedValue:
    text = normalize(value)
    if not text:
        return ClassifiedValue("unresolved", (), (), "low", "Blank or missing value.", text)
    if GENERIC_METADATA_RE.fullmatch(text):
        return ClassifiedValue("metadata_descriptor", (), ("metadata_descriptor:generic descriptor",), "high", "Exact generic descriptor or missing-value token.", text)

    components: list[tuple[str, str]] = []
    if text in EXACT_COMPONENTS:
        components.extend(EXACT_COMPONENTS[text])
    else:
        if STUDY_GROUP_RE.search(text) and "food contact surface" not in text:
            add_component(components, "study_group", "study/exposure group")
        if ACUTE_CHRONIC_STAGE_RE.search(text) and not CARE_EXCLUSION_RE.search(text):
            add_component(components, "disease_stage", "acute/chronic disease stage")
        for semantic_class, pattern, label in PATTERNS:
            if semantic_class == "sampling_context" and GENERIC_METADATA_RE.fullmatch(text):
                continue
            if pattern.search(text):
                add_component(components, semantic_class, label)

    # Compound protections and additions. These keep audit labels additive and avoid broad token overreach.
    if "freshwater fish product" in text:
        components = [(klass, label) for klass, label in components if klass != "environment_medium"]
        add_component(components, "food_commodity", "freshwater fish product")
    if "non-food-contact surface" in text or "non-food contact surface" in text:
        components = [(klass, label) for klass, label in components if klass not in {"food_commodity", "study_group"}]
        add_component(components, "environment_local_scale", "non-food-contact surface")
    elif "food contact surface" in text:
        components = [(klass, label) for klass, label in components if klass != "study_group"]
        add_component(components, "food_commodity", "food contact context")
        add_component(components, "environment_local_scale", "contact surface")
    if "dairy farm" in text:
        components = [(klass, label) for klass, label in components if klass != "food_commodity"]
        add_component(components, "production_context", "dairy production setting")
        add_component(components, "host_context", "cattle/livestock")
        add_component(components, "environment_local_scale", "farm")
    if "laboratory culture" in text:
        components = [(klass, label) for klass, label in components if klass != "environment_local_scale"]
        add_component(components, "sample_entity", "culture entity")
        add_component(components, "sample_processing", "cultured")
        add_component(components, "sampling_context", "laboratory")
    if re.search(r"\b(?:acute care hospital|chronic care facility)\b", text):
        components = [(klass, label) for klass, label in components if klass != "disease_stage"]
        add_component(components, "care_setting", "care facility")
        add_component(components, "environment_local_scale", "healthcare facility")
    if re.search(r"\b(?:rectal|nasal|wound|cloacal|vaginal|throat|oropharyngeal|nasopharyngeal|skin|ear|eye|body[- ]surface|skin/body[- ]surface) swab\b", text):
        components = [(klass, label) for klass, label in components if not (klass == "environment_local_scale" and "surface" in label)]
        add_component(components, "sample_material", "swab specimen")
        add_component(components, "collection_device", "swab")
    if "respiratory sample" in text:
        add_component(components, "sampling_context", "respiratory sample")
        add_component(components, "anatomical_site", "respiratory tract")
    if "cloacal sample" in text:
        add_component(components, "sampling_context", "cloacal sample")
        add_component(components, "anatomical_site", "cloaca")
    if "sink biofilm" in text or "drain biofilm" in text:
        add_component(components, "environment_medium", "biofilm")
        add_component(components, "environment_local_scale", "sink/drain")
    if any(niche in text for niche in ("rhizosphere", "phyllosphere", "endosphere")):
        components = [(klass, label) for klass, label in components if klass != "anatomical_site"]
        if "rhizosphere" in text:
            add_component(components, "environment_local_scale", "rhizosphere")
        if "phyllosphere" in text:
            add_component(components, "environment_local_scale", "phyllosphere")
        if "endosphere" in text:
            add_component(components, "environment_local_scale", "endosphere/plant internal compartment")
        add_component(components, "host_context", "plant-associated")
    if "plant-associated material" in text:
        add_component(components, "source_context", "plant-associated material")
        add_component(components, "host_context", "plant-associated")
        add_component(components, "sampling_context", "plant-associated")
        add_component(components, "sample_material", "plant material")
    if re.search(r"\b(?:chicken|turkey|poultry|cattle|cow|swine|pig|fish|oyster|shrimp|shellfish)\b", text) and re.search(r"\b(?:feces|faeces|stool|tissue|gut|swab|milk|manure|blood|urine)\b", text) and not re.search(r"\b(?:food|meat|product|seafood|ready[- ]?to[- ]?eat)\b", text):
        components = [(klass, label) for klass, label in components if klass != "food_commodity"]
        add_component(components, "host_context", "animal-associated")
    if "mastitis milk" in text:
        add_component(components, "disease", "mastitis")
        add_component(components, "sample_material", "milk")
        add_component(components, "host_context", "animal/livestock context")

    raw_name = normalize(raw_attribute)
    if raw_name:
        if "collection device" in raw_name:
            add_component(components, "collection_device", "raw attribute indicates collection device")
        if "collection method" in raw_name:
            add_component(components, "collection_method", "raw attribute indicates collection method")
        if "disease outcome" in raw_name:
            add_component(components, "disease_outcome", "raw attribute indicates disease outcome")
        if "exposure" in raw_name:
            add_component(components, "exposure_context", "raw attribute indicates exposure context")
    if not components and re.search(r"^[A-Z][a-z]+(?: [a-z]+)?$", str(value or "").strip()):
        add_component(components, "host_taxon", "taxonomic-looking label")
    if not components:
        add_component(components, "unresolved", "no deterministic semantic component")

    ordered_classes: list[str] = []
    component_strings: list[str] = []
    for klass, label in components:
        if klass not in ordered_classes:
            ordered_classes.append(klass)
        component_strings.append(f"{klass}:{label}")

    priority = semantic_priority_for_field(field)
    primary = next((klass for klass in priority if klass in ordered_classes), ordered_classes[0])
    secondary = tuple(klass for klass in ordered_classes if klass != primary)
    confidence = "high" if text in EXACT_COMPONENTS or len(ordered_classes) == 1 else "medium"
    if "unresolved" in ordered_classes or primary == "unresolved":
        confidence = "low"
    reason = "; ".join(component_strings)
    return ClassifiedValue(primary, secondary, tuple(component_strings), confidence, reason, text)


def expected_classes(field: str) -> set[str]:
    return FIELD_CONTRACT.get(field, set())


def class_destinations(semantic_class: str) -> tuple[str, ...]:
    mapping = {
        "host_taxon": ("Host_SD",),
        "host_context": ("Host_Context_SD",),
        "production_context": ("Host_Production_Context_SD",),
        "disease": ("Host_Disease_SD",),
        "health_state": ("Host_Health_State_SD",),
        "study_group": ("Host_Study_Group_SD",),
        "hospitalization_status": ("Host_Hospitalization_Status_SD",),
        "care_setting": ("Host_Care_Setting_SD",),
        "vital_status": ("Host_Vital_Status_SD",),
        "colonization_status": ("Host_Colonization_Status_SD",),
        "disease_stage": ("Host_Disease_Stage_SD",),
        "disease_outcome": ("Host_Disease_Outcome_SD",),
        "exposure_context": ("Host_Exposure_Context_SD",),
        "sampling_context": ("Sampling_Context_SD",),
        "sample_material": ("Sample_Material_SD", "Sample_Type_SD"),
        "sample_entity": ("Sample_Entity_SD", "Sample_Type_SD"),
        "data_product": ("Data_Product_SD",),
        "collection_device": ("Sample_Collection_Device_SD",),
        "collection_method": ("Sample_Collection_Method_SD",),
        "sample_processing": ("Sample_Processing_SD",),
        "anatomical_material": ("Host_Anatomical_Material_SD",),
        "anatomical_site": ("Host_Anatomical_Site_SD", "Isolation_Site_SD"),
        "environment_medium": ("Environment_Medium_SD",),
        "environment_local_scale": ("Environment_Local_Scale_SD",),
        "environment_broad_scale": ("Environment_Broad_Scale_SD",),
        "source_context": ("Isolation_Source_SD",),
        "food_commodity": ("Isolation_Source_SD", "Sample_Type_SD"),
        "metadata_descriptor": ("suppress/provenance only",),
        "unresolved": ("admin_review",),
    }
    return mapping.get(semantic_class, ("admin_review",))


def additive_destinations(classified: ClassifiedValue, current_field: str) -> tuple[str, ...]:
    destinations: list[str] = []
    for klass in classified.all_classes:
        for destination in class_destinations(klass):
            if destination != current_field and destination not in destinations:
                destinations.append(destination)
    return tuple(destinations)


def field_compatibility(field: str, classified: ClassifiedValue) -> str:
    expected = expected_classes(field)
    if not expected:
        return "uncontracted"
    if any(klass in expected for klass in classified.all_classes):
        if len(classified.all_classes) > 1:
            return "compatible_composite"
        return "compatible"
    if field in LEGACY_COMPATIBILITY_FIELDS:
        return "legacy_decomposition_candidate"
    return "incompatible"


def remove_from_current_field(field: str, classified: ClassifiedValue) -> bool:
    compatibility = field_compatibility(field, classified)
    if compatibility in {"compatible", "compatible_composite", "legacy_decomposition_candidate"}:
        return False
    return field in STRICT_FIELDS and classified.confidence in {"high", "medium"}


def action_class(field: str, classified: ClassifiedValue) -> str:
    compatibility = field_compatibility(field, classified)
    if classified.confidence == "low" or "unresolved" in classified.all_classes:
        return "classifier_uncertain"
    if compatibility == "incompatible" and remove_from_current_field(field, classified):
        if field in CONFIRMED_STRICT_FIX_FIELDS and classified.confidence == "high":
            return "confirmed_high_confidence_fix"
        return "manual_review"
    if len(classified.all_classes) > 1:
        return "composite_requires_split"
    if compatibility == "legacy_decomposition_candidate":
        return "legacy_compatibility_label"
    if compatibility == "compatible" and additive_destinations(classified, field):
        return "additive_axis_enrichment"
    if compatibility == "compatible":
        return "compatible_no_action"
    return "manual_review" if compatibility == "incompatible" else "legacy_compatibility_label"


def git_commit() -> str:
    configured = str(os.environ.get("FETCHM_WEBAPP_GIT_COMMIT") or "").strip()
    if configured:
        return configured
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT.parent, capture_output=True, text=True, check=False)
    return result.stdout.strip() or "unknown"


def sha256_file(path: Path) -> str:
    if not path.exists():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            return json.loads(value)
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
            classified = classify_value(value, field=field)
            compatibility = field_compatibility(field, classified)
            rows.append({
                "field": field,
                "standardized_value": value,
                "primary_semantic_class": classified.primary_semantic_class,
                "secondary_semantic_classes": "|".join(classified.secondary_semantic_classes),
                "semantic_components": "|".join(classified.semantic_components),
                "confidence": classified.confidence,
                "field_compatibility": compatibility,
                "additive_destinations": "|".join(additive_destinations(classified, field)),
                "remove_from_current_field": str(remove_from_current_field(field, classified)).lower(),
                "review_required": str(action_class(field, classified) in {"manual_review", "classifier_uncertain", "composite_requires_split", "confirmed_high_confidence_fix"}).lower(),
                "action_class": action_class(field, classified),
                "row_count": count,
                "reason": classified.reason,
                "expected_semantic_classes": "|".join(sorted(expected_classes(field))),
            })
    return rows


def field_count_rows(value_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(value_rows, key=lambda row: (row["field"], -int(row["row_count"]), row["standardized_value"]))


def queue_row(row: dict[str, Any], reason: str, proposed_destination: str) -> dict[str, Any]:
    return {
        "field": row["field"],
        "standardized_value": row["standardized_value"],
        "primary_semantic_class": row["primary_semantic_class"],
        "secondary_semantic_classes": row["secondary_semantic_classes"],
        "semantic_components": row["semantic_components"],
        "confidence": row["confidence"],
        "field_compatibility": row["field_compatibility"],
        "row_count": row["row_count"],
        "reason": reason,
        "additive_destinations": row["additive_destinations"],
        "proposed_destination": proposed_destination,
        "remove_from_current_field": row["remove_from_current_field"],
        "action_class": row["action_class"],
    }


def class_set(row: dict[str, Any]) -> set[str]:
    return {row["primary_semantic_class"], *[klass for klass in row["secondary_semantic_classes"].split("|") if klass]}


def incompatibility_rows(value_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    flags: dict[str, list[dict[str, Any]]] = {key: [] for key in INCOMPATIBILITY_FILES}
    for row in value_rows:
        field = row["field"]
        classes = class_set(row)
        if field == "Host_Health_State_SD" and not classes <= {"health_state"}:
            flags["non_health_values_in_host_health_state"].append(queue_row(row, "Host_Health_State_SD should contain strict health states only; other axes should be additive derived fields.", row["additive_destinations"]))
        if field == "Host_Disease_SD" and "disease" not in classes:
            flags["non_disease_values_in_host_disease"].append(queue_row(row, "Host_Disease_SD should contain named diseases or defensible disease classes only.", row["additive_destinations"]))
        if field in {"Sample_Type_SD", "Sample_Type_SD_Broad"} and not classes & expected_classes(field):
            flags["non_material_values_in_sample_type"].append(queue_row(row, "Sample_Type compatibility field contains a value outside the field contract and should be reviewed before decomposition.", row["additive_destinations"]))
        if field == "Sample_Type_SD" and classes & {"environment_medium", "environment_local_scale", "environment_broad_scale"}:
            flags["environment_values_in_sample_type"].append(queue_row(row, "Environmental semantics in Sample_Type_SD should be decomposed into environment fields while preserving compatibility if needed.", row["additive_destinations"]))
        if field == "Sample_Type_SD" and "sample_processing" in classes:
            flags["processing_values_in_sample_type"].append(queue_row(row, "Processing/preparation semantics should be additively represented in Sample_Processing_SD.", row["additive_destinations"]))
        if field == "Isolation_Site_SD" and "anatomical_site" not in classes:
            flags["non_site_values_in_isolation_site"].append(queue_row(row, "Isolation_Site_SD should contain anatomical sites or plant parts only.", row["additive_destinations"]))
        if field in {"Isolation_Source_SD", "Isolation_Source_SD_Broad"} and classes & {"sample_material", "anatomical_material"}:
            flags["material_values_in_isolation_source"].append(queue_row(row, "Material in source fields is a decomposition candidate, not necessarily an error under NCBI-compatible isolation_source semantics.", row["additive_destinations"]))
        if field == "Host_Health_State_SD" and classes & {"hospitalization_status", "care_setting"}:
            flags["care_setting_values_in_health_state"].append(queue_row(row, "Care or hospitalization status should be a separate derived axis.", row["additive_destinations"]))
        if field == "Host_Health_State_SD" and "study_group" in classes:
            flags["study_group_values_in_health_state"].append(queue_row(row, "Study group should be separate from host health state.", row["additive_destinations"]))
        if field == "Host_Health_State_SD" and "vital_status" in classes:
            flags["vital_status_values_in_health_state"].append(queue_row(row, "Vital status should be separate from host health state.", row["additive_destinations"]))
        if field == "Host_Health_State_SD" and "colonization_status" in classes:
            flags["colonization_values_in_health_state"].append(queue_row(row, "Colonization/carrier status should be separate from disease and health state.", row["additive_destinations"]))
        if field == "Host_Health_State_SD" and "disease_stage" in classes:
            flags["disease_stage_values_in_health_state"].append(queue_row(row, "Disease stage should be separate from host health state.", row["additive_destinations"]))
        if field in {"Isolation_Site_SD", "Host_Anatomical_Site_SD"} and classes & {"sample_material", "anatomical_material", "environment_medium", "environment_local_scale", "source_context"} and "anatomical_site" not in classes:
            flags["conflicting_anatomical_site_fields"].append(queue_row(row, "Site field contains non-site material/context without an anatomical-site component.", row["additive_destinations"]))
        if field == "Isolation_Source_SD_Broad" and row["field_compatibility"] in {"legacy_decomposition_candidate", "incompatible"}:
            flags["ambiguous_broad_source_categories"].append(queue_row(row, "Broad source category is a legacy umbrella or decomposition candidate; document before migration.", row["additive_destinations"]))
    return {key: sorted(rows, key=lambda r: (-int(r["row_count"]), r["field"], r["standardized_value"])) for key, rows in flags.items()}


def raw_match_context(payload: dict[str, Any], value: str) -> tuple[str, str, str]:
    normalized_value = normalize(value)
    exact_candidates = []
    partial_candidates = []
    for field in RAW_CONTEXT_FIELDS:
        raw_value = payload.get(field)
        if not is_present(raw_value):
            continue
        text = normalize(raw_value)
        if normalized_value == text:
            exact_candidates.append((field, str(raw_value), "exact"))
        elif normalized_value and (normalized_value in text or text in normalized_value):
            partial_candidates.append((field, str(raw_value), "partial"))
    if exact_candidates:
        return exact_candidates[0]
    if partial_candidates:
        return partial_candidates[0]
    return "", "", "unmatched"


def example_rows(records: list[dict[str, Any]], flagged_rows: list[dict[str, Any]], per_value: int) -> list[dict[str, Any]]:
    wanted = {(row["field"], row["standardized_value"]): row for row in flagged_rows}
    remaining = {key: per_value for key in wanted}
    active = set(wanted)
    examples: list[dict[str, Any]] = []
    for record in records:
        if not active:
            break
        payload = record["payload"]
        for field in AUDIT_FIELDS:
            value = str(payload.get(field) or "").strip()
            key = (field, value)
            if key not in active:
                continue
            flag = wanted[key]
            raw_field, raw_value, evidence_match = raw_match_context(payload, value)
            example = {
                "field": field,
                "standardized_value": value,
                "primary_semantic_class": flag["primary_semantic_class"],
                "secondary_semantic_classes": flag["secondary_semantic_classes"],
                "semantic_components": flag["semantic_components"],
                "action_class": flag["action_class"],
                "additive_destinations": flag["additive_destinations"],
                "remove_from_current_field": flag["remove_from_current_field"],
                "reason": flag["reason"],
                "assembly_accession": record["assembly_accession"],
                "biosample": record["biosample"],
                "organism": record["organism"],
                "raw_attribute_name": raw_field,
                "raw_value": raw_value,
                "evidence_match_type": evidence_match,
            }
            for context_field in CONTEXT_FIELDS:
                example[context_field] = payload.get(context_field, "")
            examples.append(example)
            remaining[key] -= 1
            if remaining[key] <= 0:
                active.remove(key)
    return examples


def target_value_examples(records: list[dict[str, Any]], value_rows: list[dict[str, Any]], per_value: int) -> list[dict[str, Any]]:
    target_slugs = {slug(value) for value in TARGET_VALUES}
    candidates = []
    for row in value_rows:
        normalized = normalize(row["standardized_value"])
        if slug(normalized) in target_slugs or normalized in TARGET_VALUES:
            candidates.append(queue_row(row, "Targeted value requested for semantic review.", row["additive_destinations"]))
    return example_rows(records, candidates, per_value)


def migration_decisions(flags: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for bucket, rows in flags.items():
        for row in rows:
            key = (row["field"], row["standardized_value"])
            existing = merged.setdefault(key, {
                "current_field": row["field"],
                "standardized_value": row["standardized_value"],
                "primary_semantic_class": row["primary_semantic_class"],
                "secondary_semantic_classes": row["secondary_semantic_classes"],
                "semantic_components": row["semantic_components"],
                "affected_assignments": row["row_count"],
                "additive_destinations": row["additive_destinations"],
                "remove_from_current_field": row["remove_from_current_field"],
                "action_class": row["action_class"],
                "confidence": row["confidence"],
                "rationale": row["reason"],
                "source_flags": set(),
            })
            existing["source_flags"].add(bucket)
    rows = []
    for row in merged.values():
        materialized = dict(row)
        materialized["source_flags"] = "|".join(sorted(row["source_flags"]))
        rows.append(materialized)
    return sorted(rows, key=lambda r: (-int(r["affected_assignments"]), r["current_field"], r["standardized_value"]))


def proposed_new_fields() -> list[dict[str, Any]]:
    return [
        {"field": "Sample_Material_SD", "field_status": "new_derived_field", "purpose": "Strict physical specimen/material axis.", "examples": "blood; urine; pus; cerebrospinal fluid; manure", "release_policy": "derived/additive"},
        {"field": "Sampling_Context_SD", "field_status": "new_derived_field", "purpose": "Sampling context when physical material is unspecified or supplemental.", "examples": "clinical; environmental; food; surveillance; laboratory", "release_policy": "derived/additive"},
        {"field": "Sample_Processing_SD", "field_status": "new_derived_field", "purpose": "Preparation or processing state.", "examples": "culture; enrichment culture; DNA extract", "release_policy": "derived/additive"},
        {"field": "Sample_Collection_Device_SD", "field_status": "new_derived_field", "purpose": "Device used to collect a specimen.", "examples": "swab; sponge; catheter; filter", "release_policy": "derived/additive"},
        {"field": "Sample_Collection_Method_SD", "field_status": "new_derived_field", "purpose": "Method used to obtain the specimen.", "examples": "bronchoalveolar lavage; aspiration; biopsy", "release_policy": "derived/additive"},
        {"field": "Sample_Entity_SD", "field_status": "new_derived_field", "purpose": "Biological material form/entity, especially culture/isolate labels.", "examples": "microbial isolate; cell culture; mixed culture; single cell", "release_policy": "derived/additive"},
        {"field": "Data_Product_SD", "field_status": "new_derived_field", "purpose": "Sequence-derived or computational data product, not a biological sample entity.", "examples": "metagenomic assembly; sequence assembly", "release_policy": "derived/additive"},
        {"field": "Host_Anatomical_Material_SD", "field_status": "new_derived_field", "purpose": "Host-derived anatomical material distinct from anatomical part/site.", "examples": "stool; mucus; saliva; pus; blood; CSF", "release_policy": "derived/additive"},
        {"field": "Host_Anatomical_Site_SD", "field_status": "existing_field_to_enforce", "purpose": "Anatomical part of a confirmed host.", "examples": "rectum; lung; nasopharynx; skin; stomach", "release_policy": "existing/additive"},
        {"field": "Host_Study_Group_SD", "field_status": "new_derived_field", "purpose": "Study-group assignment independent of disease and health state.", "examples": "case; control; exposed; contact", "release_policy": "derived/additive"},
        {"field": "Host_Hospitalization_Status_SD", "field_status": "new_derived_field", "purpose": "Hospitalization status only.", "examples": "hospitalized; not hospitalized", "release_policy": "derived/additive"},
        {"field": "Host_Care_Setting_SD", "field_status": "new_derived_field", "purpose": "Care setting or facility context.", "examples": "inpatient; outpatient; ICU; long-term care", "release_policy": "derived/additive"},
        {"field": "Host_Vital_Status_SD", "field_status": "new_derived_field", "purpose": "Vital status, separate from health state and disease outcome.", "examples": "alive; deceased", "release_policy": "derived/additive"},
        {"field": "Host_Colonization_Status_SD", "field_status": "new_derived_field", "purpose": "Carrier or colonization state, separate from disease.", "examples": "carrier; colonized; not colonized", "release_policy": "derived/additive"},
        {"field": "Host_Disease_Stage_SD", "field_status": "new_derived_field", "purpose": "Stage/phase of disease rather than disease identity.", "examples": "acute; chronic; convalescent; exacerbation", "release_policy": "derived/additive"},
        {"field": "Host_Disease_Outcome_SD", "field_status": "new_derived_field", "purpose": "Disease outcome when explicit.", "examples": "recovered; survived; fatal outcome", "release_policy": "derived/additive"},
        {"field": "Host_Exposure_Context_SD", "field_status": "new_derived_field", "purpose": "Exposure/contact context independent of study group.", "examples": "household contact; close contact; exposure/contact context", "release_policy": "derived/additive"},
        {"field": "Host_Production_Context_SD", "field_status": "existing_field_to_expand", "purpose": "Production, husbandry, or rearing context.", "examples": "specific pathogen free; broiler; dairy cow; farmed", "release_policy": "existing/additive"},
        {"field": "Sample_Type_SD", "field_status": "legacy_field", "purpose": "Backward-compatible NCBI-style sample type umbrella retained during decomposition.", "examples": "cell culture; swab specimen; metagenomic assembly", "release_policy": "preserve"},
        {"field": "Isolation_Source_SD", "field_status": "legacy_field", "purpose": "Backward-compatible NCBI-style physical/environmental/local source retained during decomposition.", "examples": "blood; soil; water; food", "release_policy": "preserve"},
    ]


def field_contract_rows() -> list[dict[str, Any]]:
    descriptions = {
        "Host_SD": "Biological host taxon only.",
        "Host_Context_SD": "Broad host-associated context where taxon is unavailable or source-derived.",
        "Host_Disease_SD": "Named disease or defensible disease class only.",
        "Host_Health_State_SD": "Strict health state only: healthy, diseased, asymptomatic, symptomatic, unknown.",
        "Host_Production_Context_SD": "Production/husbandry context when applicable.",
        "Host_Anatomical_Site_SD": "Anatomical part of a confirmed biological host.",
        "Sample_Type_SD": "Backward-compatible NCBI-style umbrella sample type; decompose into strict derived axes before migration.",
        "Sample_Type_SD_Broad": "Backward-compatible broad sample type/context umbrella.",
        "Isolation_Source_SD": "Backward-compatible normalized physical/environmental/local source value; not a strict context-only field.",
        "Isolation_Source_SD_Broad": "Broad source-context umbrella; may include compatibility categories requiring decomposition.",
        "Isolation_Site_SD": "Sampling/isolation site when no host-specific assertion is made, or legacy site field pending contract decision.",
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
            "field_role": "legacy_compatibility" if field in LEGACY_COMPATIBILITY_FIELDS else "strict_or_targeted",
            "phase1_action": "audit_only",
        }
        for field, classes in FIELD_CONTRACT.items()
    ]


def proposed_regression_tests() -> list[dict[str, Any]]:
    examples = [
        ("environmental sample", "sampling_context=environmental; not metadata_descriptor only"),
        ("respiratory sample", "sampling_context + respiratory anatomical/site context; not metadata_descriptor only"),
        ("cloacal sample", "sampling_context + cloaca site; not metadata_descriptor only"),
        ("food contact surface", "food/built-environment contact surface; not study group"),
        ("acute care hospital", "care setting/local scale; not disease stage"),
        ("freshwater fish product", "food commodity; not environment medium"),
        ("healthy control", "health_state=healthy + study_group=control"),
        ("rectal swab", "swab specimen + collection_device=swab + anatomical_site=rectum"),
        ("sink biofilm", "environment_local_scale=sink + environment_medium=biofilm"),
        ("gastric biopsy", "sample_material=biopsy + anatomical_site=stomach/gastrointestinal tract"),
        ("manure", "sample_material/fecal material + agricultural/environment medium; no anatomical site"),
        ("abscess", "composite/ambiguous material + site/disease manifestation; review before removal"),
        ("cell culture", "sample entity + processing, not processing-only"),
        ("specific pathogen free", "production context, not health state"),
        ("patient sample", "human/clinical sampling context; not health state"),
        ("banana blood disease", "disease context; must not create blood sample type"),
    ]
    return [{"raw_value": raw_value, "expected_behavior": expected, "phase": "phase1_classifier_regression"} for raw_value, expected in examples]


def candidate_rows_by_action(value_rows: list[dict[str, Any]], action: str) -> list[dict[str, Any]]:
    return sorted(
        [row for row in value_rows if row["action_class"] == action],
        key=lambda row: (-int(row["row_count"]), row["field"], row["standardized_value"]),
    )


def compatible_composite_rows(value_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [row for row in value_rows if row["field_compatibility"] == "compatible_composite"],
        key=lambda row: (-int(row["row_count"]), row["field"], row["standardized_value"]),
    )


def semantic_signal_rows(value_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in value_rows if row["action_class"] != "compatible_no_action"]


def action_and_unique_counts(records: list[dict[str, Any]], signal_rows: list[dict[str, Any]]) -> dict[str, Any]:
    flagged = {(row.get("current_field") or row["field"], row["standardized_value"]): row for row in signal_rows}
    unique_any: set[str] = set()
    unique_confirmed: set[str] = set()
    unique_review: set[str] = set()
    assignment_occurrences = 0
    action_counts: Counter[str] = Counter()
    for record in records:
        accession = record["assembly_accession"]
        payload = record["payload"]
        seen_for_record = False
        seen_confirmed = False
        seen_review = False
        for field in AUDIT_FIELDS:
            value = str(payload.get(field) or "").strip()
            row = flagged.get((field, value))
            if not row:
                continue
            assignment_occurrences += 1
            action = row["action_class"]
            action_counts[action] += 1
            seen_for_record = True
            if action == "confirmed_high_confidence_fix":
                seen_confirmed = True
            if action in {"manual_review", "classifier_uncertain", "composite_requires_split"}:
                seen_review = True
        if seen_for_record:
            unique_any.add(accession)
        if seen_confirmed:
            unique_confirmed.add(accession)
        if seen_review:
            unique_review.add(accession)
    return {
        "unique_assemblies_with_any_semantic_signal": len(unique_any),
        "unique_assemblies_with_confirmed_high_confidence_signal": len(unique_confirmed),
        "unique_assemblies_with_review_or_uncertain_signal": len(unique_review),
        "field_value_assignment_occurrences": assignment_occurrences,
        "action_assignment_counts": dict(sorted(action_counts.items())),
    }


def write_summary_md(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Host, Clinical, And Site Semantics Audit V2",
        "",
        f"Snapshot ID: `{summary['canonical_snapshot_id']}`",
        f"Generated: {summary['generated_at']}",
        f"Input standardization commit: `{summary['input_standardization_commit']}`",
        f"Audit code commit: `{summary['audit_code_commit']}`",
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
        f"| All candidate assignment occurrences | {summary['field_value_assignment_occurrences']:,} |",
        f"| Field-value pairs with queue signals | {summary['field_value_pairs_with_queue_signals']:,} |",
        f"| Field-value pairs with any semantic candidate signal | {summary['field_value_pairs_with_any_semantic_signal']:,} |",
        f"| Unique assemblies with any semantic candidate signal | {summary['unique_assemblies_with_any_semantic_signal']:,} |",
        f"| Unique assemblies with confirmed high-confidence signal | {summary['unique_assemblies_with_confirmed_high_confidence_signal']:,} |",
        f"| Unique assemblies with review/uncertain signal | {summary['unique_assemblies_with_review_or_uncertain_signal']:,} |",
        "",
        "## Action Classes",
        "",
        "| Action class | Assignment occurrences |",
        "| --- | ---: |",
    ]
    for action, count in summary["action_assignment_counts"].items():
        lines.append(f"| `{action}` | {count:,} |")
    lines.extend([
        "",
        "## Queue Summary",
        "",
        "| Queue | Values | Assignment count |",
        "| --- | ---: | ---: |",
    ])
    for row in summary["incompatibility_queue_counts"]:
        lines.append(f"| `{row['queue']}` | {row['distinct_values']:,} | {row['assignment_count']:,} |")
    lines.extend([
        "",
        "## Interpretation",
        "",
        "This V2 audit uses compositional classification. Counts are candidate signals and decomposition opportunities, not confirmed erroneous records. `confirmed_high_confidence_fix` is intentionally limited to reviewed strict host-health, host-disease, and isolation-site violations; environment and legacy compatibility candidates remain review or additive queues. Legacy umbrella fields such as `Isolation_Source_SD` and `Sample_Type_SD` are not treated as strict ontologies; strict derived axes are proposed separately.",
        "",
        "## Recommended Next Step",
        "",
        "Review confirmed high-confidence fixes and composite split candidates. Do not start broad remapping until the field contract is approved.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def generate_audit(snapshot_id: str, output_dir: Path, example_limit: int, command_line: str) -> dict[str, Any]:
    records = load_records(snapshot_id)
    value_rows = distinct_values(records)
    flags = incompatibility_rows(value_rows)
    flagged_rows = [row for rows in flags.values() for row in rows]
    unique_flagged = {(row["field"], row["standardized_value"]): row for row in flagged_rows}
    examples = example_rows(records, list(unique_flagged.values()), example_limit)
    target_examples = target_value_examples(records, value_rows, example_limit)
    example_keys = {(row["field"], row["standardized_value"], row["assembly_accession"]) for row in examples}
    all_examples = examples + [row for row in target_examples if (row["field"], row["standardized_value"], row["assembly_accession"]) not in example_keys]
    migration_rows = migration_decisions(flags)
    all_signal_rows = semantic_signal_rows(value_rows)
    unique_counts = action_and_unique_counts(records, all_signal_rows)
    queue_counts = [
        {"queue": queue, "distinct_values": len(rows), "assignment_count": sum(int(row["row_count"]) for row in rows)}
        for queue, rows in flags.items()
    ]
    queue_counts.sort(key=lambda row: (-row["assignment_count"], row["queue"]))
    output_dir.mkdir(parents=True, exist_ok=True)

    value_header = [
        "field", "standardized_value", "primary_semantic_class", "secondary_semantic_classes",
        "semantic_components", "confidence", "field_compatibility", "additive_destinations",
        "remove_from_current_field", "review_required", "action_class", "row_count", "reason",
        "expected_semantic_classes",
    ]
    queue_header = [
        "field", "standardized_value", "primary_semantic_class", "secondary_semantic_classes",
        "semantic_components", "confidence", "field_compatibility", "row_count", "reason",
        "additive_destinations", "proposed_destination", "remove_from_current_field", "action_class",
    ]
    example_header = [
        "field", "standardized_value", "primary_semantic_class", "secondary_semantic_classes",
        "semantic_components", "action_class", "additive_destinations", "remove_from_current_field",
        "reason", "assembly_accession", "biosample", "organism", "raw_attribute_name", "raw_value",
        "evidence_match_type", *CONTEXT_FIELDS,
    ]
    migration_header = [
        "current_field", "standardized_value", "primary_semantic_class", "secondary_semantic_classes",
        "semantic_components", "affected_assignments", "additive_destinations", "remove_from_current_field",
        "action_class", "confidence", "rationale", "source_flags",
    ]
    write_tsv(output_dir / "semantic_field_contract.tsv", ["field", "strict_meaning", "allowed_semantic_classes", "field_role", "phase1_action"], field_contract_rows())
    write_tsv(output_dir / "all_standardized_values_classified.tsv", value_header, field_count_rows(value_rows))
    for queue, filename in INCOMPATIBILITY_FILES.items():
        write_tsv(output_dir / filename, queue_header, flags[queue])
    write_tsv(output_dir / "high_impact_examples.tsv", example_header, all_examples)
    write_tsv(output_dir / "recommended_migration_decisions.tsv", migration_header, migration_rows)
    write_tsv(output_dir / "all_additive_axis_enrichment_candidates.tsv", value_header, candidate_rows_by_action(value_rows, "additive_axis_enrichment"))
    write_tsv(output_dir / "all_compatible_composite_candidates.tsv", value_header, compatible_composite_rows(value_rows))
    write_tsv(output_dir / "confirmed_high_confidence_fixes.tsv", value_header, candidate_rows_by_action(value_rows, "confirmed_high_confidence_fix"))
    write_tsv(output_dir / "classifier_uncertain_values.tsv", value_header, candidate_rows_by_action(value_rows, "classifier_uncertain"))
    write_tsv(output_dir / "legacy_compatibility_values.tsv", value_header, candidate_rows_by_action(value_rows, "legacy_compatibility_label"))
    write_tsv(output_dir / "proposed_new_fields.tsv", ["field", "field_status", "purpose", "examples", "release_policy"], proposed_new_fields())
    write_tsv(output_dir / "proposed_regression_tests.tsv", ["raw_value", "expected_behavior", "phase"], proposed_regression_tests())

    summary = {
        "generated_at": utc_now(),
        "canonical_snapshot_id": snapshot_id,
        "snapshot_id": snapshot_id,
        "input_standardization_commit": os.environ.get("FETCHM_INPUT_STANDARDIZATION_COMMIT") or "28ff0c440ebc0a7351140a69c01427355b7b4fe9",
        "audit_code_commit": git_commit(),
        "artifact_commit": os.environ.get("FETCHM_AUDIT_ARTIFACT_COMMIT") or "not_committed_at_generation_time",
        "previous_reviewed_artifact_commit": "039012f2d8c95abaffdb0892faf8710d9e0e1832",
        "database_schema_version": "dataset_production_store_v1",
        "command_line": command_line,
        "controlled_categories_sha256": sha256_file(CONTROLLED_CATEGORIES),
        "approved_broad_categories_sha256": sha256_file(APPROVED_BROAD_CATEGORIES),
        "phase": "audit_only_v2_compositional",
        "production_rules_changed": False,
        "canonical_refresh_run": False,
        "global_insights_regenerated": False,
        "deployment_run": False,
        "rows_audited": len(records),
        "distinct_values_classified": len(value_rows),
        "field_value_pairs_with_queue_signals": len(unique_flagged),
        "field_value_pairs_with_any_semantic_signal": len({(row["field"], row["standardized_value"]) for row in all_signal_rows}),
        "incompatibility_queue_counts": queue_counts,
        "semantic_classes": SEMANTIC_CLASSES,
        "audit_note": "Counts are semantic review/decomposition signals, not confirmed erroneous records.",
        **unique_counts,
    }
    (output_dir / "audit_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_summary_md(output_dir / "audit_summary.md", summary)

    manifest = {
        "generated_at": utc_now(),
        "audit_code_commit": git_commit(),
        "previous_reviewed_artifact_commit": "039012f2d8c95abaffdb0892faf8710d9e0e1832",
        "artifact_commit": os.environ.get("FETCHM_AUDIT_ARTIFACT_COMMIT") or "not_committed_at_generation_time",
        "artifact_sha256": {
            path.name: sha256_file(path)
            for path in sorted(output_dir.iterdir())
            if path.is_file() and path.name != "artifact_manifest.json"
        },
    }
    (output_dir / "artifact_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    release_manifest = {
        "generated_at": utc_now(),
        "audit_code_commit": git_commit(),
        "previous_reviewed_artifact_commit": "039012f2d8c95abaffdb0892faf8710d9e0e1832",
        "artifact_commit": os.environ.get("FETCHM_AUDIT_ARTIFACT_COMMIT") or "not_committed_at_generation_time",
        "note": "The current artifact commit is recorded after commit by setting FETCHM_AUDIT_ARTIFACT_COMMIT during release-manifest regeneration; no production data changes are implied.",
    }
    (output_dir / "artifact_release_manifest.json").write_text(json.dumps(release_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
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
    output_dir = args.output_dir or DEFAULT_OUTPUT_ROOT / f"{date}_v2"
    summary = generate_audit(args.snapshot_id, output_dir, args.example_limit, " ".join(sys.argv))
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
