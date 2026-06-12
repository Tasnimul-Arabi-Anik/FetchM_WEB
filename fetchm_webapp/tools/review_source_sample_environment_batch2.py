#!/usr/bin/env python3
"""Classify Batch 2 sample-material review signals conservatively."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

FIELDS = [
    "raw_value", "affected_rows", "current_signal_type", "current_standardized_value",
    "current_broad_value", "current_recommended_action", "reviewer_decision",
    "proposed_sample_type_sd", "proposed_isolation_site_sd", "proposed_isolation_source_sd",
    "proposed_isolation_source_broad", "confidence", "rationale", "rule_file_to_update",
    "rule_to_add", "test_to_add", "notes",
]
SITE_PATTERN = re.compile(
    r"\b(?:wound|rectal|rectum|perianal|nasal|nose|throat|oropharyngeal|cloacal|vaginal|"
    r"tracheal|bronchoalveolar|gastric|bladder|gall bladder|gallbladder|pericardial|"
    r"lung|bronch|pleural|oral|tonsil|ear|eye|skin)\b",
    re.I,
)
FOOD_PATTERN = re.compile(r"\b(?:food|meat|poultry|chicken|turkey|beef|pork|seafood|dairy|fermented milk|cheese)\b", re.I)
ENV_PATTERN = re.compile(r"\b(?:environmental|soil|water|sediment|wastewater|sewage|biofilm|air|dust)\b", re.I)
PLANT_PATTERN = re.compile(r"\b(?:plant|leaf|root|stem|flower|fruit|seed|rhizosphere)\b", re.I)
HIGH_CONFIDENCE_MATERIAL = re.compile(
    r"\b(?:feces|faeces|fecal|faecal|stool|blood|serum|plasma|urine|sputum|saliva|milk|"
    r"tissue|swab|pus|aspirate|lavage|biopsy|cerebrospinal fluid|csf|body fluid|bile|mucus|"
    r"placenta|semen)\b",
    re.I,
)


def classify(value: str, signal: str, broad: str) -> tuple[str, str, str]:
    searchable = f"{value} {broad}"
    if FOOD_PATTERN.search(searchable):
        return "preserve_food_context", "high", "Material occurs in explicit food, meat, dairy, or commodity context."
    if ENV_PATTERN.search(searchable):
        return "preserve_environment_context", "medium", "Environmental material or swab context must remain available for Batch 4 routing."
    if PLANT_PATTERN.search(searchable):
        return "plant_material_context", "high", "Plant material should route to plant tissue while preserving plant-associated context."
    if "body_site" in signal or SITE_PATTERN.search(value):
        return "split_sample_type_and_site", "high", "Phrase contains both a specimen material and an anatomical collection site."
    if HIGH_CONFIDENCE_MATERIAL.search(value):
        return "route_to_sample_type", "high", "Reviewed biological or clinical specimen material."
    return "unresolved_low_priority", "low", "Sample-like aggregate is insufficiently specific for a new automatic rule."


def proposed_values(value: str, decision: str) -> tuple[str, str, str, str]:
    cleaned = value.casefold()
    sample = value
    site = ""
    source = ""
    broad = ""
    if re.search(r"feces|faeces|fecal|faecal|stool", cleaned): sample = "feces/stool"
    elif re.search(r"serum|plasma", cleaned): sample = "blood-derived material"
    elif re.search(r"\bblood\b", cleaned): sample = "blood"
    elif re.search(r"\burine\b", cleaned): sample = "urine"
    elif re.search(r"\bsputum\b", cleaned): sample = "sputum"
    elif re.search(r"\bsaliva\b", cleaned): sample = "saliva"
    elif re.search(r"\bmilk\b", cleaned): sample = "milk"
    elif re.search(r"\btissue\b", cleaned): sample = "plant tissue" if PLANT_PATTERN.search(value) else "tissue"
    elif re.search(r"\bswab\b", cleaned): sample = value
    elif re.search(r"broncho.?alveolar.*lavage", cleaned): sample = "bronchoalveolar lavage fluid"
    elif re.search(r"\btracheal aspirate\b", cleaned): sample = "tracheal aspirate/secretion"
    elif re.search(r"\bbiopsy\b", cleaned): sample = "biopsy"
    elif re.search(r"\blavage\b", cleaned): sample = "lavage"
    elif re.search(r"\baspirate\b", cleaned): sample = "aspirate"
    elif re.search(r"cerebrospinal fluid|\bcsf\b|body fluid", cleaned): sample = "bodily fluid"

    if decision == "split_sample_type_and_site":
        if re.search(r"rectal|rectum|perianal", cleaned): site = "rectum/perianal region"
        elif re.search(r"nasal|nose", cleaned): site = "nasal cavity/sinus/upper respiratory tract"
        elif re.search(r"throat|oropharyngeal", cleaned): site = "nasopharynx/oropharynx"
        elif re.search(r"cloacal", cleaned): site = "cloaca"
        elif re.search(r"tracheal|bronchoalveolar|bronch|lung|pleural", cleaned): site = "lower respiratory tract/bronch/pleural cavity"
        elif re.search(r"gastric", cleaned): site = "gastrointestinal tract"
        elif re.search(r"gall bladder|gallbladder|pericardial", cleaned): site = "organ/tissue site"
        elif re.search(r"\bbladder\b", cleaned): site = "urogenital tract"
        elif re.search(r"wound", cleaned): site = "wound"
    if decision == "preserve_food_context":
        source, broad = value, "food"
    elif decision == "preserve_environment_context":
        source, broad = "environmental material", "environmental material"
    elif decision == "plant_material_context":
        source, broad = "plant-associated material", "plant-associated material"
    elif decision in {"route_to_sample_type", "split_sample_type_and_site"}:
        source, broad = "clinical/host-associated material", "clinical/host-associated material"
        if cleaned == "milk": source, broad = "", ""
    return sample, site, source, broad


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.candidates.open(newline="", encoding="utf-8") as handle:
        candidates = [row for row in csv.DictReader(handle) if row.get("recommended_action") == "route_to_sample_type" or row.get("current_recommended_action") == "route_to_sample_type"]
    reviewed = []
    for row in candidates:
        value = str(row.get("isolation_source_sd") or row.get("value") or row.get("raw_value") or row.get("current_standardized_value") or "").strip()
        count = int(row.get("assembly_count") or row.get("count") or row.get("affected_rows") or 0)
        signal = str(row.get("signal_classes") or row.get("signal_class") or row.get("review_signal_type") or row.get("current_signal_type") or "")
        broad = str(row.get("current_broad_value") or "")
        decision, confidence, rationale = classify(value, signal, broad)
        sample, site, source, source_broad = proposed_values(value, decision)
        reviewed.append({
            "raw_value": value, "affected_rows": count, "current_signal_type": signal,
            "current_standardized_value": value, "current_broad_value": broad,
            "current_recommended_action": "route_to_sample_type", "reviewer_decision": decision,
            "proposed_sample_type_sd": sample, "proposed_isolation_site_sd": site,
            "proposed_isolation_source_sd": source, "proposed_isolation_source_broad": source_broad,
            "confidence": confidence, "rationale": rationale,
            "rule_file_to_update": "app.py" if confidence == "high" else "",
            "rule_to_add": "material fallback after descriptor sanitization" if confidence == "high" else "",
            "test_to_add": "Batch 2 material/site routing regression" if confidence == "high" else "",
            "notes": "Review signals are triage; retained context is intentional.",
        })
    reviewed.sort(key=lambda row: (-int(row["affected_rows"]), row["raw_value"].casefold()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader(); writer.writerows(reviewed)
    print(f"reviewed={len(reviewed)} affected_rows={sum(int(row['affected_rows']) for row in reviewed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
