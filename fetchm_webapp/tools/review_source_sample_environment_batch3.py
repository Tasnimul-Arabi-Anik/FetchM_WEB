#!/usr/bin/env python3
"""Classify Batch 3 body-site and ambiguity review signals conservatively."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

FIELDS = [
    "raw_value", "affected_rows", "current_signal_type", "current_standardized_value",
    "current_broad_value", "current_recommended_action", "contributing_raw_examples_if_available",
    "reviewer_decision", "proposed_sample_type_sd", "proposed_isolation_site_sd",
    "proposed_environment_medium_sd", "proposed_environment_local_scale_sd",
    "proposed_isolation_source_sd", "proposed_isolation_source_broad", "confidence",
    "rationale", "rule_file_to_update", "rule_to_add", "test_to_add", "notes",
]

ANATOMICAL_CANALS = {
    "ear canal": "organ/tissue site",
    "birth canal": "urogenital tract",
    "anal canal": "rectum/perianal region",
    "root canal": "oral cavity",
    "bile canal": "organ/tissue site",
    "biliary canal": "organ/tissue site",
}
ENVIRONMENT_PATTERN = re.compile(r"\b(?:canal water|canal sediment|irrigation canal|drainage water)\b", re.I)
AMBIGUOUS_EXACT = {"canal", "drainage", "surface"}
FOOD_HOST_PATTERN = re.compile(
    r"\b(?:pork|beef|chicken|turkey|poultry|fish|oyster|shellfish|swine|cattle|animal)\b",
    re.I,
)
SITE_PATTERN = re.compile(
    r"\b(?:groin|skin|wound|surgical site|bite wound|rectal|rectum|perianal|anus|throat|"
    r"oropharyn|nasopharyn|nasal|nose|lung|bronch|trachea|bladder|urinary|urogenital|"
    r"vagina|cervix|uterus|prostate|ear|eye|conjunctiva|liver|kidney|spleen|brain|bone|"
    r"heart|pericard|gall bladder|biliary|cloaca)\b",
    re.I,
)
MATERIAL_PATTERN = re.compile(r"\b(?:swab|aspirate|lavage|biopsy|tissue|drainage|fluid|material)\b", re.I)


def classify(value: str) -> tuple[str, str, str, str, str, str]:
    cleaned = value.casefold().strip()
    if cleaned in ANATOMICAL_CANALS:
        return "route_to_isolation_site", "", ANATOMICAL_CANALS[cleaned], "", "", "high"
    if ENVIRONMENT_PATTERN.search(cleaned):
        medium = "sediment" if "sediment" in cleaned else "water"
        local = "irrigation canal" if "irrigation canal" in cleaned else ("canal" if "canal" in cleaned else "")
        return "preserve_environment_context", "", "", medium, local, "high"
    if cleaned in AMBIGUOUS_EXACT:
        return "manual_review", "", "", "", "", "high"
    if FOOD_HOST_PATTERN.search(cleaned):
        return "preserve_food_or_host_context", "", "", "", "", "high"
    if SITE_PATTERN.search(cleaned):
        sample = ""
        if MATERIAL_PATTERN.search(cleaned):
            if "swab" in cleaned: sample = "swab"
            elif "aspirate" in cleaned: sample = "aspirate"
            elif "lavage" in cleaned: sample = "lavage"
            elif "biopsy" in cleaned: sample = "biopsy"
            elif "tissue" in cleaned: sample = "tissue"
            elif "drainage" in cleaned: sample = "drainage"
        return ("split_sample_type_and_site" if sample else "route_to_isolation_site", sample, "reviewed anatomical site", "", "", "high")
    return "unresolved_low_priority", "", "", "", "", "low"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.candidates.open(newline="", encoding="utf-8") as handle:
        source_rows = list(csv.DictReader(handle))
    reviewed = []
    for row in source_rows:
        value = str(row.get("isolation_source_sd") or row.get("raw_value") or "").strip()
        signal = str(row.get("signal_classes") or row.get("current_signal_type") or "")
        action = str(row.get("recommended_action") or row.get("current_recommended_action") or "")
        if not ("body_site" in signal or action == "route_to_isolation_site" or value.casefold() in AMBIGUOUS_EXACT or "canal" in value.casefold() or "drainage" in value.casefold() or "surface" in value.casefold()):
            continue
        count = int(row.get("assembly_count") or row.get("affected_rows") or 0)
        decision, sample, site, medium, local, confidence = classify(value)
        if site == "reviewed anatomical site":
            site = value
        rationale = {
            "route_to_isolation_site": "Clear anatomical site; do not retain the exact term as biological source.",
            "split_sample_type_and_site": "Phrase contains both specimen material and anatomical site.",
            "preserve_environment_context": "Environmental canal or drainage phrase must not become anatomy.",
            "preserve_food_or_host_context": "Animal commodity or host anatomy must not become a human clinical site.",
            "manual_review": "Context-free term has both anatomical and environmental interpretations.",
            "unresolved_low_priority": "Insufficient evidence for a deterministic routing rule.",
        }[decision]
        reviewed.append({
            "raw_value": value, "affected_rows": count, "current_signal_type": signal,
            "current_standardized_value": value, "current_broad_value": "",
            "current_recommended_action": action, "contributing_raw_examples_if_available": "",
            "reviewer_decision": decision, "proposed_sample_type_sd": sample,
            "proposed_isolation_site_sd": site, "proposed_environment_medium_sd": medium,
            "proposed_environment_local_scale_sd": local, "proposed_isolation_source_sd": "",
            "proposed_isolation_source_broad": "", "confidence": confidence, "rationale": rationale,
            "rule_file_to_update": "app.py" if confidence == "high" else "",
            "rule_to_add": "context-aware Batch 3 routing" if confidence == "high" else "",
            "test_to_add": "Batch 3 ambiguity and precedence regression" if confidence == "high" else "",
            "notes": "Review signals are triage, not hard errors.",
        })
    reviewed.sort(key=lambda item: (-int(item["affected_rows"]), item["raw_value"].casefold()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader(); writer.writerows(reviewed)
    print(f"reviewed={len(reviewed)} affected_rows={sum(int(row['affected_rows']) for row in reviewed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
