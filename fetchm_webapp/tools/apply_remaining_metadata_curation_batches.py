"""Apply conservative remaining metadata curation batches.

This script addresses the remaining post-production-readiness batches without
rewriting the standardization pipeline:

- Batch 3: source-like values already mapped to Host_SD, spot-check/demote only
  explicit lab/source artifacts.
- Batch 4: source-like Host values that are already non-host, route them to
  sample/source/environment fields where the routing is deterministic.
- Batch 5: apply high-confidence source/sample/environment suggestions with
  safety filters.
- Batch 6: one final low-frequency host pass using exact TaxonKit matches and a
  small reviewed common-name list.

Ambiguous values are written to review files, not auto-approved.
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
QUALITY_AUDIT = ROOT / "standardization" / "review" / "quality_audit" / "20260505_034628"
SOURCE_AUDIT = ROOT / "standardization" / "review" / "source_sample_environment_audit" / "20260505_035024"
OUTPUT_DIR = ROOT / "standardization" / "review" / "remaining_batches" / "20260505"
CONTROLLED_CATEGORIES = ROOT / "standardization" / "controlled_categories.csv"
HOST_SYNONYMS = ROOT / "standardization" / "host_synonyms.csv"
HOST_NEGATIVE_RULES = ROOT / "standardization" / "host_negative_rules.csv"

CONTROLLED_FIELDS = [
    "synonym",
    "source_column",
    "original_value",
    "normalized_value",
    "destination",
    "category",
    "proposed_value",
    "broad_value",
    "ontology_id",
    "method",
    "confidence",
    "status",
    "note",
]

HOST_SYNONYM_FIELDS = ["synonym", "canonical", "taxid", "confidence", "note"]
HOST_NEGATIVE_FIELDS = ["synonym", "decision", "note"]

MICROBIAL_SUPERKINGDOMS = {"Bacteria", "Archaea", "Viruses"}

MATERIAL_WORDS = {
    "blood",
    "feces",
    "faeces",
    "fecal",
    "faecal",
    "stool",
    "urine",
    "sputum",
    "swab",
    "saliva",
    "tissue",
    "biopsy",
    "lavage",
    "fluid",
    "milk",
    "meat",
    "manure",
    "gut",
    "intestine",
    "intestinal",
    "skin",
}

HOST_ONLY_SAMPLE_BLOCKLIST = {
    "human",
    "patient",
    "people",
    "animal",
    "mammal",
    "bird",
    "poultry",
    "cattle",
    "cow",
    "pig",
    "swine",
    "chicken",
    "fish",
    "plant",
    "bacteria",
    "organism",
    "host",
}

FINAL_HOST_COMMON: dict[str, tuple[str, str, str, str]] = {
    "sow": ("Sus scrofa", "9823", "species", "reviewed common female pig host"),
    "sugar cane": ("Saccharum officinarum", "4547", "species", "reviewed crop host/common name"),
    "hazelnut": ("Corylus", "13451", "genus", "reviewed broad hazelnut plant host"),
    "protozoa": ("Eukaryota", "2759", "superkingdom", "reviewed broad protozoan/eukaryote host"),
    "dinoflagellate": ("Dinophyceae", "2864", "class", "reviewed broad dinoflagellate host"),
    "anguine": ("Squamata", "8509", "order", "reviewed broad snake/lizard adjective; kept broad"),
    "otariinae": ("Otariinae", "9709", "subfamily", "reviewed sea lion/fur seal subfamily"),
    "phalangeriformes": ("Phalangeriformes", "38603", "order", "reviewed possum group"),
    "possum": ("Phalangeriformes", "38603", "order", "reviewed broad possum group; species ambiguous"),
    "possums": ("Phalangeriformes", "38603", "order", "reviewed broad possum group; species ambiguous"),
    "barbera": ("Vitis vinifera", "29760", "species", "reviewed grape cultivar host/source"),
}


def normalize(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def compact(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize(value))


def title_count(counter: Counter[str]) -> str:
    return ", ".join(f"{key}={value:,}" for key, value in sorted(counter.items()))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def append_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str], dedupe_key) -> int:
    if not rows:
        return 0
    existing = set()
    if path.exists():
        for row in read_csv(path):
            existing.add(dedupe_key(row))
    new_rows = []
    seen = set(existing)
    for row in rows:
        key = dedupe_key(row)
        if key in seen:
            continue
        seen.add(key)
        new_rows.append(row)
    if not new_rows:
        return 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writerows(new_rows)
    return len(new_rows)


def controlled_index(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    index: dict[tuple[str, str], dict[str, str]] = {}
    if not path.exists():
        return index
    for row in read_csv(path):
        if normalize(row.get("status") or "approved") not in {"approved", "active"}:
            continue
        key = (normalize(row.get("synonym") or row.get("original_value") or row.get("normalized_value")), row.get("destination", "").strip())
        if key not in index:
            index[key] = row
    return index


def controlled_row(
    *,
    synonym: str,
    destination: str,
    value: str,
    source_column: str,
    broad_value: str,
    method: str,
    confidence: str = "high",
    note: str,
    ontology_id: str = "",
) -> dict[str, str]:
    normalized = normalize(synonym)
    return {
        "synonym": synonym,
        "source_column": source_column,
        "original_value": synonym,
        "normalized_value": normalized,
        "destination": destination,
        "category": value,
        "proposed_value": value,
        "broad_value": broad_value,
        "ontology_id": ontology_id,
        "method": method,
        "confidence": confidence,
        "status": "approved",
        "note": note,
    }


def add_controlled_if_safe(
    rows: list[dict[str, str]],
    skipped: list[dict[str, str]],
    existing: dict[tuple[str, str], dict[str, str]],
    row: dict[str, str],
) -> None:
    key = (normalize(row["synonym"]), row["destination"])
    current = existing.get(key)
    proposed = normalize(row.get("proposed_value") or row.get("category"))
    if current:
        current_proposed = normalize(current.get("proposed_value") or current.get("category"))
        if current_proposed != proposed:
            skipped.append(
                {
                    "synonym": row["synonym"],
                    "destination": row["destination"],
                    "proposed_value": row["proposed_value"],
                    "skipped_reason": f"existing approved rule maps to {current.get('proposed_value') or current.get('category')}",
                }
            )
        return
    existing[key] = row
    rows.append(row)


def classify_source_route(value: str) -> tuple[str, str, str, str] | None:
    key = normalize(value)
    if not key:
        return None
    if any(token in key for token in ("university", "institute", "laboratory", "professor", "collection center", "database")):
        return None
    if any(token in key for token in ("not applicable", "not reported", "unknown", "missing", "no applicable", "nil")):
        return None
    if key == "water hyacinth":
        return ("Isolation_Source_SD", "plant-associated material", "plant-associated material", "aquatic plant/source material")
    if "dairy plant" in key or "processing plant" in key or "feed mill" in key:
        return ("Isolation_Source_SD", "food-processing environment", "food-processing environment", "food/processing environment")
    if "wastewater" in key or "waste water" in key:
        return ("Environment_Medium_SD", "wastewater", "environmental material", "wastewater source")
    if key == "sewage" or " sewage" in f" {key} ":
        return ("Environment_Medium_SD", "sewage", "environmental material", "sewage source")
    if "sludge" in key:
        return ("Environment_Medium_SD", "sludge", "environmental material", "sludge source")
    if "sediment" in key:
        return ("Environment_Medium_SD", "sediment", "environmental material", "sediment source")
    if "soil" in key or "rhizosphere" in key:
        return ("Environment_Medium_SD", "soil", "environmental material", "soil source")
    if "seawater" in key or "sea water" in key or "marine water" in key:
        return ("Environment_Medium_SD", "seawater", "environmental material", "marine water source")
    if "freshwater" in key or "fresh water" in key:
        return ("Environment_Medium_SD", "freshwater", "environmental material", "freshwater source")
    if "lake water" in key:
        return ("Environment_Medium_SD", "lake water", "environmental material", "lake water source")
    if "storm water" in key or "stormwater" in key:
        return ("Environment_Medium_SD", "stormwater", "environmental material", "stormwater source")
    if key in {"water", "environmental waters", "environmental water", "raw water"} or " water " in f" {key} ":
        return ("Environment_Medium_SD", "water", "environmental material", "water source")
    if key in {"air", "dust"} or " dust" in key:
        return ("Environment_Medium_SD", "dust" if "dust" in key else "air", "environmental material", "air/dust source")
    if "biofilm" in key:
        return ("Environment_Medium_SD", "biofilm", "biofilm", "biofilm source")
    if "hospital" in key or "clinic" in key or key in {"icu environment", "ward", "healthcare facility"}:
        return ("Isolation_Source_SD", "healthcare facility", "healthcare-associated environment", "healthcare environment")
    if any(token in key for token in ("stool", "feces", "faeces", "fecal", "faecal")):
        return ("Sample_Type_SD", "feces/stool", "feces/stool", "feces/stool material")
    if "urine" in key:
        return ("Sample_Type_SD", "urine", "clinical/host-associated material", "urine specimen")
    if "blood" in key and "agar" not in key:
        return ("Sample_Type_SD", "blood", "clinical/host-associated material", "blood specimen")
    if "sputum" in key:
        return ("Sample_Type_SD", "sputum", "respiratory sample", "sputum specimen")
    if "saliva" in key:
        return ("Sample_Type_SD", "saliva", "oral/upper respiratory site", "saliva specimen")
    if "swab" in key:
        if "environment" in key or "surface" in key:
            return ("Sample_Type_SD", "environmental swab", "swab", "environmental swab")
        return ("Sample_Type_SD", "swab", "swab", "swab specimen")
    if "milk" in key or "dairy" in key:
        return ("Sample_Type_SD", "milk/dairy product", "food/dairy", "milk/dairy source")
    if "meat" in key or "sausage" in key or "bacon" in key or "poultry meat" in key:
        return ("Sample_Type_SD", "meat product", "food/meat", "meat/food source")
    if "seafood" in key or "fish product" in key or "shrimp product" in key or "oyster product" in key:
        return ("Sample_Type_SD", "seafood", "food/seafood", "seafood source")
    if "food" in key or "salad" in key or "fermented" in key:
        return ("Isolation_Source_SD", "food/food product", "food", "food source")
    if "culture" in key or "agar" in key or "cell line" in key:
        if "cell line" in key:
            return ("Sample_Type_SD", "cell culture/cell line", "culture/laboratory", "cell line/culture")
        return ("Sample_Type_SD", "bacterial culture", "culture/laboratory", "culture/laboratory")
    if "environment" in key or "environmental" in key:
        return ("Isolation_Source_SD", "environmental material", "environmental material", "environment/source context")
    return None


def batch3_mapped_host_spotcheck(existing_controlled: dict[tuple[str, str], dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    input_path = QUALITY_AUDIT / "suspicious_source_like_mapped_hosts.csv"
    rows = read_csv(input_path)
    reviewed: list[dict[str, str]] = []
    negatives: list[dict[str, str]] = []
    controlled: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []

    def negative(raw: str, decision_class: str, reason: str) -> None:
        negatives.append({"synonym": raw, "decision": "non_host_source", "note": f"Batch 3 2026-05-05: {reason}"})
        route = classify_source_route(raw)
        if route:
            destination, value, broad, route_reason = route
            add_controlled_if_safe(
                controlled,
                skipped,
                existing_controlled,
                controlled_row(
                    synonym=raw,
                    destination=destination,
                    value=value,
                    source_column="Host",
                    broad_value=broad,
                    method="batch3_source_like_host_review",
                    confidence="high",
                    note=f"Batch 3 route after Host demotion: {route_reason}.",
                ),
            )
        return decision_class

    for row in rows:
        raw = row.get("host_original", "")
        key = normalize(raw)
        decision = "keep_host_add_sample_type"
        reason = "Source-like text contains an explicit host plus specimen/source material; keep host mapping and rely on sample/source routing."

        if key in {"human animal or environmental", "patient environment", "water for animals", "sewage plant", "environmental swabs sponge"}:
            decision = negative(raw, "remove_host_route_to_source", "reviewed mixed/source descriptor; not a reliable Host_SD value")
            reason = "Reviewed mixed/source descriptor; demoted from Host_SD and routed where deterministic."
        elif re.search(r"\b(bacteria|bacterial|microbial)\b.*\bculture\b", key) or re.search(r"\bculture\b.*\b(bacteria|bacterial)\b", key):
            decision = negative(raw, "lab_artifact_not_host", "bacterial culture/lab artifact, not host organism")
            reason = "Bacterial culture/lab artifact; blocked from Host_SD."
        elif "pure culture of bacteria" in key or "bacterial extracellular vesicles" in key:
            decision = negative(raw, "lab_artifact_not_host", "bacterial culture/lab artifact, not host organism")
            reason = "Bacterial culture/lab artifact; blocked from Host_SD."
        elif "environment" in key and not any(token in key for token in MATERIAL_WORDS):
            decision = negative(raw, "remove_host_route_to_source", "environment/context phrase without explicit specimen material")
            reason = "Environment/context phrase without explicit specimen material; blocked from Host_SD."
        elif key in {"water deer", "water buffalo"}:
            decision = "keep_host_add_sample_type"
            reason = "Animal common name containing water; keep as host and do not route as water."

        reviewed.append(
            {
                "count": row.get("count", "0"),
                "host_original": raw,
                "current_host_sd": row.get("host_sd", ""),
                "current_taxid": row.get("taxid", ""),
                "current_method": row.get("host_method", ""),
                "decision": decision,
                "reason": reason,
            }
        )

    write_csv(
        OUTPUT_DIR / "suspicious_source_like_mapped_hosts_reviewed.csv",
        reviewed,
        ["count", "host_original", "current_host_sd", "current_taxid", "current_method", "decision", "reason"],
    )
    write_csv(OUTPUT_DIR / "batch3_controlled_rules_proposed.csv", controlled, CONTROLLED_FIELDS)
    write_csv(OUTPUT_DIR / "batch3_controlled_rules_skipped.csv", skipped, ["synonym", "destination", "proposed_value", "skipped_reason"])
    return negatives, controlled, skipped


def batch4_unmapped_source_routing(existing_controlled: dict[tuple[str, str], dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    input_path = QUALITY_AUDIT / "source_like_unmapped_hosts_for_review.csv"
    rows = read_csv(input_path)
    reviewed: list[dict[str, str]] = []
    controlled: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []

    for row in rows:
        raw = row.get("host_original", "")
        route = classify_source_route(raw)
        decision = "retained_manual_review"
        destination = ""
        proposed = ""
        reason = "No deterministic source/sample/environment route matched."
        if route:
            destination, proposed, broad, route_reason = route
            decision = "route_non_host_source"
            reason = route_reason
            add_controlled_if_safe(
                controlled,
                skipped,
                existing_controlled,
                controlled_row(
                    synonym=raw,
                    destination=destination,
                    value=proposed,
                    source_column="Host",
                    broad_value=broad,
                    method="batch4_source_like_unmapped_routing",
                    confidence="high",
                    note=f"Batch 4 route from non-host Host value: {route_reason}.",
                ),
            )
        reviewed.append(
            {
                "count": row.get("count", "0"),
                "host_original": raw,
                "host_method": row.get("host_method", ""),
                "decision": decision,
                "destination": destination,
                "proposed_value": proposed,
                "reason": reason,
            }
        )

    write_csv(
        OUTPUT_DIR / "source_like_unmapped_hosts_routing_reviewed.csv",
        reviewed,
        ["count", "host_original", "host_method", "decision", "destination", "proposed_value", "reason"],
    )
    write_csv(OUTPUT_DIR / "batch4_controlled_rules_proposed.csv", controlled, CONTROLLED_FIELDS)
    write_csv(OUTPUT_DIR / "batch4_controlled_rules_skipped.csv", skipped, ["synonym", "destination", "proposed_value", "skipped_reason"])
    return controlled, skipped, reviewed


def suggestion_is_safe(row: dict[str, str]) -> tuple[bool, str]:
    destination = row.get("destination", "").strip()
    proposed = normalize(row.get("proposed_value") or row.get("category"))
    source_column = row.get("source_column", "").strip()
    raw = normalize(row.get("original_value") or row.get("synonym") or row.get("normalized_value"))
    confidence = normalize(row.get("confidence"))

    if confidence != "high":
        return False, "confidence is not high"
    allowed_destinations = {
        "Sample_Type_SD",
        "Isolation_Source_SD",
        "Isolation_Site_SD",
        "Environment_Medium_SD",
        "Environment_Broad_Scale_SD",
        "Environment_Local_Scale_SD",
        "Host_Disease_SD",
        "Host_Health_State_SD",
    }
    if destination not in allowed_destinations:
        return False, "destination not in allowed source/sample/environment set"
    if destination == "Sample_Type_SD" and proposed in HOST_ONLY_SAMPLE_BLOCKLIST:
        return False, "would create host-only Sample_Type_SD"
    if destination == "Sample_Type_SD" and proposed in {"metadata descriptor non source", "metadata descriptor", "surveillance sample"}:
        return False, "metadata/admin descriptor should not become Sample_Type_SD"
    if destination == "Environment_Medium_SD" and proposed in {"feces stool", "feces", "stool"}:
        if not any(token in raw for token in ("manure", "slurry", "environmental", "farm waste")):
            return False, "feces/stool should not become Environment_Medium_SD without environmental context"
    if source_column == "Host Disease" and destination == "Sample_Type_SD":
        specimen_terms = {"rectal swab", "blood", "urine", "sputum", "saliva", "stool", "feces", "faeces", "swab"}
        if not any(term in raw for term in specimen_terms):
            return False, "Host Disease value is not a clear specimen material"
    if destination == "Sample_Type_SD" and proposed in {"blood"} and "infection" in raw:
        return False, "disease phrase should not infer blood specimen"
    return True, "safe"


def batch5_high_confidence_suggestions(existing_controlled: dict[tuple[str, str], dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    input_path = SOURCE_AUDIT / "suggested_high_confidence_rules.csv"
    rows = read_csv(input_path)
    applied: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []
    reviewed: list[dict[str, str]] = []
    for row in rows:
        safe, reason = suggestion_is_safe(row)
        raw = row.get("synonym") or row.get("original_value") or row.get("normalized_value") or ""
        destination = row.get("destination", "").strip()
        proposed = row.get("proposed_value") or row.get("category") or ""
        decision = "apply" if safe else "skip"
        reviewed.append(
            {
                "source_column": row.get("source_column", ""),
                "original_value": raw,
                "destination": destination,
                "proposed_value": proposed,
                "confidence": row.get("confidence", ""),
                "decision": decision,
                "reason": reason,
            }
        )
        if not safe:
            skipped.append(
                {
                    "synonym": raw,
                    "destination": destination,
                    "proposed_value": proposed,
                    "skipped_reason": reason,
                }
            )
            continue
        add_controlled_if_safe(
            applied,
            skipped,
            existing_controlled,
            controlled_row(
                synonym=raw,
                destination=destination,
                value=proposed,
                source_column=row.get("source_column", ""),
                broad_value=row.get("broad_value") or row.get("category") or proposed,
                method="batch5_high_confidence_audit_suggestion",
                confidence="high",
                note=f"Batch 5 applied high-confidence source/sample/environment audit suggestion: {row.get('note', '').strip()}",
                ontology_id=row.get("ontology_id", ""),
            ),
        )

    write_csv(
        OUTPUT_DIR / "applied_high_confidence_rules_20260505_review.csv",
        reviewed,
        ["source_column", "original_value", "destination", "proposed_value", "confidence", "decision", "reason"],
    )
    write_csv(OUTPUT_DIR / "batch5_controlled_rules_proposed.csv", applied, CONTROLLED_FIELDS)
    write_csv(OUTPUT_DIR / "batch5_controlled_rules_skipped.csv", skipped, ["synonym", "destination", "proposed_value", "skipped_reason"])
    return applied, skipped, reviewed


def variant_names(raw: str) -> list[str]:
    cleaned = raw.strip()
    cleaned = re.sub(r"\([^)]*\)", "", cleaned)
    cleaned = re.sub(r"\[[^\]]*\]", "", cleaned)
    cleaned = re.sub(
        r"\b(strain|isolate|clone|sample|specimen|larvae|larva|colony|lineage|cv|cultivar)\b.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip(" ,;:.")
    variants: list[str] = []
    for value in (raw, cleaned):
        value = value.strip(" ,;:.")
        if value and value not in variants:
            variants.append(value)
        words = re.findall(r"[A-Za-z][A-Za-z.-]*", value)
        if len(words) >= 2:
            binomial = " ".join(words[:2]).strip(" ,;:.")
            if binomial not in variants:
                variants.append(binomial)
    return variants


def taxonkit_name_lookup(names: list[str]) -> dict[str, dict[str, str]]:
    unique_names = sorted({name for name in names if name})
    if not unique_names:
        return {}
    try:
        result = subprocess.run(
            ["taxonkit", "name2taxid", "--show-rank"],
            input="\n".join(unique_names) + "\n",
            text=True,
            capture_output=True,
            check=False,
            timeout=180,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}
    raw: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) >= 3 and parts[1].strip():
            raw[normalize(parts[0])].append({"name": parts[0].strip(), "taxid": parts[1].strip(), "rank": parts[2].strip()})
    matches: dict[str, dict[str, str]] = {}
    for key, values in raw.items():
        if len({value["taxid"] for value in values}) == 1:
            matches[key] = values[0]
    return matches


def taxonkit_lineage(taxids: list[str]) -> dict[str, dict[str, str]]:
    unique_taxids = sorted({taxid for taxid in taxids if str(taxid).isdigit()})
    if not unique_taxids:
        return {}
    try:
        result = subprocess.run(
            ["taxonkit", "lineage", "-r"],
            input="\n".join(unique_taxids) + "\n",
            text=True,
            capture_output=True,
            check=False,
            timeout=180,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {}
    out: dict[str, dict[str, str]] = {}
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        lineage_text = parts[1]
        superkingdom = ""
        for candidate in ("Eukaryota", "Bacteria", "Archaea", "Viruses"):
            if candidate in lineage_text.split(";"):
                superkingdom = candidate
                break
        out[parts[0]] = {"rank": parts[2], "superkingdom": superkingdom, "lineage": lineage_text}
    return out


def batch6_host_low_frequency() -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    rows = read_csv(QUALITY_AUDIT / "top_host_review_needed.csv")
    variants = {row["host_original"]: variant_names(row["host_original"]) for row in rows}
    lookup = taxonkit_name_lookup([variant for values in variants.values() for variant in values])
    candidate_taxids = []
    selected: dict[str, tuple[dict[str, str], str]] = {}
    for raw, values in variants.items():
        for variant in values:
            match = lookup.get(normalize(variant))
            if match:
                selected[raw] = (match, variant)
                candidate_taxids.append(match["taxid"])
                break
    lineages = taxonkit_lineage(candidate_taxids)

    reviewed: list[dict[str, str]] = []
    synonyms: list[dict[str, str]] = []
    negatives: list[dict[str, str]] = []
    existing_synonyms = {compact(row.get("synonym")) for row in read_csv(HOST_SYNONYMS)}
    existing_negatives = {compact(row.get("synonym")) for row in read_csv(HOST_NEGATIVE_RULES)}

    for row in rows:
        raw = row["host_original"]
        key = normalize(raw)
        action = "leave_review_needed"
        host_sd = ""
        taxid = ""
        rank = ""
        confidence = "none"
        reason = "No conservative final-host rule matched."

        common = FINAL_HOST_COMMON.get(key)
        if common:
            host_sd, taxid, rank, reason = common
            confidence = "high" if rank in {"species", "subspecies"} else "medium"
            action = "add_reviewed_host_synonym"
        elif raw in selected:
            match, variant = selected[raw]
            lineage = lineages.get(match["taxid"], {})
            if lineage.get("superkingdom") and lineage.get("superkingdom") not in MICROBIAL_SUPERKINGDOMS:
                host_sd = match["name"]
                taxid = match["taxid"]
                rank = match["rank"]
                confidence = "high" if rank in {"species", "subspecies"} else "medium"
                action = "add_taxonkit_exact_host_synonym"
                reason = f"Exact unique TaxonKit match for `{variant}`; superkingdom={lineage.get('superkingdom')}."
            elif lineage.get("superkingdom") in MICROBIAL_SUPERKINGDOMS:
                action = "leave_review_needed"
                reason = f"TaxonKit match is microbial ({lineage.get('superkingdom')}); not auto-approved as host."

        if action.startswith("add") and compact(raw) not in existing_synonyms:
            synonyms.append(
                {
                    "synonym": raw,
                    "canonical": host_sd,
                    "taxid": taxid,
                    "confidence": confidence,
                    "note": f"Batch 6 final host curation 2026-05-05: {reason}; rank={rank}",
                }
            )
            existing_synonyms.add(compact(raw))
        reviewed.append(
            {
                "count": row.get("count", "0"),
                "host_original": raw,
                "cluster": "exact_taxonkit_scientific_name" if action == "add_taxonkit_exact_host_synonym" else ("common_host_name" if common else "ambiguous_manual_review"),
                "recommended_action": action,
                "recommended_host_sd": host_sd,
                "recommended_taxid": taxid,
                "rank": rank,
                "confidence": confidence,
                "reason": reason,
            }
        )

    write_csv(
        OUTPUT_DIR / "host_final_review_clustered.csv",
        reviewed,
        ["count", "host_original", "cluster", "recommended_action", "recommended_host_sd", "recommended_taxid", "rank", "confidence", "reason"],
    )
    return synonyms, negatives, reviewed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    existing_controlled = controlled_index(CONTROLLED_CATEGORIES)

    batch3_negatives, batch3_controlled, batch3_skipped = batch3_mapped_host_spotcheck(existing_controlled)
    batch4_controlled, batch4_skipped, batch4_reviewed = batch4_unmapped_source_routing(existing_controlled)
    batch5_controlled, batch5_skipped, batch5_reviewed = batch5_high_confidence_suggestions(existing_controlled)
    batch6_synonyms, batch6_negatives, batch6_reviewed = batch6_host_low_frequency()

    all_controlled = batch3_controlled + batch4_controlled + batch5_controlled
    all_negatives = batch3_negatives + batch6_negatives

    applied_controlled = 0
    applied_negatives = 0
    applied_synonyms = 0
    if args.apply:
        applied_controlled = append_csv(
            CONTROLLED_CATEGORIES,
            all_controlled,
            CONTROLLED_FIELDS,
            lambda row: (normalize(row.get("synonym") or row.get("normalized_value")), row.get("destination", "")),
        )
        applied_negatives = append_csv(
            HOST_NEGATIVE_RULES,
            all_negatives,
            HOST_NEGATIVE_FIELDS,
            lambda row: compact(row.get("synonym")),
        )
        applied_synonyms = append_csv(
            HOST_SYNONYMS,
            batch6_synonyms,
            HOST_SYNONYM_FIELDS,
            lambda row: compact(row.get("synonym")),
        )

    write_csv(OUTPUT_DIR / "controlled_rules_all_proposed.csv", all_controlled, CONTROLLED_FIELDS)
    write_csv(OUTPUT_DIR / "host_negative_rules_all_proposed.csv", all_negatives, HOST_NEGATIVE_FIELDS)
    write_csv(OUTPUT_DIR / "host_synonyms_batch6_proposed.csv", batch6_synonyms, HOST_SYNONYM_FIELDS)

    decision_counts = Counter(row["decision"] for row in read_csv(OUTPUT_DIR / "suspicious_source_like_mapped_hosts_reviewed.csv"))
    route_counts = Counter(row["decision"] for row in batch4_reviewed)
    batch5_counts = Counter(row["decision"] for row in batch5_reviewed)
    batch6_counts = Counter(row["recommended_action"] for row in batch6_reviewed)

    markdown = [
        "# Remaining Metadata Curation Batches",
        "",
        f"- Generated: {datetime.now(timezone.utc).isoformat()}",
        f"- Applied to rule files: {'yes' if args.apply else 'no'}",
        "",
        "## Batch 3: Source-Like Mapped Host Spot-Check",
        "",
        f"- Reviewed rows: {sum(1 for _ in read_csv(OUTPUT_DIR / 'suspicious_source_like_mapped_hosts_reviewed.csv')):,}",
        f"- Decisions: {title_count(decision_counts)}",
        f"- Host negative rules proposed: {len(batch3_negatives):,}",
        f"- Controlled routing rules proposed: {len(batch3_controlled):,}",
        "",
        "## Batch 4: Source-Like Unmapped Host Routing",
        "",
        f"- Reviewed rows: {len(batch4_reviewed):,}",
        f"- Decisions: {title_count(route_counts)}",
        f"- Controlled routing rules proposed: {len(batch4_controlled):,}",
        "",
        "## Batch 5: High-Confidence Source/Sample/Environment Suggestions",
        "",
        f"- Reviewed suggestions: {len(batch5_reviewed):,}",
        f"- Decisions: {title_count(batch5_counts)}",
        f"- Controlled rules proposed after safety filters: {len(batch5_controlled):,}",
        "",
        "## Batch 6: Final Low-Frequency Host Pass",
        "",
        f"- Reviewed host values: {len(batch6_reviewed):,}",
        f"- Decisions: {title_count(batch6_counts)}",
        f"- Host synonym rules proposed: {len(batch6_synonyms):,}",
        "",
        "## Applied Counts",
        "",
        f"- Controlled category rows appended: {applied_controlled:,}",
        f"- Host negative rows appended: {applied_negatives:,}",
        f"- Host synonym rows appended: {applied_synonyms:,}",
        "",
        "Generated files:",
        "",
        "- `suspicious_source_like_mapped_hosts_reviewed.csv`",
        "- `source_like_unmapped_hosts_routing_reviewed.csv`",
        "- `applied_high_confidence_rules_20260505_review.csv`",
        "- `host_final_review_clustered.csv`",
        "- `controlled_rules_all_proposed.csv`",
        "- `host_negative_rules_all_proposed.csv`",
        "- `host_synonyms_batch6_proposed.csv`",
    ]
    (OUTPUT_DIR / "remaining_metadata_curation_batches.md").write_text("\n".join(markdown), encoding="utf-8")

    print(OUTPUT_DIR)
    print(f"batch3_negatives_proposed\t{len(batch3_negatives)}")
    print(f"batch4_controlled_proposed\t{len(batch4_controlled)}")
    print(f"batch5_controlled_proposed\t{len(batch5_controlled)}")
    print(f"batch6_host_synonyms_proposed\t{len(batch6_synonyms)}")
    print(f"controlled_appended\t{applied_controlled}")
    print(f"host_negatives_appended\t{applied_negatives}")
    print(f"host_synonyms_appended\t{applied_synonyms}")


if __name__ == "__main__":
    main()
