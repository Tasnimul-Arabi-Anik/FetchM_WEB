#!/usr/bin/env python3
"""Build a validation-ready review of remaining host standardization candidates."""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import app, clean_host_lookup_text, get_db, normalize_standardization_lookup, run_taxonkit_lineage_batch


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "standardization" / "review" / "host_review"

CONTEXT_COLUMNS = [
    "Organism Name",
    "Host",
    "Host_Original",
    "Isolation Source",
    "Isolation Site",
    "Sample Type",
    "Environment Medium",
    "Environment (Broad Scale)",
    "Environment (Local Scale)",
    "Host_Context_SD",
    "Sample_Type_SD",
    "Isolation_Source_SD",
    "Environment_Medium_SD",
]

PERSON_OR_LAB_PATTERN = re.compile(
    r"\b("
    r"university|institute|instituto|laboratory|lab\b|csic|department|center|centre|"
    r"collection|facility|tisa lab|products lacteos|young$|ennis"
    r")\b",
    re.IGNORECASE,
)

NOISE_PATTERN = re.compile(
    r"\b("
    r"accession|barcode|sample id|specimen id|assembly|project|na$|n/a|unknown|"
    r"not applicable|not collected|not provided|missing"
    r")\b",
    re.IGNORECASE,
)

LAB_MICROBE_PATTERN = re.compile(
    r"\b("
    r"pseudomonas|bacillus|salmonella|staphylococcus|streptococcus|escherichia|"
    r"clostridium|clostridioides|enterococcus|enterobacter|klebsiella|vibrio|"
    r"lactobacillus|bifidobacterium|mycobacterium|campylobacter"
    r")\b",
    re.IGNORECASE,
)

LAB_CONTEXT_PATTERN = re.compile(
    r"\b(strain|culture|isolate|cell culture|pure culture|bacterium culture|microbial culture|single colony)\b",
    re.IGNORECASE,
)

SOURCE_ROUTE_PATTERNS: list[tuple[re.Pattern[str], str, str, str]] = [
    (re.compile(r"\b(food|pickle|cheese|milk|dairy|meat|pork|beef|seafood|fish product)\b", re.I), "non_host_source", "Isolation_Source_SD", "food/source material"),
    (re.compile(r"\b(water|wastewater|sewage|sediment|soil|sand|rhizosphere|hydrothermal|spring|river|lake|sea)\b", re.I), "non_host_source", "Environment_Medium_SD", "environmental material"),
    (re.compile(r"\b(flower shop|fermenter|nursery|field|farm|hospital|icu|facility)\b", re.I), "non_host_source", "Isolation_Source_SD", "site/source context"),
    (re.compile(r"\b(metagenome|metagenomic assembly|microbial community)\b", re.I), "non_host_source", "Sample_Type_SD", "metagenomic assembly/community descriptor"),
]

BROAD_HOSTS: dict[str, tuple[str, str, str, str]] = {
    "ape": ("Hominoidea", "314295", "superfamily", "broad ape host; avoids ambiguous plant taxon named ape"),
    "common chimpanzee": ("Pan troglodytes", "9598", "species", "chimpanzee host"),
    "people": ("Homo sapiens", "9606", "species", "human host"),
    "person": ("Homo sapiens", "9606", "species", "human host"),
    "fowl": ("Gallus gallus", "9031", "species", "domestic fowl/chicken host"),
    "animal": ("Metazoa", "33208", "kingdom", "broad animal host"),
    "mammal": ("Mammalia", "40674", "class", "broad mammal host"),
    "mammals": ("Mammalia", "40674", "class", "broad mammal host"),
    "bird": ("Aves", "8782", "class", "broad bird host"),
    "birds": ("Aves", "8782", "class", "broad bird host"),
    "avian": ("Aves", "8782", "class", "broad bird host"),
    "seabird": ("Aves", "8782", "class", "broad bird host"),
    "wader": ("Aves", "8782", "class", "broad bird host"),
    "nestling stork": ("Aves", "8782", "class", "broad bird host"),
    "day-old chicks": ("Gallus gallus", "9031", "species", "domestic chicken host"),
    "day old chicks": ("Gallus gallus", "9031", "species", "domestic chicken host"),
    "chick": ("Gallus gallus", "9031", "species", "domestic chicken host"),
    "chicks": ("Gallus gallus", "9031", "species", "domestic chicken host"),
    "ox": ("Bos taurus", "9913", "species", "cattle host"),
    "sprague dawley": ("Rattus norvegicus", "10116", "species", "laboratory rat strain"),
    "rat": ("Rattus norvegicus", "10116", "species", "rat host"),
    "mouse": ("Mus musculus", "10090", "species", "mouse host"),
    "mus": ("Mus", "10088", "genus", "mouse genus host"),
    "rodent": ("Rodentia", "9989", "order", "broad rodent host"),
    "reptile": ("Reptilia", "8457", "class", "broad reptile host"),
    "testudines": ("Testudines", "8459", "order", "turtle/tortoise host"),
    "gecko": ("Gekkota", "8509", "infraorder", "broad gecko host"),
    "pogona": ("Pogona", "103695", "genus", "bearded dragon genus host"),
    "pogona sp": ("Pogona", "103695", "genus", "bearded dragon genus host"),
    "fish": ("Actinopterygii", "7898", "class", "broad ray-finned fish host"),
    "sturgeon": ("Acipenseridae", "7902", "family", "sturgeon host"),
    "sablefish": ("Anoplopoma fimbria", "229290", "species", "sablefish host"),
    "scallop": ("Pectinidae", "6570", "family", "scallop host"),
    "oyster": ("Ostreidae", "6563", "family", "oyster host"),
    "porifera": ("Porifera", "6040", "phylum", "sponge host"),
    "sponge": ("Porifera", "6040", "phylum", "sponge host"),
    "protist": ("Eukaryota", "2759", "superkingdom", "broad protist/eukaryote host"),
    "uncultured eukaryote": ("Eukaryota", "2759", "superkingdom", "broad eukaryote host"),
    "algae": ("Viridiplantae", "33090", "kingdom", "broad algal/plant host"),
    "macroalgae": ("Viridiplantae", "33090", "kingdom", "broad algal/plant host"),
    "marine macroalgae": ("Viridiplantae", "33090", "kingdom", "broad algal/plant host"),
    "kelp": ("Phaeophyceae", "2870", "class", "kelp/brown algae host"),
    "plant": ("Viridiplantae", "33090", "kingdom", "broad plant host"),
    "plants": ("Viridiplantae", "33090", "kingdom", "broad plant host"),
    "flower": ("Viridiplantae", "33090", "kingdom", "plant material; host not taxonomically specific"),
    "southern caracara": ("Aves", "8782", "class", "bird host; species-level TaxonKit exact lookup unavailable"),
    "crested tern": ("Thalasseus bergii", "1843445", "species", "crested tern host"),
    "bullfinch": ("Aves", "8782", "class", "bird host; common name is not species-specific"),
    "vulture": ("Aves", "8782", "class", "broad vulture/bird host"),
    "oregon junco": ("Junco", "40209", "genus", "junco host; subspecies/common-name resolution should be validated"),
    "owl": ("Strigiformes", "30472", "order", "broad owl host"),
    "crane": ("Gruidae", "9109", "family", "broad crane host"),
    "chacoan mara": ("Dolichotis salinicola", "210422", "species", "chacoan mara host"),
    "finnraccoon": ("Nyctereutes procyonoides", "34880", "species", "finnraccoon/finnish raccoon dog host"),
    "aachener minipig": ("Sus scrofa", "9823", "species", "domestic pig/minipig host"),
    "pufferfish": ("Tetraodontiformes", "31031", "order", "broad pufferfish host; avoids viral TaxonKit fuzzy false positive"),
    "albino bristlenose pleco": ("Ancistrus", "52070", "genus", "bristlenose pleco host"),
}

COMMON_PLANT_MATERIAL = {
    "parsley",
    "tea",
    "artichoke",
    "muskmelon",
    "melon",
    "mungbean",
    "beet",
    "blackberry",
    "plantain",
    "clover",
    "medicinal cannabis",
    "beaked hazelnut",
    "coffe tree",
    "coffee tree",
}


def text_is_missing(value: Any) -> bool:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return True
    return bool(NOISE_PATTERN.search(text))


def first_nonempty(row: pd.Series, columns: list[str]) -> str:
    for column in columns:
        value = row.get(column, "")
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() != "nan":
            return text
    return ""


def get_genus_paths(limit: int | None = None) -> list[tuple[str, Path]]:
    with app.app_context():
        db = get_db()
        rows = db.execute(
            """
            SELECT species_name, metadata_clean_path
            FROM species
            WHERE taxon_rank='genus'
              AND metadata_status='ready'
              AND metadata_clean_path IS NOT NULL
            ORDER BY species_name
            """
        ).fetchall()
    paths = [(str(row["species_name"]), Path(row["metadata_clean_path"])) for row in rows]
    return paths[:limit] if limit else paths


def read_review_needed(paths: list[tuple[str, Path]]) -> tuple[Counter[str], dict[str, dict[str, str]], Counter[str]]:
    counts: Counter[str] = Counter()
    examples: dict[str, dict[str, str]] = {}
    file_errors: Counter[str] = Counter()
    wanted = set(CONTEXT_COLUMNS + ["Host_Review_Status"])
    for genus, path in paths:
        if not path.exists():
            file_errors[f"missing:{path}"] += 1
            continue
        try:
            chunks = pd.read_csv(
                path,
                dtype=str,
                chunksize=100000,
                usecols=lambda column: column in wanted,
                on_bad_lines="skip",
                low_memory=False,
            )
            for chunk in chunks:
                if "Host_Review_Status" not in chunk.columns:
                    continue
                mask = chunk["Host_Review_Status"].fillna("").astype(str).eq("review_needed")
                if not mask.any():
                    continue
                for _, row in chunk.loc[mask].iterrows():
                    host = first_nonempty(row, ["Host_Original", "Host"])
                    if not host:
                        continue
                    counts[host] += 1
                    if host not in examples:
                        examples[host] = {
                            "example_genus": genus,
                            "example_organism": first_nonempty(row, ["Organism Name"]),
                            "example_isolation_source": first_nonempty(row, ["Isolation Source"]),
                            "example_isolation_site": first_nonempty(row, ["Isolation Site"]),
                            "example_sample_type": first_nonempty(row, ["Sample Type"]),
                            "example_environment_medium": first_nonempty(row, ["Environment Medium"]),
                            "example_environment_broad": first_nonempty(row, ["Environment (Broad Scale)"]),
                            "example_environment_local": first_nonempty(row, ["Environment (Local Scale)"]),
                            "existing_host_context": first_nonempty(row, ["Host_Context_SD"]),
                            "existing_sample_type_sd": first_nonempty(row, ["Sample_Type_SD"]),
                            "existing_isolation_source_sd": first_nonempty(row, ["Isolation_Source_SD"]),
                            "existing_environment_medium_sd": first_nonempty(row, ["Environment_Medium_SD"]),
                        }
        except Exception as exc:  # pragma: no cover - operational audit path
            file_errors[f"{path}:{exc}"] += 1
    return counts, examples, file_errors


def taxonkit_name_lookup(names: list[str]) -> dict[str, dict[str, str]]:
    if not names:
        return {}
    input_text = "\n".join(names) + "\n"
    try:
        result = subprocess.run(
            ["taxonkit", "name2taxid", "--show-rank"],
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return {}
    raw_matches: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for line in result.stdout.splitlines():
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 3:
            continue
        name, taxid, rank = parts[0].strip(), parts[1].strip(), parts[2].strip()
        if not name or not taxid:
            continue
        raw_matches[normalize_standardization_lookup(name)].append({"taxon_name": name, "taxid": taxid, "rank": rank})
    matches: dict[str, dict[str, str]] = {}
    for key, key_matches in raw_matches.items():
        unique_taxids = {match["taxid"] for match in key_matches}
        if len(unique_taxids) == 1:
            matches[key] = key_matches[0]
    return matches


def lookup_key_variants(value: str) -> list[str]:
    cleaned = clean_host_lookup_text(value)
    variants = [cleaned]
    stripped = re.sub(r"\b(specimen|sample|strain|isolate|culture)\b.*$", "", cleaned).strip()
    if stripped and stripped not in variants:
        variants.append(stripped)
    stripped = re.sub(r"\bsp\.?$", "", stripped).strip()
    if stripped and stripped not in variants:
        variants.append(stripped)
    return variants


def classify_value(value: str, example: dict[str, str], taxon_matches: dict[str, dict[str, str]]) -> dict[str, str]:
    cleaned = clean_host_lookup_text(value)
    normalized = normalize_standardization_lookup(value)
    compact = re.sub(r"[^a-z0-9]+", "", normalized)
    context = " | ".join(example.get(key, "") for key in example if key.startswith("example_")).lower()

    if text_is_missing(value) or re.fullmatch(r"[\W\d_]+", value.strip()):
        return {
            "proposed_decision": "not_identifiable_or_missing",
            "proposed_host_sd": "",
            "proposed_taxid": "",
            "proposed_rank": "",
            "confidence": "high",
            "suggested_destination": "Host_Review_Status=not_identifiable",
            "reasoning": "Value is empty, numeric/symbolic, or an administrative missing/noise token.",
            "apply_recommendation": "safe_to_apply",
        }

    if PERSON_OR_LAB_PATTERN.search(value):
        return {
            "proposed_decision": "non_host_lab_or_person",
            "proposed_host_sd": "",
            "proposed_taxid": "",
            "proposed_rank": "",
            "confidence": "high",
            "suggested_destination": "Host_SD blank; preserve original; route context to laboratory/source if needed",
            "reasoning": "Value appears to be a collector, laboratory, institute, or facility rather than a biological host.",
            "apply_recommendation": "safe_to_apply",
        }

    if LAB_MICROBE_PATTERN.search(value) and (LAB_CONTEXT_PATTERN.search(value) or LAB_CONTEXT_PATTERN.search(context)):
        return {
            "proposed_decision": "non_host_microbe_culture_descriptor",
            "proposed_host_sd": "",
            "proposed_taxid": "",
            "proposed_rank": "",
            "confidence": "high",
            "suggested_destination": "Sample_Type_SD=pure/single culture or bacterial culture",
            "reasoning": "Bacterial name is used as culture/isolate/strain context, so it should not become Host_SD.",
            "apply_recommendation": "safe_to_apply",
        }

    if normalized in COMMON_PLANT_MATERIAL:
        return {
            "proposed_decision": "broad_host_group",
            "proposed_host_sd": "Viridiplantae",
            "proposed_taxid": "33090",
            "proposed_rank": "kingdom",
            "confidence": "medium",
            "suggested_destination": "Host_SD=Viridiplantae; keep Host_Common_Name as original",
            "reasoning": "Common plant/material term; useful as broad plant host but not species-level.",
            "apply_recommendation": "review_before_apply",
        }

    for key in lookup_key_variants(value):
        if key in BROAD_HOSTS:
            name, taxid, rank, reason = BROAD_HOSTS[key]
            return {
                "proposed_decision": "broad_host_group",
                "proposed_host_sd": name,
                "proposed_taxid": taxid,
                "proposed_rank": rank,
                "confidence": "medium",
                "suggested_destination": f"Host_SD={name}; Host_TaxID={taxid}",
                "reasoning": reason,
                "apply_recommendation": "review_before_apply",
            }

    for key in lookup_key_variants(value):
        match = taxon_matches.get(key)
        if match:
            confidence = "high" if key == normalized else "medium"
            return {
                "proposed_decision": "taxonomy_exact_lookup",
                "proposed_host_sd": match["taxon_name"],
                "proposed_taxid": match["taxid"],
                "proposed_rank": match["rank"],
                "confidence": confidence,
                "suggested_destination": f"Host_SD={match['taxon_name']}; Host_TaxID={match['taxid']}",
                "reasoning": "TaxonKit exact name lookup found a taxonomic identifier.",
                "apply_recommendation": "safe_to_apply" if confidence == "high" else "review_before_apply",
            }

    for pattern, decision, destination, label in SOURCE_ROUTE_PATTERNS:
        if pattern.search(value):
            return {
                "proposed_decision": decision,
                "proposed_host_sd": "",
                "proposed_taxid": "",
                "proposed_rank": "",
                "confidence": "medium",
                "suggested_destination": destination,
                "reasoning": f"Host value itself describes {label}, not a host organism.",
                "apply_recommendation": "safe_to_apply_if_destination_empty",
            }

    if compact in {"tique", "tiques"}:
        return {
            "proposed_decision": "probable_broad_host_group",
            "proposed_host_sd": "Ixodida",
            "proposed_taxid": "6935",
            "proposed_rank": "order",
            "confidence": "low",
            "suggested_destination": "Possible tick term; validate before applying",
            "reasoning": "Tique is likely a tick term in some languages, but this is not safe enough for automatic mapping.",
            "apply_recommendation": "manual_validation_required",
        }

    if len(value.strip()) <= 2:
        return {
            "proposed_decision": "not_identifiable_or_missing",
            "proposed_host_sd": "",
            "proposed_taxid": "",
            "proposed_rank": "",
            "confidence": "medium",
            "suggested_destination": "Host_Review_Status=not_identifiable",
            "reasoning": "Too short to identify reliably without context.",
            "apply_recommendation": "safe_to_apply",
        }

    return {
        "proposed_decision": "manual_review_remaining",
        "proposed_host_sd": "",
        "proposed_taxid": "",
        "proposed_rank": "",
        "confidence": "none",
        "suggested_destination": "Keep in review queue",
        "reasoning": "No conservative taxonomy/source/noise rule matched.",
        "apply_recommendation": "manual_validation_required",
    }


def write_outputs(
    output_dir: Path,
    counts: Counter[str],
    examples: dict[str, dict[str, str]],
    decisions: list[dict[str, str]],
    file_errors: Counter[str],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "remaining_host_review_detailed.csv"
    fieldnames = [
        "count",
        "host_original",
        "host_cleaned",
        "proposed_decision",
        "proposed_host_sd",
        "proposed_taxid",
        "proposed_rank",
        "proposed_superkingdom",
        "proposed_phylum",
        "proposed_class",
        "proposed_order",
        "proposed_family",
        "proposed_genus",
        "proposed_species",
        "confidence",
        "apply_recommendation",
        "suggested_destination",
        "reasoning",
        "example_genus",
        "example_organism",
        "example_isolation_source",
        "example_isolation_site",
        "example_sample_type",
        "example_environment_medium",
        "example_environment_broad",
        "example_environment_local",
        "existing_host_context",
        "existing_sample_type_sd",
        "existing_isolation_source_sd",
        "existing_environment_medium_sd",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in decisions:
            writer.writerow({key: row.get(key, "") for key in fieldnames})

    decision_counts = Counter(row["proposed_decision"] for row in decisions)
    apply_counts = Counter(row["apply_recommendation"] for row in decisions)
    row_decision_counts: defaultdict[str, int] = defaultdict(int)
    for row in decisions:
        row_decision_counts[row["proposed_decision"]] += int(row["count"])

    md_lines = [
        "# Remaining Host Review - Comprehensive Proposal",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Scope",
        "",
        f"- Unique host values reviewed: {len(decisions):,}",
        f"- Total review-needed rows represented: {sum(counts.values()):,}",
        f"- File read issues: {sum(file_errors.values()):,}",
        "",
        "## Proposed Decision Summary",
        "",
        "| Decision | Unique values | Rows represented |",
        "|---|---:|---:|",
    ]
    for decision, unique_count in decision_counts.most_common():
        md_lines.append(f"| {decision} | {unique_count:,} | {row_decision_counts[decision]:,} |")
    md_lines.extend(
        [
            "",
            "## Apply Recommendation Summary",
            "",
            "| Recommendation | Unique values |",
            "|---|---:|",
        ]
    )
    for recommendation, unique_count in apply_counts.most_common():
        md_lines.append(f"| {recommendation} | {unique_count:,} |")
    md_lines.extend(
        [
            "",
            "## Highest Impact Values",
            "",
            "| Count | Host original | Proposed decision | Proposed Host_SD | TaxID | Recommendation | Reasoning |",
            "|---:|---|---|---|---|---|---|",
        ]
    )
    for row in decisions[:120]:
        md_lines.append(
            "| {count} | {host_original} | {proposed_decision} | {proposed_host_sd} | {proposed_taxid} | {apply_recommendation} | {reasoning} |".format(
                **{key: str(row.get(key, "")).replace("|", "/") for key in row}
            )
        )
    if file_errors:
        md_lines.extend(["", "## File Read Issues", ""])
        for issue, count in file_errors.most_common(20):
            md_lines.append(f"- {count}: `{issue}`")
    md_lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "The `safe_to_apply` decisions are conservative administrative, missing/noise, exact taxonomy, or non-host culture/source decisions.",
            "The `review_before_apply` decisions are biologically plausible but broader than a species-level host, so they should be validated before becoming permanent rules.",
            "The `manual_validation_required` decisions should remain in review unless another model or curator validates them.",
        ]
    )
    (output_dir / "remaining_host_review_comprehensive.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    paths = get_genus_paths(args.limit)
    counts, examples, file_errors = read_review_needed(paths)
    lookup_names = sorted({variant for value in counts for variant in lookup_key_variants(value) if variant})
    taxon_matches = taxonkit_name_lookup(lookup_names)

    base_decisions: list[tuple[str, int, dict[str, str], dict[str, str]]] = []
    for value, count in counts.most_common():
        example = examples.get(value, {})
        decision = classify_value(value, example, taxon_matches)
        base_decisions.append((value, count, example, decision))

    lineage_by_taxid = run_taxonkit_lineage_batch(
        sorted({decision.get("proposed_taxid", "") for _, _, _, decision in base_decisions if decision.get("proposed_taxid")})
    )

    decisions: list[dict[str, str]] = []
    for value, count, example, decision in base_decisions:
        lineage = lineage_by_taxid.get(decision.get("proposed_taxid", ""), {})
        decision.update(
            {
                "count": str(count),
                "host_original": value,
                "host_cleaned": clean_host_lookup_text(value),
                "proposed_superkingdom": lineage.get("Host_Superkingdom", ""),
                "proposed_phylum": lineage.get("Host_Phylum", ""),
                "proposed_class": lineage.get("Host_Class", ""),
                "proposed_order": lineage.get("Host_Order", ""),
                "proposed_family": lineage.get("Host_Family", ""),
                "proposed_genus": lineage.get("Host_Genus", ""),
                "proposed_species": lineage.get("Host_Species", ""),
                **example,
            }
        )
        decisions.append(decision)
    write_outputs(args.output_dir, counts, examples, decisions, file_errors)
    print(args.output_dir)
    print(f"unique_values\t{len(decisions)}")
    print(f"rows_represented\t{sum(counts.values())}")
    print(f"file_read_issues\t{sum(file_errors.values())}")
    for decision, unique_count in Counter(row["proposed_decision"] for row in decisions).most_common():
        row_count = sum(int(row["count"]) for row in decisions if row["proposed_decision"] == decision)
        print(f"{decision}\t{unique_count}\t{row_count}")


if __name__ == "__main__":
    main()
