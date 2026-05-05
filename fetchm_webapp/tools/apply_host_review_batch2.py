"""Apply conservative host curation rules from the final low-frequency review.

This batch intentionally avoids embeddings and fuzzy auto-approval. It uses:
- existing review_remaining_host_candidates.py output,
- exact TaxonKit matches after light author/strain cleanup,
- lineage checks to prevent bacterial/archaeal/viral names from becoming Host_SD,
- a small reviewed broad-host/common-name list.
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "standardization" / "review" / "host_review" / "20260505_batch2" / "remaining_host_review_detailed.csv"
DEFAULT_OUTPUT_DIR = ROOT / "standardization" / "review" / "host_review" / "20260505_batch2"
HOST_SYNONYMS = ROOT / "standardization" / "host_synonyms.csv"
HOST_NEGATIVE_RULES = ROOT / "standardization" / "host_negative_rules.csv"


MICROBIAL_SUPERKINGDOMS = {"Bacteria", "Archaea", "Viruses"}

REVIEWED_BROAD_HOSTS: dict[str, tuple[str, str, str, str]] = {
    "aachener minipig": ("Sus scrofa", "9823", "species", "reviewed domestic minipig host"),
    "anopheles gambiae g3 strain lab colony": ("Anopheles gambiae", "7165", "species", "reviewed mosquito host; lab colony is context"),
    "anopheles gambiae g3 strain, lab colony": ("Anopheles gambiae", "7165", "species", "reviewed mosquito host; lab colony is context"),
    "arvicolinae sp": ("Arvicolinae", "39087", "subfamily", "reviewed broad vole/lemming/related rodent host"),
    "blackberry": ("Viridiplantae", "33090", "kingdom", "reviewed broad plant host/material"),
    "celery": ("celery", "117781", "no rank", "reviewed plant host/common name"),
    "chacoan mara": ("Dolichotis salinicola", "210422", "species", "reviewed common animal host"),
    "clover": ("Viridiplantae", "33090", "kingdom", "reviewed broad plant host/material"),
    "flower": ("Viridiplantae", "33090", "kingdom", "reviewed broad plant host/material"),
    "hexactinellid": ("Hexactinellida", "60882", "class", "reviewed sponge class host"),
    "macroalgae": ("Viridiplantae", "33090", "kingdom", "reviewed broad algal/plant host"),
    "marine macroalgae": ("Viridiplantae", "33090", "kingdom", "reviewed broad algal/plant host"),
    "melon": ("Viridiplantae", "33090", "kingdom", "reviewed broad plant host/material"),
    "protist": ("Eukaryota", "2759", "superkingdom", "reviewed broad eukaryote/protist host"),
    "sprague dawley": ("Rattus norvegicus", "10116", "species", "reviewed laboratory rat strain host"),
    "sponges": ("Porifera", "6040", "phylum", "reviewed broad sponge host"),
    "uncultured eukaryote": ("Eukaryota", "2759", "superkingdom", "reviewed broad eukaryote host"),
}

REVIEWED_NEGATIVES: dict[str, tuple[str, str]] = {
    "sea whip gorgonian": ("non_host_source", "reviewed source-like descriptor; keep out of Host_SD unless explicit host context is curated"),
}


def normalize(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def compact(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize(value))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def existing_keys(path: Path, key_field: str) -> set[str]:
    if not path.exists():
        return set()
    rows = read_csv(path)
    return {compact(row.get(key_field)) for row in rows if row.get(key_field)}


def append_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> int:
    if not rows:
        return 0
    existing = existing_keys(path, fieldnames[0])
    new_rows = [row for row in rows if compact(row.get(fieldnames[0])) not in existing]
    if not new_rows:
        return 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writerows(new_rows)
    return len(new_rows)


def variant_names(raw: str) -> list[str]:
    cleaned = raw.strip()
    cleaned = re.sub(r"\([^)]*\)", "", cleaned)
    cleaned = re.sub(r"\[[^\]]*\]", "", cleaned)
    cleaned = re.sub(
        r"\b(strain|isolate|clone|sample|specimen|larvae|larva|colony|lineage|cv|var|ssp)\b.*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip(" ,;:.")
    cleaned = re.sub(
        r"\b(L\.|L\.DC\.|DC\.|Gray|Bertoni|Meyer|Sieb\.|Vent\.|Bunge|Miq\.|Oliver|Crantz|Mill\.|Hance|Chatin|Duch\.|Ueda|Linn\.|Maxim\.)\b",
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
        if len(words) >= 3:
            trinomial = " ".join(words[:3]).strip(" ,;:.")
            if trinomial not in variants:
                variants.append(trinomial)
    return variants


def taxonkit_name_lookup(names: list[str]) -> dict[str, dict[str, str]]:
    unique_names = sorted({name for name in names if name})
    if not unique_names:
        return {}
    result = subprocess.run(
        ["taxonkit", "name2taxid", "--show-rank"],
        input="\n".join(unique_names) + "\n",
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    raw: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) >= 3 and parts[1].strip():
            raw[normalize(parts[0])].append({"name": parts[0].strip(), "taxid": parts[1].strip(), "rank": parts[2].strip()})
    matches: dict[str, dict[str, str]] = {}
    for key, values in raw.items():
        taxids = {value["taxid"] for value in values}
        if len(taxids) == 1:
            matches[key] = values[0]
    return matches


def taxonkit_lineage(taxids: list[str]) -> dict[str, dict[str, str]]:
    unique_taxids = sorted({taxid for taxid in taxids if str(taxid).isdigit()})
    if not unique_taxids:
        return {}
    lineage_result = subprocess.run(
        ["taxonkit", "lineage", "-r"],
        input="\n".join(unique_taxids) + "\n",
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    lineage: dict[str, dict[str, str]] = {}
    for line in lineage_result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        taxid, lineage_text, rank = parts[0].strip(), parts[1].strip(), parts[2].strip()
        lineage_set = set(lineage_text.split(";"))
        superkingdom = ""
        for candidate in ("Eukaryota", "Bacteria", "Archaea", "Viruses"):
            if candidate in lineage_set:
                superkingdom = candidate
                break
        lineage[taxid] = {"rank": rank, "superkingdom": superkingdom, "lineage": lineage_text}
    return lineage


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    rows = read_csv(args.input)
    name_variants: dict[str, list[str]] = {row["host_original"]: variant_names(row["host_original"]) for row in rows}
    lookup = taxonkit_name_lookup([variant for variants in name_variants.values() for variant in variants])

    candidate_taxids: set[str] = set()
    for row in rows:
        reviewed = REVIEWED_BROAD_HOSTS.get(normalize(row["host_original"]))
        if reviewed:
            candidate_taxids.add(reviewed[1])
            continue
        for variant in name_variants[row["host_original"]]:
            match = lookup.get(normalize(variant))
            if match:
                candidate_taxids.add(match["taxid"])
                break
    lineages = taxonkit_lineage(sorted(candidate_taxids))

    clustered: list[dict[str, str]] = []
    synonym_rows: list[dict[str, str]] = []
    negative_rows: list[dict[str, str]] = []

    for row in rows:
        raw = row["host_original"]
        key = normalize(raw)
        count = row.get("count", "0")
        cluster = "ambiguous_manual_review"
        action = "leave_review_needed"
        host_sd = ""
        taxid = ""
        rank = ""
        destination = "Host_Review_Status=review_needed"
        reason = "No conservative deterministic rule matched."
        confidence = "none"

        if key in REVIEWED_NEGATIVES:
            decision, note = REVIEWED_NEGATIVES[key]
            cluster = "food_or_product_not_host"
            action = "add_host_negative_rule"
            destination = f"Host_Review_Status={decision}"
            reason = note
            confidence = "medium"
            negative_rows.append({"synonym": raw, "decision": decision, "note": f"Batch 2 host curation 2026-05-05: {note}"})
        elif key in REVIEWED_BROAD_HOSTS:
            host_sd, taxid, rank, reason = REVIEWED_BROAD_HOSTS[key]
            cluster = "common_animal_or_broad_host_name"
            action = "add_host_synonym"
            destination = f"Host_SD={host_sd}; Host_TaxID={taxid}"
            confidence = "medium" if rank not in {"species", "subspecies"} else "high"
            synonym_rows.append(
                {
                    "synonym": raw,
                    "canonical": host_sd,
                    "taxid": taxid,
                    "confidence": confidence,
                    "note": f"Batch 2 host curation 2026-05-05: {reason}; rank={rank}",
                }
            )
        else:
            selected_match: dict[str, str] | None = None
            selected_variant = ""
            for variant in name_variants[raw]:
                match = lookup.get(normalize(variant))
                if match:
                    selected_match = match
                    selected_variant = variant
                    break
            if selected_match:
                host_sd = selected_match["name"]
                taxid = selected_match["taxid"]
                rank = selected_match["rank"]
                lineage = lineages.get(taxid, {})
                superkingdom = lineage.get("superkingdom", "")
                if superkingdom in MICROBIAL_SUPERKINGDOMS:
                    cluster = "microbial_or_viral_not_host"
                    action = "add_host_negative_rule"
                    destination = "Host_Review_Status=non_host_source"
                    reason = f"Exact TaxonKit match is {superkingdom}; bacterial/archaeal/viral taxa are blocked from Host_SD in this curation policy."
                    confidence = "high"
                    negative_rows.append(
                        {
                            "synonym": raw,
                            "decision": "non_host_source",
                            "note": f"Batch 2 host curation 2026-05-05: {reason}",
                        }
                    )
                    host_sd = ""
                    taxid = ""
                    rank = ""
                elif superkingdom == "Eukaryota":
                    cluster = "exact_taxonkit_scientific_name"
                    action = "add_host_synonym"
                    destination = f"Host_SD={host_sd}; Host_TaxID={taxid}"
                    reason = f"Exact TaxonKit match from variant `{selected_variant}`; lineage superkingdom Eukaryota."
                    confidence = "high" if normalize(selected_variant) == normalize(raw) else "medium"
                    synonym_rows.append(
                        {
                            "synonym": raw,
                            "canonical": host_sd,
                            "taxid": taxid,
                            "confidence": confidence,
                            "note": f"Batch 2 host curation 2026-05-05: {reason}; rank={rank}",
                        }
                    )
                else:
                    cluster = "ambiguous_manual_review"
                    reason = "TaxonKit matched but lineage superkingdom was unavailable; left for manual review."

        clustered.append(
            {
                "count": count,
                "host_original": raw,
                "cluster": cluster,
                "recommended_action": action,
                "recommended_host_sd": host_sd,
                "recommended_taxid": taxid,
                "recommended_rank": rank,
                "recommended_destination": destination,
                "reason": reason,
                "confidence": confidence,
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "count",
        "host_original",
        "cluster",
        "recommended_action",
        "recommended_host_sd",
        "recommended_taxid",
        "recommended_rank",
        "recommended_destination",
        "reason",
        "confidence",
    ]
    write_csv(args.output_dir / "host_final_review_clustered.csv", clustered, fieldnames)

    applied_synonyms = append_rows(HOST_SYNONYMS, synonym_rows, ["synonym", "canonical", "taxid", "confidence", "note"]) if args.apply else 0
    applied_negatives = append_rows(HOST_NEGATIVE_RULES, negative_rows, ["synonym", "decision", "note"]) if args.apply else 0

    rows_by_cluster: defaultdict[str, int] = defaultdict(int)
    unique_by_cluster: defaultdict[str, int] = defaultdict(int)
    for row in clustered:
        unique_by_cluster[row["cluster"]] += 1
        rows_by_cluster[row["cluster"]] += int(row["count"] or 0)

    summary = [
        "# Host Final Review Batch 2",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Applied rules: {'yes' if args.apply else 'no'}",
        "",
        "## Scope",
        "",
        f"- Unique values reviewed: {len(clustered):,}",
        f"- Rows represented: {sum(int(row.get('count') or 0) for row in rows):,}",
        f"- Host synonym rules proposed: {len(synonym_rows):,}",
        f"- Host negative rules proposed: {len(negative_rows):,}",
        f"- Host synonym rules appended: {applied_synonyms:,}",
        f"- Host negative rules appended: {applied_negatives:,}",
        "",
        "## Cluster Summary",
        "",
        "| Cluster | Unique values | Rows represented |",
        "|---|---:|---:|",
    ]
    for cluster, unique_count in sorted(unique_by_cluster.items(), key=lambda item: rows_by_cluster[item[0]], reverse=True):
        summary.append(f"| {cluster} | {unique_count:,} | {rows_by_cluster[cluster]:,} |")
    summary.extend(
        [
            "",
            "## Policy",
            "",
            "Only exact Eukaryota TaxonKit matches, reviewed broad/common host names, and reviewed negative host descriptors are applied.",
            "Bacterial, archaeal, and viral exact matches are blocked from Host_SD because they are usually isolate/pathogen/lab descriptors in this bacterial-genome metadata context.",
            "Ambiguous, malformed, hybrid, or lineage-unresolved values remain in manual review.",
        ]
    )
    (args.output_dir / "host_final_review_summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")

    print(args.output_dir)
    print(f"unique_values\t{len(clustered)}")
    print(f"rows_represented\t{sum(int(row.get('count') or 0) for row in rows)}")
    print(f"synonym_rules_proposed\t{len(synonym_rows)}")
    print(f"negative_rules_proposed\t{len(negative_rows)}")
    print(f"synonym_rules_appended\t{applied_synonyms}")
    print(f"negative_rules_appended\t{applied_negatives}")
    for cluster, unique_count in sorted(unique_by_cluster.items(), key=lambda item: rows_by_cluster[item[0]], reverse=True):
        print(f"{cluster}\t{unique_count}\t{rows_by_cluster[cluster]}")


if __name__ == "__main__":
    main()
