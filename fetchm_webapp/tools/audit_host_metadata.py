from __future__ import annotations

import csv
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import (  # noqa: E402
    NON_HOST_SOURCE_HINTS,
    NON_HOST_SOURCE_PATTERN,
    app,
    clean_host_lookup_text,
    get_db,
    metadata_value_is_missing,
    normalize_standardization_lookup,
    standardize_host_metadata,
)


BROAD_HOST_TERMS = {
    "animal",
    "animals",
    "avian",
    "bird",
    "birds",
    "fish",
    "fishes",
    "insect",
    "insects",
    "mammal",
    "mammals",
    "plant",
    "plants",
    "poultry",
    "reptile",
    "reptiles",
    "rodent",
    "rodents",
    "shellfish",
    "wild bird",
    "wild birds",
}

SOURCE_OR_SAMPLE_REVIEW_PATTERN = re.compile(
    r"\b("
    r"blood|biopsy|broth|carcass|culture|environment|environmental|feces|faeces|fecal|faecal|"
    r"food|gut|intestinal|manure|meat|metagenome|milk|product|sample|seafood|sediment|"
    r"sludge|soil|stool|surface|swab|tissue|urine|wastewater|water"
    r")\b",
    re.IGNORECASE,
)

SCIENTIFIC_NAME_PATTERN = re.compile(
    r"^[A-Z][a-z]+(?:\s+(?:[a-z][a-z-]+|x|cf\.|aff\.|sp\.|subsp\.|var\.)){1,3}(?:\s+[A-Za-z0-9_.-]+)?$"
)


def write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def classify_unresolved_host(value: Any) -> tuple[str, str]:
    text = "" if value is None else str(value).strip()
    if metadata_value_is_missing(text):
        return "missing_value", "Treat as absent; do not map to Host_SD."

    cleaned = clean_host_lookup_text(text)
    normalized = normalize_standardization_lookup(text)

    if cleaned in NON_HOST_SOURCE_HINTS or NON_HOST_SOURCE_PATTERN.search(cleaned):
        return "sample_source_misplaced_in_host", "Move to controlled source/sample/environment mapping, not Host_SD."

    if SOURCE_OR_SAMPLE_REVIEW_PATTERN.search(cleaned):
        return "sample_source_misplaced_in_host", "Review for controlled_categories.csv destination."

    if normalized in BROAD_HOST_TERMS or cleaned in BROAD_HOST_TERMS:
        return "broad_host", "Review as broad host category; use medium confidence if accepted."

    if SCIENTIFIC_NAME_PATTERN.match(text):
        return "taxonomic_name_needing_taxid_lookup", "Resolve with NCBI Taxonomy/TaxonKit before adding to host_synonyms.csv."

    if len(cleaned.split()) >= 2 and not re.search(r"\d|[:;/]", cleaned):
        return "taxonomic_name_needing_taxid_lookup", "Likely name phrase; resolve with NCBI Taxonomy or manual review."

    return "ambiguous_free_text", "Keep for manual review or later embedding/AI-assisted triage."


def review_decision_for_class(unresolved_class: str) -> str:
    if unresolved_class == "missing_value":
        return "missing"
    if unresolved_class == "sample_source_misplaced_in_host":
        return "non_host_source"
    if unresolved_class == "broad_host":
        return "broad_host"
    return "needs_manual_review"


def main() -> None:
    output_dir = ROOT / "data" / "host_review"
    output_dir.mkdir(parents=True, exist_ok=True)

    with app.app_context():
        db = get_db()
        paths = [
            Path(row[0])
            for row in db.execute(
                """
                SELECT metadata_path
                FROM species
                WHERE taxon_rank = 'genus'
                  AND metadata_status = 'ready'
                  AND metadata_path IS NOT NULL
                """
            ).fetchall()
        ]

    files = 0
    total_rows = 0
    raw_host_present = 0
    raw_host_missing = 0
    stored_host_mapped = 0
    recomputed_host_mapped = 0
    recomputed_host_taxid = 0
    file_errors: list[tuple[str, str]] = []
    method_counts: Counter[str] = Counter()
    confidence_counts: Counter[str] = Counter()
    raw_host_counts: Counter[str] = Counter()
    unmapped_counts: Counter[str] = Counter()
    non_host_counts: Counter[str] = Counter()
    unresolved_class_counts: Counter[str] = Counter()
    mapped_host_counts: Counter[tuple[str, str]] = Counter()

    for path in paths:
        if not path.exists():
            file_errors.append((str(path), "missing_file"))
            continue
        try:
            frame = pd.read_csv(path, sep="\t", dtype=str, low_memory=False)
        except Exception as exc:
            file_errors.append((str(path), str(exc)))
            continue

        files += 1
        total_rows += len(frame)
        for column in ["Host", "Host_SD", "Host_TaxID", "Host_SD_Method", "Host_SD_Confidence"]:
            if column not in frame.columns:
                frame[column] = ""

        host_values = frame["Host"].fillna("").astype(str).str.strip()
        raw_host_counts.update(host_values.value_counts(dropna=False).to_dict())
        unique_standardized = {value: standardize_host_metadata(value) for value in host_values.unique()}

        for value, count in host_values.value_counts(dropna=False).items():
            value = "" if value is None else str(value).strip()
            count = int(count)
            standardized = unique_standardized[value]
            method = standardized.get("Host_SD_Method") or ""
            confidence = standardized.get("Host_SD_Confidence") or ""
            taxid = standardized.get("Host_TaxID") or ""
            host_sd = standardized.get("Host_SD") or ""
            method_counts[method] += count
            confidence_counts[confidence] += count

            if metadata_value_is_missing(value):
                raw_host_missing += count
            else:
                raw_host_present += count

            if taxid:
                recomputed_host_mapped += count
                recomputed_host_taxid += count
                mapped_host_counts[(host_sd, taxid)] += count
            elif method == "non_host_source":
                non_host_counts[value] += count
            elif not metadata_value_is_missing(value):
                unmapped_counts[value] += count
                unresolved_class, _ = classify_unresolved_host(value)
                unresolved_class_counts[unresolved_class] += count

        stored_method = frame["Host_SD_Method"].fillna("").astype(str).str.strip()
        stored_confidence = frame["Host_SD_Confidence"].fillna("").astype(str).str.strip()
        stored_mapped_mask = (
            ~stored_method.isin(["", "missing", "unmapped", "non_host_source"])
            & ~stored_confidence.isin(["", "none"])
        )
        stored_host_mapped += int(stored_mapped_mask.sum())

    summary_rows = [
        ["files_scanned", files],
        ["rows_scanned", total_rows],
        ["raw_host_present", raw_host_present],
        ["raw_host_missing", raw_host_missing],
        ["stored_host_sd_mapped", stored_host_mapped],
        ["recomputed_raw_host_mapped", recomputed_host_mapped],
        ["recomputed_raw_host_with_taxid", recomputed_host_taxid],
        ["raw_present_unmapped_after_recompute", sum(unmapped_counts.values())],
        ["non_host_source_after_recompute", sum(non_host_counts.values())],
        ["file_errors", len(file_errors)],
    ]

    write_csv(output_dir / "host_audit_summary.csv", ["metric", "value"], summary_rows)
    write_csv(
        output_dir / "host_standardization_methods.csv",
        ["count", "method"],
        [[count, value] for value, count in method_counts.most_common()],
    )
    write_csv(
        output_dir / "host_standardization_confidence.csv",
        ["count", "confidence"],
        [[count, value] for value, count in confidence_counts.most_common()],
    )
    write_csv(
        output_dir / "host_raw_values.csv",
        ["count", "raw_host"],
        [[count, value] for value, count in raw_host_counts.most_common()],
    )
    write_csv(
        output_dir / "host_unmapped_review_queue.csv",
        [
            "count",
            "raw_host",
            "unresolved_class",
            "review_decision",
            "proposed_host_sd",
            "proposed_taxid",
            "review_note",
        ],
        [
            [
                count,
                value,
                classify_unresolved_host(value)[0],
                review_decision_for_class(classify_unresolved_host(value)[0]),
                "",
                "",
                classify_unresolved_host(value)[1],
            ]
            for value, count in unmapped_counts.most_common()
        ],
    )
    write_csv(
        output_dir / "host_taxonomy_name_candidates.csv",
        ["raw_host"],
        [
            [value]
            for value, _ in unmapped_counts.most_common()
            if classify_unresolved_host(value)[0] == "taxonomic_name_needing_taxid_lookup"
        ],
    )
    write_csv(
        output_dir / "host_unresolved_classification.csv",
        ["count", "unresolved_class"],
        [[count, value] for value, count in unresolved_class_counts.most_common()],
    )
    write_csv(
        output_dir / "host_non_host_source_values.csv",
        ["count", "raw_host"],
        [[count, value] for value, count in non_host_counts.most_common()],
    )
    write_csv(
        output_dir / "host_mapped_values.csv",
        ["count", "host_sd", "taxid"],
        [[count, *key] for key, count in mapped_host_counts.most_common()],
    )
    write_csv(output_dir / "host_file_errors.csv", ["file", "error"], file_errors)

    print(output_dir)
    for metric, value in summary_rows:
        print(f"{metric}\\t{value}")


if __name__ == "__main__":
    main()
