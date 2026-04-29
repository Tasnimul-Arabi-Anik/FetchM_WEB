from __future__ import annotations

import argparse
import csv
import difflib
import re
import subprocess
import sys
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any

try:
    from rapidfuzz import fuzz, process
except Exception:  # pragma: no cover - fallback keeps the review tool usable.
    fuzz = None
    process = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import (  # noqa: E402
    ENVIRONMENT_MEDIUM_SYNONYMS,
    HOST_SYNONYMS,
    HOST_CONTEXT_SOURCE_SKIP_TERMS,
    HOST_CONTEXT_SYNONYMS,
    HOST_BROAD_SYNONYMS,
    NON_HOST_SOURCE_HINTS,
    NON_HOST_SOURCE_PATTERN,
    SAMPLE_TYPE_SYNONYMS,
    app,
    clean_host_lookup_text,
    get_db,
    metadata_value_is_missing,
    normalize_standardization_lookup,
    standardization_lookup_variants,
    standardize_metadata_concept,
)


CONTEXT_COLUMNS = [
    "Isolation Source",
    "Isolation Site",
    "Sample Type",
    "Environment Medium",
    "Environment (Broad Scale)",
    "Environment (Local Scale)",
]

HOST_RECOVERY_COLUMNS = [
    "Host",
    "BioSample Host",
    "BioSample Specific Host",
    "BioSample NAT Host",
    "BioSample LAB Host",
    "BioSample Host Common Name",
    "BioSample Common Name",
    "Isolation Source",
    "Isolation Site",
    "Sample Type",
    "Environment Medium",
    "Environment (Broad Scale)",
    "Environment (Local Scale)",
]

FUZZY_MIN_SCORE = 94
FUZZY_REVIEW_MIN_SCORE = 88

MISSING_EXACT = {
    "no collected",
    "not collected",
    "not collect",
    "not applicable",
    "not available",
    "not determined",
    "not provided",
    "unidentified",
    "unknown host",
    "unknown",
}

ENVIRONMENT_HINTS = re.compile(
    r"\b(soil|soild|sediment|sludge|water|seawater|freshwater|groundwater|wastewater|pond|river|lake|stream|canal|estuary|marine|surface)\b",
    re.IGNORECASE,
)
SAMPLE_HINTS = re.compile(
    r"\b(milk|dairy|food|meat|seafood|blood|gut|intestinal|feces|faeces|stool|manure|culture|swab|urine)\b",
    re.IGNORECASE,
)
NON_HOST_ADMIN_HINTS = re.compile(r"\b(laboratory|collection center|culture collection|in vitro|isolation source)\b", re.IGNORECASE)


def normalize_missing_candidate(value: Any) -> bool:
    text = "" if value is None else str(value).strip()
    if metadata_value_is_missing(text):
        return True
    normalized = normalize_standardization_lookup(text)
    if normalized in MISSING_EXACT:
        return True
    return bool(re.match(r"^\d+\s*(not applicable|not available|not collected|unknown)\b", normalized))


def non_empty(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if not text or metadata_value_is_missing(text):
        return ""
    return text


@lru_cache(maxsize=100_000)
def environment_hint_mapping(value: str) -> tuple[str, str, str, str]:
    text = clean_host_lookup_text(value)
    if not ENVIRONMENT_HINTS.search(text):
        return "", "", "", ""
    if "soil" in text or "soild" in text:
        return "Environment_Medium_SD", "soil", "ENVO:00001998", "hint"
    if "seawater" in text or "sea water" in text:
        return "Environment_Medium_SD", "seawater", "ENVO:00002149", "hint"
    if "freshwater" in text or "fresh water" in text:
        return "Environment_Medium_SD", "freshwater", "ENVO:00002011", "hint"
    if "groundwater" in text:
        return "Environment_Medium_SD", "groundwater", "ENVO:01001004", "hint"
    if "sediment" in text:
        return "Environment_Medium_SD", "sediment", "ENVO:00002007", "hint"
    return "Environment_Medium_SD", value, "", "hint"


@lru_cache(maxsize=200_000)
def best_standardized_destination(value: str, source_column: str = "") -> tuple[str, str, str, str]:
    env_value, env_method, env_ontology = standardize_metadata_concept(value, ENVIRONMENT_MEDIUM_SYNONYMS)
    sample_value, sample_method, sample_ontology = standardize_metadata_concept(value, SAMPLE_TYPE_SYNONYMS)

    env_matched = bool(env_value and env_method != "original")
    sample_matched = bool(sample_value and sample_method != "original")
    text = clean_host_lookup_text(value)
    if source_column.startswith("Environment"):
        if env_matched:
            return "Environment_Medium_SD", env_value, env_ontology, env_method
        environment_hint = environment_hint_mapping(value)
        if environment_hint[0]:
            return environment_hint

    if env_matched and not sample_matched:
        return "Environment_Medium_SD", env_value, env_ontology, env_method
    if sample_matched and not env_matched:
        return "Sample_Type_SD", sample_value, sample_ontology, sample_method
    if env_matched and sample_matched:
        if ENVIRONMENT_HINTS.search(text) and not SAMPLE_HINTS.search(text):
            return "Environment_Medium_SD", env_value, env_ontology, env_method
        return "Sample_Type_SD", sample_value, sample_ontology, sample_method

    if ENVIRONMENT_HINTS.search(text):
        return environment_hint_mapping(value)

    if SAMPLE_HINTS.search(text):
        if "milk" in text or "dairy" in text:
            return "Sample_Type_SD", "milk" if "milk" in text else "dairy product", "", "hint"
        if "gut" in text or "intestinal" in text:
            return "Sample_Type_SD", "gut content", "", "hint"
        if "blood" in text:
            return "Sample_Type_SD", "blood", "UBERON:0000178", "hint"
        if "culture" in text:
            return "Sample_Type_SD", "culture", "", "hint"
        return "Sample_Type_SD", value, "", "hint"

    if NON_HOST_ADMIN_HINTS.search(text):
        return "Isolation_Source_SD", value, "", "admin_hint"

    return "", "", "", ""


@lru_cache(maxsize=100_000)
def is_non_host_source_value(value: Any) -> bool:
    cleaned = clean_host_lookup_text(value)
    return bool(cleaned and (cleaned in NON_HOST_SOURCE_HINTS or NON_HOST_SOURCE_PATTERN.search(cleaned)))


@lru_cache(maxsize=100_000)
def host_needs_context_recovery(value: Any) -> bool:
    if metadata_value_is_missing(value) or normalize_missing_candidate(value):
        return True
    cleaned = clean_host_lookup_text(value)
    if not cleaned:
        return True
    if is_non_host_source_value(cleaned):
        return True
    for candidate in standardization_lookup_variants(value):
        if candidate in HOST_SYNONYMS:
            return False
    if cleaned in HOST_CONTEXT_SYNONYMS or cleaned in HOST_BROAD_SYNONYMS:
        return False
    return True


def context_candidate_values(row: dict[str, str]) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for column in HOST_RECOVERY_COLUMNS:
        value = non_empty(row.get(column))
        if not value:
            continue
        key = (column, value)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(key)
    return candidates


@lru_cache(maxsize=200_000)
def reviewed_host_match(value: Any) -> tuple[str, str, str]:
    for candidate in standardization_lookup_variants(value):
        if candidate in HOST_SYNONYMS:
            name, taxid = HOST_SYNONYMS[candidate]
            return name, taxid, "dictionary"
    cleaned = clean_host_lookup_text(value)
    for key, (name, taxid) in HOST_CONTEXT_SYNONYMS.items():
        if re.search(rf"(^|\s){re.escape(key)}(\s|$)", cleaned):
            return name, taxid, "context_dictionary"
    for key, (name, taxid) in HOST_BROAD_SYNONYMS.items():
        if re.search(rf"(^|\s){re.escape(key)}(\s|$)", cleaned):
            return name, taxid, "broad_dictionary"
    return "", "", ""


@lru_cache(maxsize=100_000)
def is_context_source_skip(column: str, value: str) -> bool:
    cleaned = clean_host_lookup_text(value)
    if column == "Isolation Source" and cleaned in HOST_CONTEXT_SOURCE_SKIP_TERMS:
        return True
    return bool(re.search(r"\b(product|meat|carcass|food|seafood|shellfish|shrimp|prawn|oyster)\b", cleaned))


def context_evidence(row: dict[str, str]) -> tuple[str, str]:
    parts: list[str] = []
    values: list[str] = []
    for column in CONTEXT_COLUMNS:
        value = non_empty(row.get(column))
        if not value:
            continue
        parts.append(f"{column}={value}")
        values.append(value)
    return " | ".join(parts[:6]), " ".join(values)


def load_unresolved_hosts(path: Path) -> set[str]:
    unresolved: set[str] = set()
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            value = (row.get("raw_host") or "").strip()
            if value:
                unresolved.add(value)
    return unresolved


def host_reference_terms() -> dict[str, tuple[str, str]]:
    references: dict[str, tuple[str, str]] = {}
    for synonym, (canonical, taxid) in HOST_SYNONYMS.items():
        normalized = normalize_standardization_lookup(synonym)
        if normalized and taxid:
            references[normalized] = (canonical, taxid)
        canonical_key = normalize_standardization_lookup(canonical)
        if canonical_key and taxid:
            references[canonical_key] = (canonical, taxid)
    return references


def fuzzy_host_match(cleaned: str, references: dict[str, tuple[str, str]]) -> tuple[str, str, str, int]:
    if not cleaned or len(cleaned) < 5 or not references:
        return "", "", "", 0
    choices = list(references.keys())
    if process is not None and fuzz is not None:
        # Use plain edit-distance ratio, not WRatio/partial matching. Partial
        # matching is useful for search, but it creates unsafe biological
        # suggestions such as "marmot" -> "Marmota himalayana".
        result = process.extractOne(cleaned, choices, scorer=fuzz.ratio)
        if result is None:
            return "", "", "", 0
        match, score, _ = result
        canonical, taxid = references[match]
        return match, canonical, taxid, int(round(score))

    match = difflib.get_close_matches(cleaned, choices, n=1, cutoff=FUZZY_REVIEW_MIN_SCORE / 100)
    if not match:
        return "", "", "", 0
    candidate = match[0]
    score = int(round(difflib.SequenceMatcher(None, cleaned, candidate).ratio() * 100))
    canonical, taxid = references[candidate]
    return candidate, canonical, taxid, score


def resolve_taxonkit_names(values: list[str]) -> dict[str, str]:
    """Resolve exact input names with local NCBI Taxonomy via taxonkit.

    Ambiguous names are intentionally skipped. Canonical naming is left for
    reviewed synonym rules; this step only confirms that the raw value is an
    exact NCBI-recognized host taxon before fuzzy matching is considered.
    """
    unique_values = [value.strip() for value in dict.fromkeys(values) if value and value.strip()]
    if not unique_values:
        return {}
    try:
        result = subprocess.run(
            ["taxonkit", "name2taxid"],
            input="\n".join(unique_values) + "\n",
            text=True,
            capture_output=True,
            check=False,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if result.returncode != 0 or not result.stdout.strip():
        return {}

    name_taxids: dict[str, set[str]] = defaultdict(set)
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        name = parts[0].strip()
        taxid = parts[1].strip()
        if name and taxid.isdigit():
            name_taxids[name].add(taxid)
    return {name: next(iter(taxids)) for name, taxids in name_taxids.items() if len(taxids) == 1}


def build_suggestions(
    unresolved_path: Path,
    output_dir: Path,
    limit: int | None = None,
    max_files: int | None = None,
    max_rows: int | None = None,
) -> dict[str, int]:
    unresolved_hosts = load_unresolved_hosts(unresolved_path)
    if limit:
        unresolved_hosts = set(list(unresolved_hosts)[:limit])

    context_counts: dict[str, Counter[str]] = defaultdict(Counter)
    context_examples: dict[str, str] = {}
    context_value_counts: Counter[tuple[str, str, str]] = Counter()
    context_value_examples: dict[tuple[str, str, str], str] = {}
    raw_counts: Counter[str] = Counter()
    fuzzy_references = host_reference_terms()

    with app.app_context():
        paths = [
            Path(row[0])
            for row in get_db()
            .execute(
                """
                SELECT metadata_path
                FROM species
                WHERE taxon_rank = 'genus'
                  AND metadata_status = 'ready'
                  AND metadata_path IS NOT NULL
                """
            )
            .fetchall()
        ]

    files_scanned = 0
    rows_scanned = 0
    for path in paths:
        if max_files is not None and files_scanned >= max_files:
            break
        if not path.exists():
            continue
        files_scanned += 1
        with path.open(newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                if max_rows is not None and rows_scanned >= max_rows:
                    break
                rows_scanned += 1
                host_raw = "" if row.get("Host") is None else str(row.get("Host")).strip()
                host = non_empty(row.get("Host"))
                if host in unresolved_hosts:
                    raw_counts[host] += 1
                if not host_needs_context_recovery(host_raw):
                    continue
                evidence, context_text = context_evidence(row)
                if host in unresolved_hosts and evidence and host not in context_examples:
                    context_examples[host] = evidence
                recovery_state = "missing_host" if metadata_value_is_missing(host_raw) or normalize_missing_candidate(host_raw) else "ambiguous_or_non_host"
                for column, value in context_candidate_values(row):
                    if column == "Host":
                        continue
                    if is_context_source_skip(column, value):
                        continue
                    key = (recovery_state, column, value)
                    context_value_counts[key] += 1
                    if evidence and key not in context_value_examples:
                        context_value_examples[key] = evidence
                if context_text:
                    if host in unresolved_hosts:
                        context_counts[host][context_text] += 1
        if max_rows is not None and rows_scanned >= max_rows:
            break

    rows: list[dict[str, Any]] = []
    direct_taxids = resolve_taxonkit_names(list(raw_counts))
    for raw_host, count in raw_counts.most_common():
        cleaned = clean_host_lookup_text(raw_host)
        evidence = context_examples.get(raw_host, "")
        context_blob = " ".join(context_counts[raw_host].keys())

        suggested_decision = "needs_manual_review"
        destination = ""
        suggested_value = ""
        suggested_taxid = ""
        ontology_id = ""
        confidence = "low"
        reason = "Unresolved host value needs manual review."
        fuzzy_score = 0

        if normalize_missing_candidate(raw_host):
            suggested_decision = "missing"
            confidence = "high"
            reason = "Missing/not-collected host variant."
        else:
            if cleaned in HOST_SYNONYMS:
                host_name, taxid = HOST_SYNONYMS[cleaned]
                suggested_decision = "approve_host"
                destination = "Host_SD"
                suggested_value = host_name
                suggested_taxid = taxid
                confidence = "high"
                reason = "Exact reviewed host synonym match with TaxID."
            elif raw_host in direct_taxids:
                suggested_decision = "approve_host_taxonomy"
                destination = "Host_SD"
                suggested_value = raw_host
                suggested_taxid = direct_taxids[raw_host]
                confidence = "high"
                reason = "Direct local NCBI Taxonomy/TaxonKit name2taxid match before fuzzy matching."
            else:
                fuzzy_match, fuzzy_canonical, fuzzy_taxid, fuzzy_score = fuzzy_host_match(cleaned, fuzzy_references)
                if fuzzy_score >= FUZZY_MIN_SCORE:
                    suggested_decision = "approve_host_fuzzy"
                    destination = "Host_SD"
                    suggested_value = fuzzy_canonical
                    suggested_taxid = fuzzy_taxid
                    confidence = "high"
                    reason = f"High-confidence fuzzy typo match to NCBI-validated reference `{fuzzy_match}` with score {fuzzy_score}; review before adding synonym."
                elif fuzzy_score >= FUZZY_REVIEW_MIN_SCORE:
                    suggested_decision = "fuzzy_review"
                    destination = "Host_SD"
                    suggested_value = fuzzy_canonical
                    suggested_taxid = fuzzy_taxid
                    confidence = "medium"
                    reason = f"Possible fuzzy host match to NCBI-validated reference `{fuzzy_match}` with score {fuzzy_score}; manual review required."
                else:
                    fuzzy_score = 0

        if suggested_decision in {"needs_manual_review"}:
            destination, suggested_value, ontology_id, method = best_standardized_destination(raw_host)
            if destination:
                suggested_decision = "non_host_source"
                confidence = "high" if method in {"dictionary", "context_dictionary"} else "medium"
                if context_blob and (ENVIRONMENT_HINTS.search(context_blob) or SAMPLE_HINTS.search(context_blob)):
                    confidence = "high"
                reason = "Host field appears to contain source/sample/environment text; keep Host unchanged and route standardized value to destination field."
            elif len(cleaned.split()) >= 2:
                suggested_decision = "taxonomy_lookup"
                destination = "Host_SD"
                confidence = "medium"
                reason = "Likely taxonomic/common-name phrase; resolve with TaxonKit/NCBI or manual review before approval."

        rows.append(
            {
                "raw_host": raw_host,
                "count": count,
                "suggested_decision": suggested_decision,
                "suggested_destination": destination,
                "suggested_value": suggested_value,
                "suggested_taxid": suggested_taxid,
                "ontology_id": ontology_id,
                "confidence": confidence,
                "evidence_columns": evidence,
                "reason": reason,
                "fuzzy_score": fuzzy_score,
                "ai_model_stage": "rules_context_first; BGE-small optional for ambiguous_free_text review",
            }
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "host_context_refinement_suggestions.csv"
    fieldnames = [
        "raw_host",
        "count",
        "suggested_decision",
        "suggested_destination",
        "suggested_value",
        "suggested_taxid",
        "ontology_id",
        "confidence",
        "evidence_columns",
        "reason",
        "fuzzy_score",
        "ai_model_stage",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    recovery_rows: list[dict[str, Any]] = []
    for (state, column, value), count in context_value_counts.most_common():
        destination, routed_value, ontology_id, method = best_standardized_destination(value, column)
        if destination:
            recovery_rows.append(
                {
                    "host_state": state,
                    "source_column": column,
                    "source_value": value,
                    "suggested_destination": destination,
                    "suggested_value": routed_value,
                    "count": count,
                    "evidence_columns": context_value_examples.get((state, column, value), ""),
                    "review_note": "Context-aware source/sample/environment routing candidate; review before adding a permanent rule.",
                }
            )
            continue
        name, taxid, match_method = reviewed_host_match(value)
        if name and taxid:
            recovery_rows.append(
                {
                    "host_state": state,
                    "source_column": column,
                    "source_value": value,
                    "suggested_destination": "Host_SD",
                    "suggested_value": name,
                    "count": count,
                    "evidence_columns": context_value_examples.get((state, column, value), ""),
                    "review_note": f"Context-aware host recovery candidate by {match_method}; review before adding a permanent rule.",
                }
            )
    recovery_path = output_dir / "host_context_recovery_candidates.csv"
    with recovery_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "host_state",
                "source_column",
                "source_value",
                "suggested_destination",
                "suggested_value",
                "count",
                "evidence_columns",
                "review_note",
            ],
        )
        writer.writeheader()
        writer.writerows(recovery_rows)

    decision_counts = Counter(str(row["suggested_decision"]) for row in rows)
    summary_path = output_dir / "host_context_refinement_summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        writer.writerow(["files_scanned", files_scanned])
        writer.writerow(["rows_scanned", rows_scanned])
        writer.writerow(["unresolved_values_in_scope", len(unresolved_hosts)])
        writer.writerow(["suggested_values_found", len(rows)])
        writer.writerow(["rows_represented", sum(int(row["count"]) for row in rows)])
        writer.writerow(["context_recovery_candidates", len(recovery_rows)])
        writer.writerow(["context_recovery_rows_represented", sum(int(row["count"]) for row in recovery_rows)])
        for decision, decision_count in decision_counts.most_common():
            writer.writerow([f"decision_{decision}", decision_count])

    return {
        "files_scanned": files_scanned,
        "rows_scanned": rows_scanned,
        "suggested_values_found": len(rows),
        "rows_represented": sum(int(row["count"]) for row in rows),
        "context_recovery_candidates": len(recovery_rows),
        "context_recovery_rows_represented": sum(int(row["count"]) for row in recovery_rows),
        **{f"decision_{key}": value for key, value in decision_counts.items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build context-aware review suggestions for unresolved Host values.")
    parser.add_argument(
        "--unresolved",
        type=Path,
        default=ROOT / "standardization" / "review" / "host_review" / "host_unmapped_review_queue.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "standardization" / "review" / "host_review" / "context_refinement",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-files", type=int, default=None, help="Optional smoke-test cap on metadata files scanned.")
    parser.add_argument("--max-rows", type=int, default=None, help="Optional smoke-test cap on metadata rows scanned.")
    args = parser.parse_args()
    summary = build_suggestions(args.unresolved, args.output_dir, args.limit, args.max_files, args.max_rows)
    for key, value in summary.items():
        print(f"{key}\t{value}")
    print(f"output\t{args.output_dir / 'host_context_refinement_suggestions.csv'}")


if __name__ == "__main__":
    main()
