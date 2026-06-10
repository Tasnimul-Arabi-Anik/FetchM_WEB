#!/usr/bin/env python3
"""Materialize committed host-review batches into canonical CSV rule files."""

from __future__ import annotations

import argparse
import csv
import io
import re
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STANDARDIZATION_DIR = ROOT / "standardization"
REVIEW_DIR = STANDARDIZATION_DIR / "review" / "host_review"
DEFAULT_BATCHES = [
    REVIEW_DIR / "reviewer_batch_20260609.csv",
    REVIEW_DIR / "reviewer_batch_20260609_part2.csv",
    REVIEW_DIR / "reviewer_batch_20260609_part3.csv",
    REVIEW_DIR / "reviewer_batch_20260609_part4.csv",
    REVIEW_DIR / "reviewer_batch_20260610_water_lettuce.csv",
    REVIEW_DIR / "reviewer_batch_20260610_supplemental.csv",
]
HOST_SYNONYMS = STANDARDIZATION_DIR / "host_synonyms.csv"
HOST_NEGATIVE_RULES = STANDARDIZATION_DIR / "host_negative_rules.csv"
HOST_CONTEXT_RULES = STANDARDIZATION_DIR / "host_context_rules.csv"
DEFAULT_REPORT_DIR = REVIEW_DIR / "canonical_import_20260610"

APPROVED_TYPES = {"exact_host", "broad_host"}
NEGATIVE_TYPES = {"ignore", "non_host_source", "missing"}
RULE_TYPE_ALIASES = {
    "exact_or_genus_taxon": "exact_host",
    "exact_taxonomy_or_cleaned_scientific_name": "exact_host",
    "broad_taxonomy_or_common_name": "broad_host",
    "ignore_unsafe_ambiguous": "ignore",
    "non_host_source_or_material": "non_host_source",
}
MICROBIAL_SUPERKINGDOMS = {"Bacteria", "Archaea", "Viruses"}
CONTEXT_ONLY_HOSTS = {
    "fish": "fish",
    "marine invertebrate": "marine invertebrate",
    "slime mold": "slime mold",
}

HOST_SYNONYM_FIELDS = ["synonym", "canonical", "taxid", "confidence", "note"]
HOST_NEGATIVE_FIELDS = ["synonym", "decision", "note"]
HOST_CONTEXT_FIELDS = ["synonym", "context", "note"]
REPORT_FIELDS = [
    "source_batch",
    "raw_host",
    "normalized_host",
    "review_action",
    "reviewed_host",
    "resolved_taxid",
    "rank",
    "superkingdom",
    "result",
    "detail",
]


def clean(value: Any) -> str:
    return str(value or "").strip()


def normalize(value: Any) -> str:
    text = clean(value).lower()
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[_;/,|:+-]+", " ", text)
    text = re.sub(r"[^a-z0-9. ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_bool(value: Any) -> bool:
    return normalize(value) in {"1", "true", "yes", "y"}


def normalize_confidence(value: Any) -> str:
    confidence = normalize(value)
    if confidence.startswith("high"):
        return "high"
    if confidence.startswith("medium"):
        return "medium"
    if confidence.startswith("low"):
        return "low"
    return confidence


def first_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = clean(row.get(key))
        if value:
            return value
    return ""


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        lineterminator = "\r\n" if path in {HOST_SYNONYMS, HOST_NEGATIVE_RULES} else "\n"
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator=lineterminator)
        writer.writeheader()
        writer.writerows({key: row.get(key, "") for key in fieldnames} for row in rows)


def normalize_review_row(row: dict[str, str], source_batch: Path) -> dict[str, str]:
    rule_type = first_value(row, "rule_type").strip().lower()
    rule_type = RULE_TYPE_ALIASES.get(rule_type, rule_type)
    approved = parse_bool(first_value(row, "final_is_approved", "is_approved_final", "is_approved"))
    return {
        "source_batch": source_batch.name,
        "raw_host": first_value(row, "raw_host"),
        "normalized_host": normalize(first_value(row, "raw_host")),
        "approved": "true" if approved else "false",
        "rule_type": rule_type,
        "final_host": first_value(row, "final_host", "standardized_host", "proposed_host"),
        "supplied_taxid": first_value(row, "final_taxid", "taxid_final", "taxid"),
        "confidence": normalize_confidence(first_value(row, "final_confidence", "confidence_final", "confidence")),
        "reviewer_note": first_value(row, "reviewer_note", "note"),
    }


def decision_signature(row: dict[str, str]) -> tuple[str, str, str]:
    return row["approved"], row["rule_type"], normalize(row["final_host"])


def consolidate_batches(batch_paths: list[Path]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    selected: dict[str, dict[str, str]] = {}
    conflicts: list[dict[str, str]] = []
    for path in batch_paths:
        for raw_row in read_csv(path):
            row = normalize_review_row(raw_row, path)
            key = row["normalized_host"]
            if not key:
                continue
            previous = selected.get(key)
            if previous and decision_signature(previous) != decision_signature(row):
                conflicts.append(
                    {
                        **row,
                        "review_action": row["rule_type"],
                        "reviewed_host": row["final_host"],
                        "resolved_taxid": "",
                        "rank": "",
                        "superkingdom": "",
                        "result": "batch_conflict_latest_wins",
                        "detail": (
                            f"Replaced {previous['source_batch']}: "
                            f"{previous['rule_type']} -> {previous['final_host']}"
                        ),
                    }
                )
            selected[key] = row
    return list(selected.values()), conflicts


def taxonkit_name2taxid(names: list[str]) -> dict[str, str]:
    unique = list(dict.fromkeys(name for name in names if name))
    if not unique:
        return {}
    result = subprocess.run(
        ["taxonkit", "name2taxid"],
        input="\n".join(unique) + "\n",
        text=True,
        capture_output=True,
        check=False,
        timeout=180,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "TaxonKit name2taxid failed")
    lookup: dict[str, str] = {}
    ambiguous: set[str] = set()
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 2 or not parts[1].strip():
            continue
        name, taxid = parts[0].strip(), parts[1].strip()
        if name in lookup and lookup[name] != taxid:
            ambiguous.add(name)
        lookup[name] = taxid
    for name in ambiguous:
        lookup.pop(name, None)
    return lookup


def taxonkit_lineage(taxids: list[str]) -> dict[str, dict[str, str]]:
    unique = sorted({taxid for taxid in taxids if taxid.isdigit()})
    if not unique:
        return {}
    result = subprocess.run(
        ["taxonkit", "lineage", "-r"],
        input="\n".join(unique) + "\n",
        text=True,
        capture_output=True,
        check=False,
        timeout=180,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "TaxonKit lineage failed")
    output: dict[str, dict[str, str]] = {}
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        taxid, lineage_text, rank = parts[0].strip(), parts[1].strip(), parts[2].strip()
        lineage_set = set(lineage_text.split(";"))
        superkingdom = next(
            (name for name in ("Eukaryota", "Bacteria", "Archaea", "Viruses") if name in lineage_set),
            "",
        )
        output[taxid] = {
            "rank": rank,
            "superkingdom": superkingdom,
            "lineage": lineage_text,
        }
    return output


def index_rows(rows: list[dict[str, str]], key_field: str) -> tuple[list[str], dict[str, dict[str, str]]]:
    order: list[str] = []
    indexed: dict[str, dict[str, str]] = {}
    for row in rows:
        key = normalize(row.get(key_field))
        if not key:
            continue
        if key not in indexed:
            order.append(key)
        indexed[key] = row
    return order, indexed


def upsert(
    order: list[str],
    indexed: dict[str, dict[str, str]],
    key: str,
    row: dict[str, str],
) -> str:
    result = "updated" if key in indexed and indexed[key] != row else "duplicate"
    if key not in indexed:
        order.append(key)
        result = "added"
    indexed[key] = row
    return result


def remove(order: list[str], indexed: dict[str, dict[str, str]], key: str) -> bool:
    if key not in indexed:
        return False
    indexed.pop(key)
    order[:] = [item for item in order if item != key]
    return True


def ordered_rows(order: list[str], indexed: dict[str, dict[str, str]]) -> list[dict[str, str]]:
    return [indexed[key] for key in order if key in indexed]


def serialize_csv_row(fieldnames: list[str], row: dict[str, str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writerow({key: row.get(key, "") for key in fieldnames})
    return buffer.getvalue().encode("utf-8")


def write_canonical_csv_preserving(
    path: Path,
    fieldnames: list[str],
    order: list[str],
    indexed: dict[str, dict[str, str]],
) -> None:
    raw_lines = path.read_bytes().splitlines(keepends=True) if path.exists() else []
    output = [raw_lines[0]] if raw_lines else [(",".join(fieldnames) + "\n").encode("utf-8")]
    emitted: set[str] = set()
    for raw_line in raw_lines[1:]:
        values = next(csv.reader([raw_line.decode("utf-8").rstrip("\r\n")]))
        existing = dict(zip(fieldnames, values))
        key = normalize(existing.get("synonym"))
        if not key or key in emitted or key not in indexed:
            continue
        desired = indexed[key]
        if all(clean(existing.get(field)) == clean(desired.get(field)) for field in fieldnames):
            output.append(raw_line)
        else:
            output.append(serialize_csv_row(fieldnames, desired))
        emitted.add(key)
    for key in order:
        if key in indexed and key not in emitted:
            output.append(serialize_csv_row(fieldnames, indexed[key]))
            emitted.add(key)
    path.write_bytes(b"".join(output))


def report_row(
    row: dict[str, str],
    *,
    result: str,
    detail: str,
    taxid: str = "",
    lineage: dict[str, str] | None = None,
) -> dict[str, str]:
    lineage = lineage or {}
    return {
        "source_batch": row["source_batch"],
        "raw_host": row["raw_host"],
        "normalized_host": row["normalized_host"],
        "review_action": row["rule_type"],
        "reviewed_host": row["final_host"],
        "resolved_taxid": taxid,
        "rank": lineage.get("rank", ""),
        "superkingdom": lineage.get("superkingdom", ""),
        "result": result,
        "detail": detail,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", action="append", type=Path, dest="batches")
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    batch_paths = args.batches or DEFAULT_BATCHES
    decisions, batch_conflicts = consolidate_batches(batch_paths)
    approved = [
        row
        for row in decisions
        if row["approved"] == "true" and row["rule_type"] in APPROVED_TYPES
    ]
    names = [
        row["final_host"]
        for row in approved
        if normalize(row["final_host"]) not in CONTEXT_ONLY_HOSTS
    ]
    name_lookup = taxonkit_name2taxid(names)
    supplied_taxids = [row["supplied_taxid"] for row in approved if row["supplied_taxid"]]
    resolved_taxids = [name_lookup.get(row["final_host"], "") for row in approved]
    lineage_lookup = taxonkit_lineage(supplied_taxids + resolved_taxids)

    synonym_order, synonyms = index_rows(read_csv(HOST_SYNONYMS), "synonym")
    negative_order, negatives = index_rows(read_csv(HOST_NEGATIVE_RULES), "synonym")
    context_order, contexts = index_rows(read_csv(HOST_CONTEXT_RULES), "synonym")
    existing_lineages = taxonkit_lineage([clean(row.get("taxid")) for row in synonyms.values()])

    report = list(batch_conflicts)
    for row in decisions:
        key = row["normalized_host"]
        rule_type = row["rule_type"]
        approved_flag = row["approved"] == "true"
        note = row["reviewer_note"] or f"Finalized reviewed-host import from {row['source_batch']}."

        if approved_flag and rule_type in APPROVED_TYPES:
            context = CONTEXT_ONLY_HOSTS.get(normalize(row["final_host"]))
            if context:
                remove(synonym_order, synonyms, key)
                negative_result = upsert(
                    negative_order,
                    negatives,
                    key,
                    {"synonym": row["raw_host"], "decision": "not_identifiable", "note": note},
                )
                context_result = upsert(
                    context_order,
                    contexts,
                    key,
                    {"synonym": row["raw_host"], "context": context, "note": note},
                )
                report.append(
                    report_row(
                        row,
                        result="context_only",
                        detail=f"negative={negative_result}; context={context_result}",
                    )
                )
                continue

            resolved_taxid = name_lookup.get(row["final_host"], "")
            supplied_taxid = row["supplied_taxid"]
            if supplied_taxid and resolved_taxid and supplied_taxid != resolved_taxid:
                report.append(
                    report_row(
                        row,
                        result="taxid_conflict",
                        detail=f"Supplied TaxID {supplied_taxid} != TaxonKit {resolved_taxid}.",
                        taxid=resolved_taxid,
                        lineage=lineage_lookup.get(resolved_taxid),
                    )
                )
                continue
            taxid = supplied_taxid or resolved_taxid
            lineage = lineage_lookup.get(taxid, {})
            if not taxid or not lineage:
                remove(synonym_order, synonyms, key)
                result = upsert(
                    negative_order,
                    negatives,
                    key,
                    {
                        "synonym": row["raw_host"],
                        "decision": "not_identifiable",
                        "note": f"{note} TaxonKit could not validate the reviewed host.",
                    },
                )
                report.append(
                    report_row(
                        row,
                        result="unresolved_to_not_identifiable",
                        detail=result,
                    )
                )
                continue
            if lineage.get("superkingdom") in MICROBIAL_SUPERKINGDOMS:
                remove(synonym_order, synonyms, key)
                result = upsert(
                    negative_order,
                    negatives,
                    key,
                    {
                        "synonym": row["raw_host"],
                        "decision": "non_host_source",
                        "note": (
                            f"{note} Blocked from Host_SD because TaxonKit lineage is "
                            f"{lineage['superkingdom']}."
                        ),
                    },
                )
                report.append(
                    report_row(
                        row,
                        result="microbial_blocked",
                        detail=result,
                        taxid=taxid,
                        lineage=lineage,
                    )
                )
                continue

            remove(negative_order, negatives, key)
            remove(context_order, contexts, key)
            confidence = row["confidence"] or (
                "high" if lineage.get("rank") in {"species", "subspecies"} else "medium"
            )
            result = upsert(
                synonym_order,
                synonyms,
                key,
                {
                    "synonym": row["raw_host"],
                    "canonical": row["final_host"],
                    "taxid": taxid,
                    "confidence": confidence,
                    "note": note,
                },
            )
            report.append(
                report_row(
                    row,
                    result=f"host_synonym_{result}",
                    detail="Validated non-microbial NCBI taxonomy.",
                    taxid=taxid,
                    lineage=lineage,
                )
            )
            continue

        if approved_flag or rule_type not in NEGATIVE_TYPES:
            report.append(
                report_row(
                    row,
                    result="invalid_decision",
                    detail="Approval flag and rule type disagree or rule type is unsupported.",
                )
            )
            continue

        decision = "not_identifiable" if rule_type == "ignore" else rule_type
        remove(synonym_order, synonyms, key)
        remove(context_order, contexts, key)
        result = upsert(
            negative_order,
            negatives,
            key,
            {"synonym": row["raw_host"], "decision": decision, "note": note},
        )
        report.append(report_row(row, result=f"negative_{result}", detail=decision))

    existing_microbial: list[dict[str, str]] = []
    for key in synonym_order:
        row = synonyms.get(key)
        if not row:
            continue
        lineage = existing_lineages.get(clean(row.get("taxid")), {})
        if lineage.get("superkingdom") in MICROBIAL_SUPERKINGDOMS:
            existing_microbial.append(
                {
                    "synonym": row.get("synonym", ""),
                    "canonical": row.get("canonical", ""),
                    "taxid": row.get("taxid", ""),
                    "rank": lineage.get("rank", ""),
                    "superkingdom": lineage.get("superkingdom", ""),
                    "note": row.get("note", ""),
                }
            )

    args.report_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.report_dir / "import_report.csv", REPORT_FIELDS, report)
    write_csv(
        args.report_dir / "existing_microbial_host_synonyms.csv",
        ["synonym", "canonical", "taxid", "rank", "superkingdom", "note"],
        existing_microbial,
    )
    counts = Counter(row["result"] for row in report)
    summary = [
        "# Reviewed Host Canonical Import",
        "",
        f"- Batch files: {len(batch_paths)}",
        f"- Raw decision rows: {sum(len(read_csv(path)) for path in batch_paths):,}",
        f"- Unique normalized decisions: {len(decisions):,}",
        f"- Batch conflicts resolved by latest batch: {len(batch_conflicts):,}",
        f"- Existing microbial canonical synonyms flagged: {len(existing_microbial):,}",
        f"- Applied: {'yes' if args.apply else 'no (dry run)'}",
        "",
        "## Results",
        "",
    ]
    summary.extend(f"- `{key}`: {value:,}" for key, value in sorted(counts.items()))
    summary.extend(
        [
            "",
            "## Outputs",
            "",
            "- `import_report.csv`",
            "- `existing_microbial_host_synonyms.csv`",
        ]
    )
    (args.report_dir / "README.md").write_text("\n".join(summary) + "\n", encoding="utf-8")

    if args.apply:
        write_canonical_csv_preserving(
            HOST_SYNONYMS, HOST_SYNONYM_FIELDS, synonym_order, synonyms
        )
        write_canonical_csv_preserving(
            HOST_NEGATIVE_RULES, HOST_NEGATIVE_FIELDS, negative_order, negatives
        )
        write_csv(HOST_CONTEXT_RULES, HOST_CONTEXT_FIELDS, ordered_rows(context_order, contexts))

    print(f"decisions\t{len(decisions)}")
    print(f"batch_conflicts\t{len(batch_conflicts)}")
    for key, value in sorted(counts.items()):
        print(f"{key}\t{value}")
    print(f"existing_microbial_synonyms\t{len(existing_microbial)}")
    print(f"report_dir\t{args.report_dir}")
    return 1 if counts.get("invalid_decision") or counts.get("taxid_conflict") else 0


if __name__ == "__main__":
    raise SystemExit(main())
