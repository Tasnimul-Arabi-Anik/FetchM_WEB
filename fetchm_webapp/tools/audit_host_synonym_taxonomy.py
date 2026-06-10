#!/usr/bin/env python3
"""Audit and optionally demote microbial host synonym leakage."""

from __future__ import annotations

import argparse
import csv
import io
import re
import subprocess
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STANDARDIZATION_DIR = ROOT / "standardization"
REVIEW_DIR = STANDARDIZATION_DIR / "review" / "host_review"
HOST_SYNONYMS = STANDARDIZATION_DIR / "host_synonyms.csv"
HOST_NEGATIVE_RULES = STANDARDIZATION_DIR / "host_negative_rules.csv"
HOST_CONTEXT_RULES = STANDARDIZATION_DIR / "host_context_rules.csv"
HOST_MICROBIAL_ALLOWLIST = STANDARDIZATION_DIR / "host_microbial_allowlist.csv"
DEFAULT_REPORT_DIR = REVIEW_DIR / "microbial_host_audit_20260610"

MICROBIAL_SUPERKINGDOMS = {"Bacteria", "Archaea", "Viruses"}
HOST_SYNONYM_FIELDS = ["synonym", "canonical", "taxid", "confidence", "note"]
HOST_NEGATIVE_FIELDS = ["synonym", "decision", "note"]
HOST_CONTEXT_FIELDS = ["synonym", "context", "note"]
HOST_MICROBIAL_ALLOWLIST_FIELDS = [
    "synonym",
    "canonical",
    "taxid",
    "reason",
    "reviewer",
    "reviewed_at",
]
AUDIT_FIELDS = [
    "synonym",
    "canonical",
    "taxid",
    "confidence",
    "rank",
    "superkingdom",
    "lineage",
    "current_note",
    "recommended_action",
    "action_detail",
]

REVIEWED_CONTEXT_ROWS = [
    ("invertebrate", "invertebrate", "Reviewed context-only broad host label."),
    ("invertebrates", "invertebrate", "Reviewed context-only broad host label."),
    ("mudfish", "fish", "Ambiguous common fish label; preserve context only."),
    ("fish", "fish", "Reviewed broad biological context label."),
    ("Pisces", "fish", "Reviewed broad/legacy fish context label."),
    ("algea", "algae", "Reviewed typo for algae context; do not force Host_SD."),
    ("algae", "algae", "Reviewed broad algal context; do not force Host_SD."),
    ("lichen", "lichen", "Reviewed broad lichen context; do not force Host_SD."),
    ("lichens", "lichen", "Reviewed broad lichen context; do not force Host_SD."),
    ("Zooplatnkon", "zooplankton", "Reviewed typo for zooplankton context."),
    ("zooplankton", "zooplankton", "Reviewed broad zooplankton context."),
    ("bracket mold", "fungus", "Reviewed broad fungal context label."),
    ("green alga", "green algae", "Reviewed broad green algae context label."),
    ("marine invertebrate", "marine invertebrate", "Reviewed context-only broad host label."),
    ("slime mold", "slime mold", "Reviewed context-only broad host label."),
    ("white bream", "fish", "Ambiguous common fish name; preserve context only."),
]


def clean(value: Any) -> str:
    return str(value or "").strip()


def normalize(value: Any) -> str:
    text = clean(value).lower()
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[_;/,|:+-]+", " ", text)
    text = re.sub(r"[^a-z0-9. ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in fieldnames} for row in rows)


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


def serialize_csv_row(fieldnames: list[str], row: dict[str, str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writerow({field: row.get(field, "") for field in fieldnames})
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
        timeout=240,
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


def upsert(order: list[str], indexed: dict[str, dict[str, str]], key: str, row: dict[str, str]) -> str:
    if key not in indexed:
        order.append(key)
        indexed[key] = row
        return "added"
    if indexed[key] == row:
        return "duplicate"
    indexed[key] = row
    return "updated"


def canonical_mismatch(row: dict[str, str], lineage: dict[str, str]) -> bool:
    if lineage.get("superkingdom") not in MICROBIAL_SUPERKINGDOMS:
        return False
    label = f"{row.get('synonym', '')} {row.get('canonical', '')}".lower()
    eukaryotic_hints = {
        "beetle",
        "scarabaeoidea",
        "fish",
        "bird",
        "mammal",
        "plant",
        "alga",
        "algae",
        "fungus",
        "mold",
        "insect",
        "sponge",
        "coral",
        "worm",
        "tick",
        "fly",
        "mosquito",
    }
    return any(hint in label for hint in eukaryotic_hints)


def audit_rows(
    synonyms: dict[str, dict[str, str]],
    lineages: dict[str, dict[str, str]],
    allowlist_keys: set[str],
) -> list[dict[str, str]]:
    audit: list[dict[str, str]] = []
    for key, row in synonyms.items():
        taxid = clean(row.get("taxid"))
        lineage = lineages.get(taxid, {})
        superkingdom = lineage.get("superkingdom", "")
        action = "keep"
        detail = "TaxID lineage is not bacterial/archaeal/viral."
        if superkingdom in MICROBIAL_SUPERKINGDOMS:
            if key in allowlist_keys:
                action = "keep_allowlisted_microbial"
                detail = "Microbial Host_SD retained by explicit allowlist."
            elif canonical_mismatch(row, lineage):
                action = "demote_canonical_taxid_mismatch"
                detail = "Canonical/common label appears eukaryotic but TaxID lineage is microbial."
            else:
                action = "demote_microbial_taxon_in_host_field"
                detail = "Bacterial/archaeal/viral lineage is not a default Host_SD."
        audit.append(
            {
                "synonym": row.get("synonym", ""),
                "canonical": row.get("canonical", ""),
                "taxid": taxid,
                "confidence": row.get("confidence", ""),
                "rank": lineage.get("rank", ""),
                "superkingdom": superkingdom,
                "lineage": lineage.get("lineage", ""),
                "current_note": row.get("note", ""),
                "recommended_action": action,
                "action_detail": detail,
            }
        )
    return audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    synonym_order, synonyms = index_rows(read_csv(HOST_SYNONYMS), "synonym")
    negative_order, negatives = index_rows(read_csv(HOST_NEGATIVE_RULES), "synonym")
    context_order, contexts = index_rows(read_csv(HOST_CONTEXT_RULES), "synonym")
    allowlist_order, allowlist = index_rows(read_csv(HOST_MICROBIAL_ALLOWLIST), "synonym")

    lineages = taxonkit_lineage([clean(row.get("taxid")) for row in synonyms.values()])
    audit = audit_rows(synonyms, lineages, set(allowlist))

    demoted = 0
    context_added = 0
    if args.apply:
        for row in audit:
            key = normalize(row["synonym"])
            if not row["recommended_action"].startswith("demote_"):
                continue
            old = synonyms.pop(key, None)
            if not old:
                continue
            synonym_order[:] = [item for item in synonym_order if item != key]
            decision = "non_host_source"
            note = (
                f"Microbial host audit 2026-06-10: {row['action_detail']} "
                f"Previous Host_SD={row['canonical']} TaxID={row['taxid']} "
                f"rank={row['rank']} superkingdom={row['superkingdom']}. "
                f"Previous note: {row['current_note']}"
            )
            upsert(
                negative_order,
                negatives,
                key,
                {"synonym": old.get("synonym", ""), "decision": decision, "note": note},
            )
            demoted += 1

        for synonym, context, note in REVIEWED_CONTEXT_ROWS:
            key = normalize(synonym)
            result = upsert(
                context_order,
                contexts,
                key,
                {"synonym": synonym, "context": context, "note": note},
            )
            if result == "added":
                context_added += 1
            # Context-only labels should not produce Host_SD unless explicitly taxonomic.
            if key in {"algea", "algae", "fish", "invertebrate", "invertebrates", "zooplatnkon", "zooplankton", "lichen", "lichens", "mudfish", "bracket mold", "green alga", "white bream", "marine invertebrate", "slime mold"}:
                synonyms.pop(key, None)
                synonym_order[:] = [item for item in synonym_order if item != key]
                negatives.setdefault(
                    key,
                    {
                        "synonym": synonym,
                        "decision": "not_identifiable",
                        "note": f"{note} Preserve Host_Context_SD without forcing Host_SD.",
                    },
                )
                if key not in negative_order:
                    negative_order.append(key)

        if not HOST_MICROBIAL_ALLOWLIST.exists():
            write_csv(HOST_MICROBIAL_ALLOWLIST, HOST_MICROBIAL_ALLOWLIST_FIELDS, [])
        write_canonical_csv_preserving(HOST_SYNONYMS, HOST_SYNONYM_FIELDS, synonym_order, synonyms)
        write_canonical_csv_preserving(HOST_NEGATIVE_RULES, HOST_NEGATIVE_FIELDS, negative_order, negatives)
        write_csv(HOST_CONTEXT_RULES, HOST_CONTEXT_FIELDS, [contexts[key] for key in context_order if key in contexts])

    args.report_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.report_dir / "host_synonyms_taxonomy_audit.csv", AUDIT_FIELDS, audit)
    remaining = [
        row for row in audit
        if row["superkingdom"] in MICROBIAL_SUPERKINGDOMS
        and row["recommended_action"] != "keep_allowlisted_microbial"
    ]
    summary = [
        "# Host Synonym Taxonomy Audit",
        "",
        f"- Host synonym rows audited: {len(audit):,}",
        f"- Non-allowlisted microbial rows found before apply: {len(remaining):,}",
        f"- Rows demoted on this run: {demoted:,}",
        f"- Context rows added on this run: {context_added:,}",
        f"- Applied: {'yes' if args.apply else 'no (dry run)'}",
        "",
        "Policy: Bacteria, Archaea, and Viruses do not populate `Host_SD` by default. Eukaryotic algae, fungi, protists, plants, and animals are preserved when TaxID lineage supports them.",
    ]
    (args.report_dir / "README.md").write_text("\n".join(summary) + "\n", encoding="utf-8")

    print(f"audited\t{len(audit)}")
    print(f"non_allowlisted_microbial_before_apply\t{len(remaining)}")
    print(f"demoted\t{demoted}")
    print(f"context_added\t{context_added}")
    print(f"report_dir\t{args.report_dir}")
    return 1 if remaining and not args.apply else 0


if __name__ == "__main__":
    raise SystemExit(main())
