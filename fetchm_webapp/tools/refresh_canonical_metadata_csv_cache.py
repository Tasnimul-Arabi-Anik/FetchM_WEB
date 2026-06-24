#!/usr/bin/env python3
"""Refresh a canonical metadata CSV cache without importing the Flask app.

The normal web-app writer imports the full plotting/application stack. For the
3.13M-row bacterial root report that can exceed the web container memory limit.
This tool reads the canonical CSV column definitions from app.py with AST,
streams canonical rows from PostgreSQL, and atomically replaces the cache file.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import sys
import re
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))
APP_PATH = BASE_DIR / "app.py"
CANONICAL_REPORTS_DIR = BASE_DIR / "data" / "canonical_metadata_reports"

RANK_COLUMNS = {
    "domain": "domain_name",
    "phylum": "phylum_name",
    "class": "class_name",
    "order": "order_name",
    "family": "family_name",
    "genus": "genus_name",
    "species": "species_name",
}


def normalize_metadata_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "none", "nan"} else text


def normalize_species_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def literal_assignment(tree: ast.Module, name: str) -> Any:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            return ast.literal_eval(node.value)
    raise KeyError(f"Assignment not found: {name}")


def canonical_csv_columns() -> list[str]:
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"), filename=str(APP_PATH))
    species_columns = literal_assignment(tree, "SPECIES_TSV_COLUMNS")
    extra_columns = literal_assignment(tree, "CANONICAL_METADATA_EXTRA_COLUMNS")
    return list(dict.fromkeys(list(species_columns) + list(extra_columns)))


def cache_key(snapshot_id: str, rule_fingerprint: str, rank: str, name: str, include_provisional: bool) -> str:
    payload = json.dumps(
        {
            "snapshot_id": snapshot_id,
            "rule_fingerprint": rule_fingerprint,
            "rank": rank,
            "name": normalize_species_name(name),
            "include_provisional": bool(include_provisional),
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def output_paths(snapshot_id: str, rule_fingerprint: str, rank: str, name: str) -> tuple[str, Path]:
    key = cache_key(snapshot_id, rule_fingerprint, rank, name, True)
    return key, CANONICAL_REPORTS_DIR / key / "metadata_clean.csv"


def payload_row(payload: Mapping[str, Any], lineage: Mapping[str, Any], columns: Iterable[str]) -> dict[str, str]:
    row = {column: normalize_metadata_value(payload.get(column)) for column in columns}
    row["Assembly Accession"] = normalize_metadata_value(payload.get("Assembly Accession") or lineage.get("assembly_accession"))
    row["Organism Name"] = normalize_metadata_value(payload.get("Organism Name") or lineage.get("reported_name"))
    row["Taxonomy Domain"] = normalize_metadata_value(lineage.get("domain_name"))
    row["Taxonomy Phylum"] = normalize_metadata_value(lineage.get("phylum_name"))
    row["Taxonomy Class"] = normalize_metadata_value(lineage.get("class_name"))
    row["Taxonomy Order"] = normalize_metadata_value(lineage.get("order_name"))
    row["Taxonomy Family"] = normalize_metadata_value(lineage.get("family_name"))
    row["Taxonomy Genus"] = normalize_metadata_value(lineage.get("genus_name"))
    row["Taxonomy Species"] = normalize_metadata_value(lineage.get("species_name"))
    row["Taxonomy Source"] = normalize_metadata_value(lineage.get("taxonomy_source"))
    row["Taxonomy Resolution Status"] = normalize_metadata_value(lineage.get("resolution_status"))
    return row


def refresh_cache(
    *,
    snapshot_id: str,
    rule_fingerprint: str,
    rank: str,
    name: str,
    output_path: Path,
    ordered: bool,
    batch_size: int,
    limit: int | None = None,
) -> dict[str, Any]:
    from dataset_production_store import connect

    if rank not in RANK_COLUMNS:
        raise SystemExit(f"Unsupported rank: {rank}")
    columns = canonical_csv_columns()
    rank_column = RANK_COLUMNS[rank]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(f".{uuid.uuid4().hex}.tmp")
    order_clause = "ORDER BY l.assembly_accession" if ordered else ""
    limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""
    query = f"""
        SELECT l.assembly_accession, l.reported_name, l.domain_name, l.phylum_name, l.class_name,
               l.order_name, l.family_name, l.genus_name, l.species_name, l.taxonomy_source,
               l.resolution_status, s.standardized_payload
        FROM assembly_taxonomy_lineage l
        JOIN assembly_standardization s ON s.assembly_accession = l.assembly_accession
        WHERE l.snapshot_id = %s AND l.{rank_column} = %s
        {order_clause}
        {limit_clause}
    """
    total = 0
    try:
        with connect() as connection, temp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            with connection.cursor(name=f"canonical_csv_{uuid.uuid4().hex}") as cursor:
                cursor.execute(query, (snapshot_id, normalize_species_name(name)))
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    for db_row in rows:
                        lineage = {
                            "assembly_accession": db_row[0],
                            "reported_name": db_row[1],
                            "domain_name": db_row[2],
                            "phylum_name": db_row[3],
                            "class_name": db_row[4],
                            "order_name": db_row[5],
                            "family_name": db_row[6],
                            "genus_name": db_row[7],
                            "species_name": db_row[8],
                            "taxonomy_source": db_row[9],
                            "resolution_status": db_row[10],
                        }
                        writer.writerow(payload_row(db_row[11] or {}, lineage, columns))
                        total += 1
        temp_path.replace(output_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    return {
        "snapshot_id": snapshot_id,
        "rule_fingerprint": rule_fingerprint,
        "rank": rank,
        "name": name,
        "ordered": ordered,
        "rows_written": total,
        "metadata_csv": str(output_path),
        "columns": len(columns),
        "has_sample_material": "Sample_Material_SD" in columns,
        "has_sampling_context": "Sampling_Context_SD" in columns,
        "limit": limit,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--rule-fingerprint", required=True)
    parser.add_argument("--rank", default="domain", choices=sorted(RANK_COLUMNS))
    parser.add_argument("--name", default="Bacteria")
    parser.add_argument("--output")
    parser.add_argument("--ordered", action="store_true", help="Preserve accession ordering. Slower for the full bacterial root.")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--limit", type=int, help="Write at most this many rows; intended for smoke tests.")
    args = parser.parse_args(argv)
    key, default_output = output_paths(args.snapshot_id, args.rule_fingerprint, args.rank, args.name)
    output_path = Path(args.output) if args.output else default_output
    summary = refresh_cache(
        snapshot_id=args.snapshot_id,
        rule_fingerprint=args.rule_fingerprint,
        rank=args.rank,
        name=args.name,
        output_path=output_path,
        ordered=args.ordered,
        batch_size=args.batch_size,
        limit=args.limit if args.limit and args.limit > 0 else None,
    )
    summary["cache_key"] = key
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
