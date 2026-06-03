#!/usr/bin/env python3
"""Build a host-standardization validation review sample from the active canonical snapshot.

Run inside the FetchM WEB container or environment where dataset_production_store is configured:
    python tools/build_host_validation_sample.py --snapshot-id SNAPSHOT --output global_insights/host_validation_review_sample.csv
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from dataset_production_store import connect

FIELDNAMES = [
    "field",
    "validation_group",
    "assembly_accession",
    "organism_name",
    "raw_value",
    "standardized_value",
    "standardized_taxid",
    "evidence_source",
    "confidence",
    "reviewer_label",
    "reviewer_decision",
    "error_type",
    "notes",
    "host_source_value",
    "isolation_source",
    "sample_type",
    "environment_medium",
    "country",
    "collection_year",
    "snapshot_id",
]

QUERIES = [
    (
        "host_primary_high_confidence",
        """
        SELECT l.assembly_accession, s.standardized_payload
        FROM assembly_taxonomy_lineage l JOIN assembly_standardization s ON s.assembly_accession=l.assembly_accession
        WHERE l.snapshot_id=%s
          AND COALESCE(s.standardized_payload->>'Host_SD','') <> ''
          AND COALESCE(s.standardized_payload->>'Host_SD_Confidence','') = 'high'
          AND COALESCE(s.standardized_payload->>'Host_SD_Method','') IN ('dictionary','cleaned_match')
        ORDER BY md5(l.assembly_accession) LIMIT %s
        """,
    ),
    (
        "host_secondary_evidence_recovery",
        """
        SELECT l.assembly_accession, s.standardized_payload
        FROM assembly_taxonomy_lineage l JOIN assembly_standardization s ON s.assembly_accession=l.assembly_accession
        WHERE l.snapshot_id=%s
          AND COALESCE(s.standardized_payload->>'Host_SD','') <> ''
          AND COALESCE(s.standardized_payload->>'Host_SD_Method','') LIKE 'context%%'
        ORDER BY md5(l.assembly_accession) LIMIT %s
        """,
    ),
    (
        "host_unresolved_or_missing",
        """
        SELECT l.assembly_accession, s.standardized_payload
        FROM assembly_taxonomy_lineage l JOIN assembly_standardization s ON s.assembly_accession=l.assembly_accession
        WHERE l.snapshot_id=%s
          AND lower(COALESCE(s.standardized_payload->>'Host_SD','')) IN ('','absent','unknown','not collected','not applicable','not provided','not available','missing')
        ORDER BY md5(l.assembly_accession) LIMIT %s
        """,
    ),
    (
        "host_source_like_edge_case",
        """
        SELECT l.assembly_accession, s.standardized_payload
        FROM assembly_taxonomy_lineage l JOIN assembly_standardization s ON s.assembly_accession=l.assembly_accession
        WHERE l.snapshot_id=%s
          AND lower(COALESCE(s.standardized_payload->>'Host_SD','')) IN ('','absent','unknown','not collected','not applicable','not provided','not available','missing')
          AND (
            lower(COALESCE(s.standardized_payload->>'Isolation Source','')) ~ '(chicken|turkey|beef|pork|meat|carcass|food|soil|water|sediment|environment|wastewater|stool|feces|faeces|blood)' OR
            lower(COALESCE(s.standardized_payload->>'Sample Type','')) ~ '(metagenomic|culture|environment|food|blood|organism)'
          )
        ORDER BY md5(l.assembly_accession) LIMIT %s
        """,
    ),
]


def latest_active_snapshot_id() -> str:
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT snapshot_id
            FROM canonical_taxonomy_lineage_snapshot
            ORDER BY generated_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError("No canonical snapshot found.")
    return str(row[0])


def notes_for_group(group: str) -> str:
    if group == "host_unresolved_or_missing":
        return "Reviewer should confirm whether host is genuinely absent/unresolvable or recoverable from context."
    if group == "host_source_like_edge_case":
        return "Reviewer should check whether source/sample descriptor should remain non-host rather than Host_SD."
    if group == "host_secondary_evidence_recovery":
        return "Reviewer should confirm secondary evidence supports the standardized host assignment."
    return ""


def build_sample(snapshot_id: str, output: Path, per_group: int) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with connect() as conn, output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        cur = conn.cursor()
        for group, query in QUERIES:
            cur.execute(query, (snapshot_id, per_group))
            for accession, payload in cur.fetchall():
                p = payload or {}
                writer.writerow(
                    {
                        "field": "Host",
                        "validation_group": group,
                        "assembly_accession": accession,
                        "organism_name": p.get("Organism Name", ""),
                        "raw_value": p.get("Host") or p.get("Host_Original") or "",
                        "standardized_value": p.get("Host_SD", ""),
                        "standardized_taxid": p.get("Host_TaxID", ""),
                        "evidence_source": p.get("Host_SD_Method") or p.get("Host_Match_Method") or "",
                        "confidence": p.get("Host_SD_Confidence") or p.get("Host_Confidence") or "",
                        "reviewer_label": "",
                        "reviewer_decision": "",
                        "error_type": "",
                        "notes": notes_for_group(group),
                        "host_source_value": p.get("Host_Context_SD") or p.get("Host_Context_Value") or p.get("Host_Source_Value") or p.get("Isolation Source") or "",
                        "isolation_source": p.get("Isolation Source", ""),
                        "sample_type": p.get("Sample Type", ""),
                        "environment_medium": p.get("Environment Medium", ""),
                        "country": p.get("Country", ""),
                        "collection_year": p.get("Collection Date", ""),
                        "snapshot_id": snapshot_id,
                    }
                )
                count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-id", default="", help="Canonical snapshot ID. Defaults to latest canonical lineage snapshot.")
    parser.add_argument("--output", default="global_insights/host_validation_review_sample.csv")
    parser.add_argument("--per-group", type=int, default=150)
    args = parser.parse_args()
    snapshot_id = args.snapshot_id or latest_active_snapshot_id()
    count = build_sample(snapshot_id, Path(args.output), args.per_group)
    print(f"Wrote {count} host validation records to {args.output} for {snapshot_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
