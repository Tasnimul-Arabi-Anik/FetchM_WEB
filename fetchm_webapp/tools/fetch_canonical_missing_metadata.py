#!/usr/bin/env python3
"""Fetch and standardize canonical bacterial assemblies missing from existing metadata."""

from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import build_species_tsv_row, normalize_managed_metadata_row
from dataset_production_store import (
    insert_inventory_batch,
    missing_standardized_accession_batch,
    seed_standardized_metadata_batch,
    standardized_metadata_coverage,
)
from global_insights.generator import standardization_rule_manifest

API_BASE = "https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession"
ATTRIBUTE_COLUMNS = {
    "host": "Host",
    "isolation_source": "Isolation Source",
    "collection_date": "Collection Date",
    "geo_loc_name": "Geographic Location",
    "sample_type": "Sample Type",
    "env_package": "BioSample ENV Package",
    "env_broad_scale": "BioSample ENV Broad Scale",
    "env_local_scale": "BioSample ENV Local Scale",
    "env_medium": "BioSample ENV Medium",
    "env_material": "BioSample ENV Material",
    "host_disease": "BioSample Host Disease",
}


def biosample_attributes(biosample: dict[str, Any]) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for item in biosample.get("attributes", []) or []:
        if not isinstance(item, dict):
            continue
        key = str(item.get("name") or "").strip().lower()
        value = str(item.get("value") or "").strip()
        if key and value:
            attributes[key] = value
    return attributes


def standardizable_row(report: dict[str, Any]) -> dict[str, Any]:
    row = build_species_tsv_row(report)
    biosample = ((report.get("assembly_info") or {}).get("biosample") or {})
    attributes = biosample_attributes(biosample)
    for source, target in ATTRIBUTE_COLUMNS.items():
        value = biosample.get(source) or attributes.get(source)
        if value not in (None, ""):
            row[target] = value
    description = biosample.get("description") or {}
    if isinstance(description, dict):
        row["BioSample Title"] = description.get("title")
        row["BioSample Description"] = description.get("comment") or description.get("title")
    normalized, _ = normalize_managed_metadata_row(row, force_standardization=True)
    return normalized


def fetch_reports(accessions: list[str], *, api_key: str, max_attempts: int, retry_sleep: float, timeout: float) -> list[dict[str, Any]]:
    joined = ",".join(urllib.parse.quote(accession, safe="._") for accession in accessions)
    url = f"{API_BASE}/{joined}/dataset_report?returned_content=COMPLETE"
    headers = {"Accept": "application/json", "User-Agent": "FetchM-WEB/canonical-metadata-fetch"}
    if api_key:
        headers["api-key"] = api_key
    request = urllib.request.Request(url, headers=headers)
    error = ""
    for attempt in range(1, max(1, max_attempts) + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.load(response)
            reports = [report for report in payload.get("reports", []) if isinstance(report, dict)]
            returned = {str(report.get("accession") or report.get("current_accession") or "").strip() for report in reports}
            missing = sorted(set(accessions) - returned)
            if missing:
                raise RuntimeError(f"NCBI did not return {len(missing)} requested accessions; example: {missing[0]}")
            return reports
        except (OSError, http.client.IncompleteRead, urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
            error = f"Metadata report request attempt {attempt}/{max_attempts} failed: {exc}"
            print(error, file=sys.stderr, flush=True)
            if attempt < max(1, max_attempts):
                time.sleep(max(0.0, retry_sleep))
    raise RuntimeError(error or "NCBI metadata report request failed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-batches", type=int, default=0, help="Limit batches for controlled validation only.")
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--retry-sleep", type=float, default=5.0)
    parser.add_argument("--request-timeout", type=float, default=120.0)
    parser.add_argument("--request-sleep", type=float, default=None)
    parser.add_argument("--api-key", default=os.environ.get("NCBI_API_KEY", ""))
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 100:
        parser.error("--batch-size must be between 1 and 100")
    request_sleep = args.request_sleep if args.request_sleep is not None else (0.12 if args.api_key else 0.4)
    fingerprint = str(standardization_rule_manifest().get("version") or "not available")
    fetched = standardized = batches = 0
    while True:
        accessions = missing_standardized_accession_batch(args.snapshot_id, limit=args.batch_size)
        if not accessions:
            break
        reports = fetch_reports(
            accessions, api_key=args.api_key, max_attempts=args.max_attempts,
            retry_sleep=args.retry_sleep, timeout=args.request_timeout,
        )
        insert_inventory_batch(args.snapshot_id, reports)
        rows = [standardizable_row(report) for report in reports]
        seeded = seed_standardized_metadata_batch(
            args.snapshot_id, rows, rule_fingerprint=fingerprint, status="fetched_ncbi_full_report"
        )
        if seeded["seeded"] != len(accessions):
            raise RuntimeError(f"Only {seeded['seeded']} of {len(accessions)} retrieved rows were standardized.")
        fetched += len(reports)
        standardized += seeded["seeded"]
        batches += 1
        if batches % 25 == 0:
            print(json.dumps({"batches_complete": batches, "fetched_rows": fetched, **standardized_metadata_coverage(args.snapshot_id)}, sort_keys=True), flush=True)
        if args.max_batches and batches >= args.max_batches:
            break
        if request_sleep > 0:
            time.sleep(request_sleep)
    summary = {
        "snapshot_id": args.snapshot_id,
        "metadata_source": "NCBI Datasets REST API v2 full accession report",
        "standardization_status": "fetched_ncbi_full_report",
        "batches_complete": batches,
        "fetched_rows": fetched,
        "standardized_rows": standardized,
        **standardized_metadata_coverage(args.snapshot_id),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
