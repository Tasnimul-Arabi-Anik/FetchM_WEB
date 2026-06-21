#!/usr/bin/env python3
"""Fetch and standardize canonical assemblies missing from existing metadata."""

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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from itertools import repeat
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import NCBI_API_KEYS, build_species_tsv_row, normalize_managed_metadata_row
from domain_profiles import domain_profile_from_snapshot_id
from dataset_production_store import (
    insert_inventory_batch,
    inventory_accession_batch,
    missing_standardized_accession_batch,
    seed_standardized_metadata_batch,
    standardized_metadata_coverage,
)
from global_insights.generator import standardization_rule_manifest
from tools.host_standardization_monitoring import generate_host_monitoring

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


def standardizable_row(report: dict[str, Any], domain_profile_key: str | None = None) -> dict[str, Any]:
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
    normalized, _ = normalize_managed_metadata_row(
        row,
        force_standardization=True,
        domain_profile=domain_profile_key,
    )
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
            if missing and len(accessions) > 1:
                print(
                    f"NCBI omitted {len(missing)} of {len(accessions)} accessions in a multi-accession response; retrying omitted accessions separately.",
                    file=sys.stderr, flush=True,
                )
                return reports + fetch_reports(
                    missing, api_key=api_key, max_attempts=max_attempts,
                    retry_sleep=retry_sleep, timeout=timeout,
                )
            if missing:
                raise RuntimeError(f"NCBI did not return requested accession: {missing[0]}")
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
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--max-batches", type=int, default=0, help="Limit batches for controlled validation only.")
    parser.add_argument("--refetch-all", action="store_true", help="Refetch and re-standardize every accession in the canonical inventory, not only missing accessions.")
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--retry-sleep", type=float, default=5.0)
    parser.add_argument("--request-timeout", type=float, default=120.0)
    parser.add_argument("--request-sleep", type=float, default=None)
    parser.add_argument("--request-workers", type=int, default=0, help="Concurrent NCBI report requests; defaults to two requests per configured API key.")
    parser.add_argument("--standardization-workers", type=int, default=0, help="CPU workers for metadata normalization; defaults to FETCHM_WEBAPP_CANONICAL_STANDARDIZATION_WORKERS or 10.")
    parser.add_argument("--api-key", default=os.environ.get("NCBI_API_KEY", ""), help="Fallback when no application key pool is configured.")
    parser.add_argument("--skip-host-monitoring", action="store_true", help="Skip host-monitoring export side effects for isolated pilots.")
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 100:
        parser.error("--batch-size must be between 1 and 100")
    api_keys = list(NCBI_API_KEYS) or ([args.api_key] if args.api_key else [""])
    request_sleep = args.request_sleep if args.request_sleep is not None else (0.05 if api_keys[0] else 0.4)
    worker_limit = min(10, max(1, len(api_keys) * 2)) if api_keys[0] else 2
    request_workers = min(worker_limit, max(1, args.request_workers or worker_limit))
    standardization_default = int(os.environ.get("FETCHM_WEBAPP_CANONICAL_STANDARDIZATION_WORKERS", "10") or "10")
    standardization_workers = min(32, max(1, args.standardization_workers or standardization_default))
    active_domain_profile = domain_profile_from_snapshot_id(args.snapshot_id)
    fingerprint = str(standardization_rule_manifest().get("version") or "not available")
    fetched = standardized = batches = 0
    last_accession = ""
    standardization_executor = ProcessPoolExecutor(max_workers=standardization_workers) if standardization_workers > 1 else None
    try:
        with ThreadPoolExecutor(max_workers=request_workers) as request_executor:
            while True:
                cycle_workers = request_workers
                if args.max_batches:
                    cycle_workers = min(cycle_workers, args.max_batches - batches)
                    if cycle_workers <= 0:
                        break
                if args.refetch_all:
                    accessions = inventory_accession_batch(
                        args.snapshot_id, after_accession=last_accession, limit=args.batch_size * cycle_workers
                    )
                else:
                    accessions = missing_standardized_accession_batch(args.snapshot_id, limit=args.batch_size * cycle_workers)
                if not accessions:
                    break
                if args.refetch_all:
                    last_accession = accessions[-1]
                accession_batches = [accessions[start:start + args.batch_size] for start in range(0, len(accessions), args.batch_size)]
                reports: list[dict[str, Any]] = []
                futures = [
                    request_executor.submit(
                        fetch_reports, batch, api_key=api_keys[index % len(api_keys)], max_attempts=args.max_attempts,
                        retry_sleep=args.retry_sleep, timeout=args.request_timeout,
                    )
                    for index, batch in enumerate(accession_batches)
                ]
                for future in as_completed(futures):
                    reports.extend(future.result())
                insert_inventory_batch(args.snapshot_id, reports)
                if standardization_executor is None or len(reports) <= 1:
                    rows = [standardizable_row(report, active_domain_profile.key) for report in reports]
                else:
                    rows = list(standardization_executor.map(
                        standardizable_row, reports, repeat(active_domain_profile.key),
                        chunksize=max(1, len(reports) // (standardization_workers * 4))
                    ))
                seeded = seed_standardized_metadata_batch(
                    args.snapshot_id, rows, rule_fingerprint=fingerprint, status="fetched_ncbi_full_report"
                )
                if seeded["seeded"] != len(accessions):
                    raise RuntimeError(f"Only {seeded['seeded']} of {len(accessions)} retrieved rows were standardized.")
                fetched += len(reports)
                standardized += seeded["seeded"]
                batches += len(accession_batches)
                if batches % 20 == 0:
                    print(json.dumps({"batches_complete": batches, "fetched_rows": fetched, "request_workers": request_workers, "standardization_workers": standardization_workers, **standardized_metadata_coverage(args.snapshot_id)}, sort_keys=True), flush=True)
                if args.max_batches and batches >= args.max_batches:
                    break
                if request_sleep > 0:
                    time.sleep(request_sleep)
    finally:
        if standardization_executor is not None:
            standardization_executor.shutdown()
    summary = {
        "snapshot_id": args.snapshot_id,
        "metadata_source": "NCBI Datasets REST API v2 full accession report",
        "domain_profile": active_domain_profile.key,
        "standardization_status": "refetched_all_ncbi_full_report" if args.refetch_all else "fetched_ncbi_full_report",
        "refetch_all": bool(args.refetch_all),
        "batches_complete": batches,
        "fetched_rows": fetched,
        "standardized_rows": standardized,
        "request_workers": request_workers,
        "standardization_workers": standardization_workers,
        "configured_api_key_count": len([key for key in api_keys if key]),
        **standardized_metadata_coverage(args.snapshot_id),
    }
    if not summary["missing_standardized_assemblies"] and not args.skip_host_monitoring:
        summary["host_standardization_monitoring"] = generate_host_monitoring(args.snapshot_id)
    elif args.skip_host_monitoring:
        summary["host_standardization_monitoring"] = "skipped"
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
