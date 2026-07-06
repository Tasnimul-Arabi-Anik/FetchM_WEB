#!/usr/bin/env python3
"""Fetch and standardize metadata for an admin-hidden domain inventory."""

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

from app import NCBI_API_KEYS
from dataset_production_store import (
    domain_inventory_accession_batch,
    domain_pipeline_config,
    domain_standardized_metadata_coverage,
    insert_domain_inventory_batch,
    missing_domain_standardized_accession_batch,
    normalize_domain_pipeline_key,
    seed_domain_standardized_metadata_batch,
    seed_virus_canonical_entities_batch,
)
from global_insights.generator import standardization_rule_manifest
from tools.fetch_canonical_missing_metadata import API_BASE, standardizable_row
from virus_canonical import virus_standardization_row_fields


def fetch_reports(
    accessions: list[str],
    *,
    api_key: str,
    max_attempts: int,
    retry_sleep: float,
    timeout: float,
    domain_key: str,
) -> list[dict[str, Any]]:
    joined = ",".join(urllib.parse.quote(accession, safe="._") for accession in accessions)
    url = f"{API_BASE}/{joined}/dataset_report?returned_content=COMPLETE"
    headers = {"Accept": "application/json", "User-Agent": f"FetchM-WEB/hidden-{domain_key}-metadata-fetch"}
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
                    file=sys.stderr,
                    flush=True,
                )
                return reports + fetch_reports(
                    missing,
                    api_key=api_key,
                    max_attempts=max_attempts,
                    retry_sleep=retry_sleep,
                    timeout=timeout,
                    domain_key=domain_key,
                )
            if missing:
                raise RuntimeError(f"NCBI did not return requested accession: {missing[0]}")
            return reports
        except (
            OSError,
            http.client.IncompleteRead,
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
            RuntimeError,
        ) as exc:
            error = f"Metadata report request attempt {attempt}/{max_attempts} failed: {exc}"
            print(error, file=sys.stderr, flush=True)
            if attempt < max(1, max_attempts):
                time.sleep(max(0.0, retry_sleep))
    raise RuntimeError(error or "NCBI metadata report request failed")


def standardizable_domain_row(report: dict[str, Any], domain_key: str = "archaea") -> dict[str, Any]:
    key = normalize_domain_pipeline_key(domain_key)
    config = domain_pipeline_config(key)
    row = standardizable_row(report)
    row["FetchM_Domain"] = str(config["label"])
    row["FetchM_Domain_Key"] = key
    row["FetchM_Domain_Profile"] = str(config.get("profile") or f"{key}_hidden_v1")
    row["FetchM_Public_Release_Status"] = str(config.get("release_status") or "locked_admin_hidden")
    if key == "virus":
        row.update(virus_standardization_row_fields(report))
    return row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--domain", default="archaea", help="Hidden domain key, for example 'archaea' or 'virus'.")
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--max-batches", type=int, default=0, help="Limit batches for controlled validation only.")
    parser.add_argument("--refetch-all", action="store_true", help="Refetch and re-standardize every accession in the hidden domain inventory.")
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--retry-sleep", type=float, default=5.0)
    parser.add_argument("--request-timeout", type=float, default=120.0)
    parser.add_argument("--request-sleep", type=float, default=None)
    parser.add_argument("--request-workers", type=int, default=0, help="Concurrent NCBI report requests; defaults to two requests per configured API key.")
    parser.add_argument("--standardization-workers", type=int, default=0, help="CPU workers for metadata normalization; defaults to FETCHM_WEBAPP_CANONICAL_STANDARDIZATION_WORKERS or 10.")
    parser.add_argument("--api-key", default=os.environ.get("NCBI_API_KEY", ""), help="Fallback when no application key pool is configured.")
    args = parser.parse_args()
    args.domain = normalize_domain_pipeline_key(args.domain)
    if not 1 <= args.batch_size <= 100:
        parser.error("--batch-size must be between 1 and 100")
    if args.max_batches < 0:
        parser.error("--max-batches cannot be negative")

    config = domain_pipeline_config(args.domain)
    api_keys = list(NCBI_API_KEYS) or ([args.api_key] if args.api_key else [""])
    request_sleep = args.request_sleep if args.request_sleep is not None else (0.05 if api_keys[0] else 0.4)
    worker_limit = min(10, max(1, len(api_keys) * 2)) if api_keys[0] else 2
    request_workers = min(worker_limit, max(1, args.request_workers or worker_limit))
    standardization_default = int(os.environ.get("FETCHM_WEBAPP_CANONICAL_STANDARDIZATION_WORKERS", "10") or "10")
    standardization_workers = min(32, max(1, args.standardization_workers or standardization_default))
    fingerprint = str(standardization_rule_manifest().get("version") or "not available")
    domain_profile = str(config.get("profile") or f"{args.domain}_hidden_v1")
    domain_rule_fingerprint = f"{fingerprint};domain_profile={domain_profile}"
    fetched = standardized = batches = 0
    virus_sequences_seeded = virus_relationships_seeded = 0
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
                    accessions = domain_inventory_accession_batch(
                        args.domain, args.snapshot_id, after_accession=last_accession, limit=args.batch_size * cycle_workers
                    )
                else:
                    accessions = missing_domain_standardized_accession_batch(
                        args.domain, args.snapshot_id, limit=args.batch_size * cycle_workers
                    )
                if not accessions:
                    break
                if args.refetch_all:
                    last_accession = accessions[-1]
                accession_batches = [accessions[start:start + args.batch_size] for start in range(0, len(accessions), args.batch_size)]
                reports: list[dict[str, Any]] = []
                futures = [
                    request_executor.submit(
                        fetch_reports,
                        batch,
                        api_key=api_keys[index % len(api_keys)],
                        max_attempts=args.max_attempts,
                        retry_sleep=args.retry_sleep,
                        timeout=args.request_timeout,
                        domain_key=args.domain,
                    )
                    for index, batch in enumerate(accession_batches)
                ]
                for future in as_completed(futures):
                    reports.extend(future.result())
                insert_domain_inventory_batch(args.domain, args.snapshot_id, reports)
                if standardization_executor is None or len(reports) <= 1:
                    rows = [standardizable_domain_row(report, args.domain) for report in reports]
                else:
                    rows = list(standardization_executor.map(
                        standardizable_domain_row,
                        reports,
                        repeat(args.domain),
                        chunksize=max(1, len(reports) // (standardization_workers * 4)),
                    ))
                seeded = seed_domain_standardized_metadata_batch(
                    args.domain, args.snapshot_id, rows,
                    rule_fingerprint=domain_rule_fingerprint,
                    status="fetched_ncbi_full_report",
                )
                if args.domain == "virus":
                    virus_seeded = seed_virus_canonical_entities_batch(
                        args.snapshot_id,
                        reports,
                        source_status="refetched_all_ncbi_full_report" if args.refetch_all else "fetched_ncbi_full_report",
                    )
                    virus_sequences_seeded += int(virus_seeded["virus_sequences_seeded"] or 0)
                    virus_relationships_seeded += int(virus_seeded["taxon_relationships_seeded"] or 0)
                if seeded["seeded"] != len(accessions):
                    raise RuntimeError(f"Only {seeded['seeded']} of {len(accessions)} retrieved rows were standardized.")
                fetched += len(reports)
                standardized += seeded["seeded"]
                batches += len(accession_batches)
                if batches % 20 == 0:
                    print(json.dumps({
                        "batches_complete": batches,
                        "fetched_rows": fetched,
                        "request_workers": request_workers,
                        "standardization_workers": standardization_workers,
                        **domain_standardized_metadata_coverage(args.domain, args.snapshot_id),
                    }, sort_keys=True), flush=True)
                if args.max_batches and batches >= args.max_batches:
                    break
                if request_sleep > 0:
                    time.sleep(request_sleep)
    finally:
        if standardization_executor is not None:
            standardization_executor.shutdown()
    summary = {
        "domain_key": args.domain,
        "domain_label": config["label"],
        "snapshot_id": args.snapshot_id,
        "metadata_source": "NCBI Datasets REST API v2 full accession report",
        "standardization_status": "refetched_all_ncbi_full_report" if args.refetch_all else "fetched_ncbi_full_report",
        "domain_profile": domain_profile,
        "release_locked": bool(config.get("release_locked", True)),
        "public_enabled": bool(config.get("public_enabled")),
        "refetch_all": bool(args.refetch_all),
        "batches_complete": batches,
        "fetched_rows": fetched,
        "standardized_rows": standardized,
        "virus_sequences_seeded": virus_sequences_seeded,
        "virus_taxon_relationships_seeded": virus_relationships_seeded,
        "request_workers": request_workers,
        "standardization_workers": standardization_workers,
        "configured_api_key_count": len([key for key in api_keys if key]),
        **domain_standardized_metadata_coverage(args.domain, args.snapshot_id),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
