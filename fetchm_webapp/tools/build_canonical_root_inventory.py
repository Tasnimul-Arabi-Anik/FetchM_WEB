#!/usr/bin/env python3
"""Build a canonical GenBank domain inventory with restartable NCBI REST pages."""

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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from domain_profiles import DOMAIN_PROFILES, domain_profile, validate_snapshot_id_for_profile
from dataset_production_store import (
    fail_inventory_snapshot,
    finish_inventory_page,
    finish_inventory_snapshot,
    insert_inventory_batch,
    inventory_page_progress,
    latest_inventory_page_checkpoint,
    start_inventory_page,
    start_inventory_snapshot,
)

def api_url(taxon_id: int) -> str:
    return f"https://api.ncbi.nlm.nih.gov/datasets/v2/genome/taxon/{taxon_id}/dataset_report"


def fetch_page(args: argparse.Namespace, page_token: str | None) -> dict[str, Any]:
    params = {
        "filters.assembly_source": "GENBANK",
        "filters.assembly_version": "CURRENT",
        "returned_content": "ASSM_ACC",
        "page_size": str(args.page_size),
    }
    if page_token:
        params["page_token"] = page_token
    profile = domain_profile(args.domain)
    url = api_url(profile.ncbi_taxon_id) + "?" + urllib.parse.urlencode(params)
    headers = {"Accept": "application/json", "User-Agent": f"FetchM-WEB/{profile.user_agent_token}"}
    if args.api_key:
        headers["api-key"] = args.api_key
    request = urllib.request.Request(url, headers=headers)
    last_error = ""
    for attempt in range(1, max(1, args.max_attempts) + 1):
        try:
            with urllib.request.urlopen(request, timeout=args.request_timeout) as response:
                return json.load(response)
        except (OSError, http.client.IncompleteRead, urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = f"REST page request attempt {attempt}/{args.max_attempts} failed: {exc}"
            print(last_error, file=sys.stderr, flush=True)
            if attempt < max(1, args.max_attempts):
                time.sleep(max(0.0, args.retry_sleep))
    raise RuntimeError(last_error or "NCBI REST page request failed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--domain", default="bacteria", choices=sorted(DOMAIN_PROFILES), help="Canonical domain profile to inventory.")
    parser.add_argument("--datasets-bin", default="datasets", help=argparse.SUPPRESS)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--retry-sleep", type=float, default=10.0)
    parser.add_argument("--request-timeout", type=float, default=120.0)
    parser.add_argument("--api-key", default=os.environ.get("NCBI_API_KEY", ""))
    parser.add_argument("--max-pages", type=int, default=0, help="Stop after N pages for validation only; never completes a release snapshot.")
    args = parser.parse_args()
    if not 1 <= args.page_size <= 1000:
        parser.error("--page-size must be between 1 and 1000")
    profile = domain_profile(args.domain)
    snapshot_id = (
        validate_snapshot_id_for_profile(args.snapshot_id, profile)
        if args.snapshot_id
        else profile.snapshot_id(datetime.now(timezone.utc))
    )
    invocation = (
        f"GET {api_url(profile.ncbi_taxon_id)}?filters.assembly_source=GENBANK&filters.assembly_version=CURRENT"
        f"&returned_content=ASSM_ACC&page_size={args.page_size}&page_token=<checkpoint>"
    )
    start_inventory_snapshot(snapshot_id, invocation, "NCBI Datasets REST API v2", profile_key=profile.key)
    checkpoint = latest_inventory_page_checkpoint(snapshot_id)
    page_number = int(checkpoint["page_number"] if checkpoint else 0)
    page_token = checkpoint["next_page_token"] if checkpoint else None
    if checkpoint and not page_token:
        progress = inventory_page_progress(snapshot_id)
        summary = finish_inventory_snapshot(snapshot_id, progress["raw_records"], progress["noncanonical_records"], progress["duplicate_records"])
        print(json.dumps({"snapshot_id": snapshot_id, "domain_profile": profile.key, **summary, **progress, "inventory_mode": "rest_page_checkpoints"}, sort_keys=True))
        return 0 if summary.get("status") == "completed" else 1
    try:
        while True:
            page_number += 1
            start_inventory_page(snapshot_id, page_number, page_token)
            payload = fetch_page(args, page_token)
            reports = [report for report in payload.get("reports", []) if isinstance(report, dict)]
            expected_total = int(payload.get("total_count") or 0)
            written, invalid, duplicates = insert_inventory_batch(snapshot_id, reports) if reports else (0, 0, 0)
            next_token = str(payload.get("next_page_token") or "") or None
            finish_inventory_page(
                snapshot_id, page_number, "completed", next_page_token=next_token, expected_total=expected_total,
                raw_records=len(reports), canonical_records=written, noncanonical_records=invalid, duplicate_records=duplicates,
            )
            if args.max_pages and page_number >= args.max_pages:
                raise RuntimeError("Validation max-pages limit reached; snapshot intentionally not complete.")
            if not next_token:
                break
            page_token = next_token
        progress = inventory_page_progress(snapshot_id)
        summary = finish_inventory_snapshot(snapshot_id, progress["raw_records"], progress["noncanonical_records"], progress["duplicate_records"])
        summary.update(progress)
        summary["inventory_mode"] = "rest_page_checkpoints"
        print(json.dumps({"snapshot_id": snapshot_id, "domain_profile": profile.key, **summary}, sort_keys=True))
        return 0 if summary.get("status") == "completed" else 1
    except Exception as exc:
        fail_inventory_snapshot(snapshot_id, str(exc))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
