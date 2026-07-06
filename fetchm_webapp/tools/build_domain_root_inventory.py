#!/usr/bin/env python3
"""Build an admin-hidden GenBank root inventory for a non-public domain pipeline."""

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

from dataset_production_store import (
    default_domain_snapshot_id,
    domain_inventory_api_url,
    domain_inventory_page_progress,
    domain_pipeline_config,
    fail_domain_inventory_snapshot,
    finish_domain_inventory_page,
    finish_domain_inventory_snapshot,
    insert_domain_inventory_batch,
    latest_domain_inventory_page_checkpoint,
    normalize_domain_pipeline_key,
    start_domain_inventory_page,
    start_domain_inventory_snapshot,
)


def fetch_page(args: argparse.Namespace, api_url: str, page_token: str | None) -> dict[str, Any]:
    params = {
        "filters.assembly_source": "GENBANK",
        "filters.assembly_version": "CURRENT",
        "returned_content": "ASSM_ACC",
        "page_size": str(args.page_size),
    }
    if page_token:
        params["page_token"] = page_token
    url = api_url + "?" + urllib.parse.urlencode(params)
    headers = {
        "Accept": "application/json",
        "User-Agent": f"FetchM-WEB/hidden-{args.domain}-inventory",
    }
    if args.api_key:
        headers["api-key"] = args.api_key
    request = urllib.request.Request(url, headers=headers)
    last_error = ""
    for attempt in range(1, max(1, args.max_attempts) + 1):
        try:
            with urllib.request.urlopen(request, timeout=args.request_timeout) as response:
                return json.load(response)
        except (
            OSError,
            http.client.IncompleteRead,
            urllib.error.HTTPError,
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError,
        ) as exc:
            last_error = f"REST page request attempt {attempt}/{args.max_attempts} failed: {exc}"
            print(last_error, file=sys.stderr, flush=True)
            if attempt < max(1, args.max_attempts):
                time.sleep(max(0.0, args.retry_sleep))
    raise RuntimeError(last_error or "NCBI REST page request failed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--domain", default="archaea", help="Hidden domain key. Currently only 'archaea' is supported.")
    parser.add_argument("--snapshot-id", default=None)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--retry-sleep", type=float, default=10.0)
    parser.add_argument("--request-timeout", type=float, default=120.0)
    parser.add_argument("--api-key", default=os.environ.get("NCBI_API_KEY", ""))
    parser.add_argument("--max-pages", type=int, default=0, help="Stop after N pages for validation only; never completes a release snapshot.")
    args = parser.parse_args()
    args.domain = normalize_domain_pipeline_key(args.domain)
    if not 1 <= args.page_size <= 1000:
        parser.error("--page-size must be between 1 and 1000")
    if args.max_pages < 0:
        parser.error("--max-pages cannot be negative")

    config = domain_pipeline_config(args.domain)
    api_url = domain_inventory_api_url(args.domain)
    snapshot_id = args.snapshot_id or default_domain_snapshot_id(args.domain)
    invocation = (
        f"GET {api_url}?filters.assembly_source=GENBANK&filters.assembly_version=CURRENT"
        f"&returned_content=ASSM_ACC&page_size={args.page_size}&page_token=<checkpoint>"
    )
    start_domain_inventory_snapshot(args.domain, snapshot_id, invocation, "NCBI Datasets REST API v2")
    checkpoint = latest_domain_inventory_page_checkpoint(args.domain, snapshot_id)
    page_number = int(checkpoint["page_number"] if checkpoint else 0)
    page_token = checkpoint["next_page_token"] if checkpoint else None
    if checkpoint and not page_token:
        progress = domain_inventory_page_progress(args.domain, snapshot_id)
        summary = finish_domain_inventory_snapshot(
            args.domain, snapshot_id, progress["raw_records"], progress["noncanonical_records"], progress["duplicate_records"]
        )
        print(json.dumps({"snapshot_id": snapshot_id, **summary, **progress, "inventory_mode": "hidden_domain_rest_page_checkpoints"}, sort_keys=True))
        return 0 if summary.get("status") == "completed" else 1

    try:
        while True:
            page_number += 1
            start_domain_inventory_page(args.domain, snapshot_id, page_number, page_token)
            payload = fetch_page(args, api_url, page_token)
            reports = [report for report in payload.get("reports", []) if isinstance(report, dict)]
            expected_total = int(payload.get("total_count") or 0)
            written, invalid, duplicates = insert_domain_inventory_batch(args.domain, snapshot_id, reports) if reports else (0, 0, 0)
            next_token = str(payload.get("next_page_token") or "") or None
            finish_domain_inventory_page(
                args.domain,
                snapshot_id,
                page_number,
                "completed",
                next_page_token=next_token,
                expected_total=expected_total,
                raw_records=len(reports),
                canonical_records=written,
                noncanonical_records=invalid,
                duplicate_records=duplicates,
            )
            if args.max_pages and page_number >= args.max_pages:
                raise RuntimeError("Validation max-pages limit reached; hidden domain snapshot intentionally not complete.")
            if not next_token:
                break
            page_token = next_token
        progress = domain_inventory_page_progress(args.domain, snapshot_id)
        summary = finish_domain_inventory_snapshot(
            args.domain, snapshot_id, progress["raw_records"], progress["noncanonical_records"], progress["duplicate_records"]
        )
        summary.update(progress)
        summary["inventory_mode"] = "hidden_domain_rest_page_checkpoints"
        summary["public_enabled"] = bool(config.get("public_enabled"))
        summary["release_locked"] = bool(config.get("release_locked", True))
        print(json.dumps({"snapshot_id": snapshot_id, **summary}, sort_keys=True))
        return 0 if summary.get("status") == "completed" else 1
    except Exception as exc:
        fail_domain_inventory_snapshot(args.domain, snapshot_id, str(exc))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
