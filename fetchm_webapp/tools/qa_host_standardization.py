#!/usr/bin/env python3
"""Fail-fast host-standardization production QA."""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as fetchm_app
from dataset_production_store import connect
from tools.host_standardization_monitoring import latest_active_snapshot_id, read_allowlisted_taxids


STANDARDIZATION_ROOT = ROOT / "standardization"
MICROBIAL_SUPERKINGDOMS = {"Bacteria", "Archaea", "Viruses"}
EUKARYOTIC_ALGAL_PROBES = {
    "Sargassum hemiphyllum": ("Sargassum hemiphyllum", "127544"),
    "red marine alga": ("Rhodophyta", "2763"),
}
EXACT_PROBES = {
    "waterlettuce": ("Pistia stratiotes", "4477"),
    "water lettuce": ("Pistia stratiotes", "4477"),
    "water-lettuce": ("Pistia stratiotes", "4477"),
    "shorebird": ("Charadriiformes", "8906"),
    "shore bird": ("Charadriiformes", "8906"),
    "shorebirds": ("Charadriiformes", "8906"),
    "Cuttloefish": ("Sepiidae", "6608"),
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def lineage_lookup(taxids: list[str]) -> dict[str, dict[str, str]]:
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
    lookup: dict[str, dict[str, str]] = {}
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        lineage = parts[1].strip()
        lineage_set = set(lineage.split(";"))
        superkingdom = next(
            (value for value in ("Eukaryota", "Bacteria", "Archaea", "Viruses") if value in lineage_set),
            "",
        )
        lookup[parts[0].strip()] = {
            "lineage": lineage,
            "rank": parts[2].strip(),
            "superkingdom": superkingdom,
        }
    return lookup


def canonical_name_taxids(names: list[str]) -> dict[str, set[str]]:
    unique = sorted({name.strip() for name in names if name.strip()})
    if not unique:
        return {}
    result = subprocess.run(
        ["taxonkit", "name2taxid", "--show-rank"],
        input="\n".join(unique) + "\n",
        text=True,
        capture_output=True,
        check=False,
        timeout=240,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "TaxonKit name2taxid failed")
    lookup: dict[str, set[str]] = {}
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[1].strip().isdigit():
            lookup.setdefault(parts[0].strip().casefold(), set()).add(parts[1].strip())
    return lookup


def runtime_output(raw_host: str) -> dict[str, str]:
    return fetchm_app.enrich_host_standardization(
        raw_host,
        fetchm_app.standardize_host_metadata(raw_host),
    )


def run_static_checks() -> tuple[list[dict[str, Any]], list[str]]:
    checks: list[dict[str, Any]] = []
    failures: list[str] = []
    synonyms = read_csv(STANDARDIZATION_ROOT / "host_synonyms.csv")
    contexts = read_csv(STANDARDIZATION_ROOT / "host_context_rules.csv")
    allowlisted_taxids = set(read_allowlisted_taxids())
    taxids = [str(row.get("taxid") or "").strip() for row in synonyms]
    lineages = lineage_lookup(taxids)
    microbial_rules = [
        row for row in synonyms
        if lineages.get(str(row.get("taxid") or "").strip(), {}).get("superkingdom") in MICROBIAL_SUPERKINGDOMS
        and str(row.get("taxid") or "").strip() not in allowlisted_taxids
    ]
    checks.append({"check": "non_allowlisted_microbial_rules", "count": len(microbial_rules)})
    if microbial_rules:
        failures.append(f"{len(microbial_rules)} non-allowlisted microbial rows remain in host_synonyms.csv")

    context_failures = []
    for row in contexts:
        raw_host = str(row.get("synonym") or "").strip()
        expected_context = str(row.get("context") or "").strip()
        output = runtime_output(raw_host)
        if output.get("Host_SD") or output.get("Host_Context_SD") != expected_context:
            context_failures.append({"raw_host": raw_host, "output": output, "expected_context": expected_context})
    checks.append({"check": "context_only_terms", "count": len(context_failures), "examples": context_failures[:10]})
    if context_failures:
        failures.append(f"{len(context_failures)} context-only terms produced invalid host output")

    exact_failures = []
    for raw_host, expected in EXACT_PROBES.items():
        output = runtime_output(raw_host)
        actual = (output.get("Host_SD", ""), output.get("Host_TaxID", ""))
        if actual != expected:
            exact_failures.append({"raw_host": raw_host, "expected": expected, "actual": actual})
    checks.append({"check": "exact_rule_precedence", "count": len(exact_failures), "examples": exact_failures})
    if exact_failures:
        failures.append("Reviewed exact host rules were downgraded or changed")

    algae_failures = []
    for raw_host, expected in EUKARYOTIC_ALGAL_PROBES.items():
        output = runtime_output(raw_host)
        actual = (output.get("Host_SD", ""), output.get("Host_TaxID", ""))
        if actual != expected or output.get("Host_Superkingdom") != "Eukaryota":
            algae_failures.append({"raw_host": raw_host, "expected": expected, "output": output})
    checks.append({"check": "eukaryotic_algae_preserved", "count": len(algae_failures), "examples": algae_failures})
    if algae_failures:
        failures.append("Valid eukaryotic algal hosts were demoted")
    return checks, failures


def run_database_checks(snapshot_id: str) -> tuple[list[dict[str, Any]], list[str]]:
    allowlisted_taxids = read_allowlisted_taxids()
    with connect() as connection:
        leakage = connection.execute(
            """
            SELECT COALESCE(s.standardized_payload->>'Host_SD', '') AS host_sd,
                   COALESCE(s.standardized_payload->>'Host_TaxID', '') AS host_taxid,
                   COALESCE(s.standardized_payload->>'Host_Superkingdom', '') AS superkingdom,
                   COUNT(*) AS assembly_count
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
              AND COALESCE(s.standardized_payload->>'Host_Superkingdom', '') IN ('Bacteria', 'Archaea', 'Viruses')
              AND NOT (COALESCE(s.standardized_payload->>'Host_TaxID', '') = ANY(%s))
            GROUP BY 1, 2, 3 ORDER BY assembly_count DESC
            """,
            (snapshot_id, allowlisted_taxids),
        ).fetchall()
        production_pairs = connection.execute(
            """
            SELECT s.standardized_payload->>'Host_SD' AS host_sd,
                   s.standardized_payload->>'Host_TaxID' AS host_taxid,
                   s.standardized_payload->>'Host_Superkingdom' AS host_superkingdom,
                   COUNT(*) AS assembly_count
            FROM bacterial_inventory_membership AS i
            JOIN assembly_standardization AS s ON s.assembly_accession = i.assembly_accession
            WHERE i.snapshot_id = %s
              AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_SD', '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(s.standardized_payload->>'Host_TaxID', '')), '') IS NOT NULL
            GROUP BY 1, 2, 3
            """,
            (snapshot_id,),
        ).fetchall()
    count = sum(int(row[3] or 0) for row in leakage)
    checks = [{
        "check": "canonical_microbiological_host_leakage",
        "count": count,
        "examples": [
            {"Host_SD": row[0], "Host_TaxID": row[1], "Host_Superkingdom": row[2], "count": int(row[3])}
            for row in leakage[:10]
        ],
    }]
    failures = [f"{count} canonical rows contain non-allowlisted microbial Host_SD"] if leakage else []
    resolved_names = canonical_name_taxids([str(row[0] or "") for row in production_pairs])
    resolved_lineages = lineage_lookup([
        taxid for taxids in resolved_names.values() for taxid in taxids
    ])
    mismatches = []
    mismatch_rows = 0
    for host_sd, host_taxid, host_superkingdom, assembly_count in production_pairs:
        resolved = resolved_names.get(str(host_sd or "").casefold(), set())
        resolved_superkingdoms = {
            resolved_lineages.get(taxid, {}).get("superkingdom", "")
            for taxid in resolved
        } - {""}
        configured_superkingdom = str(host_superkingdom or "")
        if (
            configured_superkingdom
            and resolved_superkingdoms
            and configured_superkingdom not in resolved_superkingdoms
        ):
            mismatch_rows += int(assembly_count or 0)
            mismatches.append({
                "Host_SD": host_sd,
                "configured_taxid": host_taxid,
                "configured_superkingdom": host_superkingdom,
                "resolved_superkingdoms": sorted(resolved_superkingdoms),
                "assembly_count": int(assembly_count or 0),
            })
    checks.append({
        "check": "production_canonical_lineage_mismatches",
        "count": mismatch_rows,
        "distinct_pairs": len(mismatches),
        "examples": sorted(mismatches, key=lambda row: -row["assembly_count"])[:10],
    })
    if mismatches:
        failures.append(f"{mismatch_rows} canonical rows have Host_SD/TaxID lineage mismatches across {len(mismatches)} pairs")
    return checks, failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default="")
    parser.add_argument("--fail-on-leakage", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    checks, failures = run_static_checks()
    snapshot_id = args.snapshot_id
    database_available = bool(os.environ.get("FETCHM_WEBAPP_DATASET_DATABASE_URL", "").strip())
    if database_available:
        snapshot_id = snapshot_id or latest_active_snapshot_id()
        db_checks, db_failures = run_database_checks(snapshot_id)
        checks.extend(db_checks)
        failures.extend(db_failures)
    elif args.fail_on_leakage:
        failures.append("Canonical database QA was requested but FETCHM_WEBAPP_DATASET_DATABASE_URL is not configured")

    report = {
        "status": "fail" if failures else "pass",
        "snapshot_id": snapshot_id,
        "checks": checks,
        "failures": failures,
    }
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
