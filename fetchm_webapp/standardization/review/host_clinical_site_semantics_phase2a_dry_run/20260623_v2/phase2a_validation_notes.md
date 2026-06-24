# Phase 2A Release-Candidate Validation Notes

## Scope

This release candidate remains dry-run only. It does not write canonical metadata, regenerate Global Insights, deploy, modify host taxonomy, modify geography/date logic, or change legacy source/sample compatibility fields.

## Dry-Run Result

The hardened Phase 2A dry run was regenerated on the clean release-candidate branch after the manure enrichment rule was changed to `destination_blank`.

Key metrics:

- Canonical rows scanned: 3,131,699
- Reviewed allowlist rules: 25
- Unique assemblies projected to change: 30,738
- Primary strict-field assignments cleared: 30,851
- Companion fields cleared: 76,204
- New-axis assignments projected: 32,487
- Destination conflicts: 0
- Required evidence failures: 0
- Unknown-condition failures: 0
- Legacy compatibility field changes: 0
- Protected/raw field changes: 0
- Rows outside reviewed allowlist affected: 0
- Patient environment-only context additions: 0
- Plant context without evidence: 0
- Catheter changes: 0
- Removals without provenance: 0

## Artifact Policy

The full row-level projection is intentionally not tracked in Git:

- `phase2a_projected_row_changes.tsv`
- `phase2a_projected_row_changes.tsv.gz`

The tracked manifest is `phase2a_full_artifact_manifest.json`. It records row count, byte sizes, SHA-256 checksums, snapshot ID, ruleset version, and an external archive URI placeholder.

## Tests

Focused semantic tests passed before and after dry-run regeneration.

Docker full discovery command:

```bash
/opt/conda/bin/python -m unittest discover -s tests -p "test_*.py"
/opt/conda/bin/python -m py_compile app.py tools/semantic_phase2a_dry_run.py tests/test_metadata_standardization_regressions.py tests/test_semantic_phase2a_dry_run.py
```

Result:

```text
Ran 116 tests in 79.998s
OK
```

The test environment used an isolated temporary app data mount and initialized SQLite schema. The route tests no longer depend on production `data/` state.

## Gate Status

Dry-run mutation-safety gate: pass.

Canonical promotion gate: intentionally not run and not approved in this task.
