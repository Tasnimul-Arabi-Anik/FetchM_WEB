# Source/Environment Semantic Completion Release Summary

Date: 2026-06-24

## Scope

Applied reviewed Phase 2A strict-field corrections and one evidence-gated source/environment/material completion pass to the canonical bacterial metadata snapshot. Raw metadata and legacy compatibility fields were preserved.

## Canonical Application

- Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`
- Rows scanned: 3,131,699
- Unique assemblies changed: 842,467
- Phase 2A primary strict-field clears: 30,851
- Phase 2A companion field clears: 76,204
- Source/environment completion assignments applied: 1,016,849
- Destinations already present: 214,790
- Preserved destination conflicts: 65,183
- Ambiguous values retained: 89,603
- Backup table: `assembly_standardization_backup_source_env_20260624164131`

## Safety Gates

- Legacy compatibility-field changes: 0
- Raw field changes: 0
- Protected host/geography/date changes: 0
- Outside-allowlist changes: 0
- Destination conflicts: 0
- Required evidence failures: 0
- Unknown condition failures: 0
- Missing removal provenance: 0
- Forbidden strict-site values remaining: 0
- Catheter changes: 0
- Plant context without evidence: 0
- Patient environment-only additions: 0

## QA Results

- Post-apply semantic QA hard failures: []
- Source/sample/environment QA: `pass`
- Source hard exact leakage rows: 0
- Non-approved broad rows: 0
- Controlled-category duplicate/conflict keys: 0/0
- Host QA: `pass`
- Geography/date QA: `pass`

## Global Insights

- Snapshot: `20260624T183607Z_global_insights`
- Source snapshot: `20260602T140414Z_genbank_bacteria_root`
- App commit: `4a10291e3797397975b48096709c5547df20362d`
- Latest pointer updated: yes
- QA status: warning only for absent manual validation records

## Validation

- Full tests: 111 passed in 81.396s
- `python -m py_compile app.py tools/*.py`: passed
- `git diff --check`: passed

## Deployment

Manual deployment was not performed in this batch.
