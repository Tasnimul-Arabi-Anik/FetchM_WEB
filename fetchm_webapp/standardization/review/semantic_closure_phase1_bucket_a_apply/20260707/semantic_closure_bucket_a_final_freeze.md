# Semantic Closure Bucket A Final Freeze

Status: complete. Bucket A was manually approved and applied; Bucket B/environment rules were not applied and remain held for separate review.

## Canonical Scope

- Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`
- Rows scanned: 3,131,699
- Rows changed: 1,129
- Backup table: `assembly_standardization_backup_semantic_closure_bucket_a_20260707095230`
- Post-apply dry run changed rows: 0

## Corrected Values

- `Host_Health_State_SD = healthy/control` remaining: 0
- `Host_Health_State_SD = diseased/patient` remaining: 0
- `Isolation_Site_SD = catheter` remaining: 0
- `Host_Study_Group_SD = control` rows: 1,077

## Validation

- Full unittest discovery: 150 passed
- Host QA: pass
- Geography/date QA: pass
- Source/sample/environment QA: pass
- Controlled-category duplicate/conflict keys: 0/0
- Raw, legacy compatibility, host-taxonomy, geography/date, and outside-allowlist changes: 0

## Global Insights And Deployment

- Global Insights snapshot: `20260707T103702Z_global_insights`
- Global Insights row count: 3,131,699
- Live `/healthz` commit: `74de51ebb9aaee37736882e20fffa0c4794f76be`
- Deployed containers recreated: web and insights-worker only
- PostgreSQL and standardization workers: not recreated

## Git Reconciliation

The live runtime was built from implementation commit `74de51e`. The final GitHub main freeze commit is a later bookkeeping/evidence commit that contains `74de51e` and evidence commit `ba43457` as ancestors. This freeze does not apply more metadata changes, regenerate standardization, or apply Bucket B.
