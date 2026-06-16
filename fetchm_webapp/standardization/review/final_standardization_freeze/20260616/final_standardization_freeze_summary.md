# Final Metadata Standardization Freeze Checkpoint

Generated: 2026-06-16

This checkpoint freezes the current canonical FetchM WEB metadata standardization state after host, geography/date, and source/sample/environment curation. Deployment remains manual even though the technical readiness gate passes.

## Canonical Scope

- Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`
- Rows audited: 3,131,699
- Global Insights snapshot: `20260616T114556Z_global_insights`
- Global Insights pass: true
- Deployment gate: technical checks pass; `safe_to_deploy=false` because deployment is intentionally manual.

## Hard Gates

| Gate | Result |
| --- | ---: |
| Host QA status | pass |
| Source/sample/environment QA status | pass |
| Geography/date QA status | pass |
| Non-allowlisted microbial host leakage | 0 |
| Non-country values in `Country` | 0 |
| Country-continent mismatches | 0 |
| Country-subcontinent mismatches | 0 |
| Invalid/future/impossible collection years | 0 |
| Source hard exact leakage rows | 0 |
| Source broad vocabulary leakage rows | 0 |
| Controlled-category duplicate/conflict keys | 0 / 0 |
| Admin-review blockers | 0 |

## Coverage

| Field | Count | Percent |
| --- | ---: | ---: |
| Country | 2,739,312 | 87.47% |
| Continent | 2,739,312 | 87.47% |
| Subcontinent | 2,739,312 | 87.47% |
| Collection_Year | 2,581,763 | 82.44% |
| Isolation_Source_SD | 1,843,868 | 58.88% |
| Isolation_Source_SD_Broad | 1,739,314 | 55.54% |
| Sample_Type_SD | 1,302,642 | 41.6% |
| Environment_Medium_SD | 497,354 | 15.88% |
| Isolation_Site_SD | 392,233 | 12.52% |
| Host_Disease_SD | 57,831 | 1.85% |
| Host_Health_State_SD | 70,328 | 2.25% |

Raw-present isolation-source standardization: 80.97%.

## Review Queue

Batch 8 resolved 11 previously high-priority disease/clinical/source-context admin-review values. The remaining admin-review table contains 29 values, retained because they are ambiguous without surrounding row context. These are not release blockers and are not forced into deterministic mappings.

Review-signal rows remain triage signals, not hard errors: 149,680 rows across 1,600 unique exact-source review labels. Hard leakage is 0.

## Artifacts

- `final_standardization_freeze_summary.json`
- `source_sample_environment_qa_summary.json`
- `geography_collection_date_qa_summary.json`
- `host_standardization_freeze_qa.json`
- `global_insights_regeneration_summary.json`
- `deployment_readiness_gate.json`
- `admin_review_remaining.tsv`
- `admin_review_resolved_by_batch8.csv`
