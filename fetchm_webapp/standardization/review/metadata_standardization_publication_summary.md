# Metadata Standardization Publication Summary

Last updated: 2026-06-16

This file summarizes the manuscript-relevant FetchM WEB metadata standardization state. Detailed audit CSVs and Markdown reports are preserved under `fetchm_webapp/standardization/review/`.

## Final Canonical Freeze Checkpoint

Latest canonical freeze artifacts:

- `final_standardization_freeze/20260616/final_standardization_freeze_summary.md`
- `final_standardization_freeze/20260616/deployment_readiness_gate.json`
- `final_standardization_freeze/20260616/source_sample_environment_qa_summary.json`
- `final_standardization_freeze/20260616/geography_collection_date_qa_summary.json`
- `final_standardization_freeze/20260616/host_standardization_freeze_qa.json`
- `final_standardization_freeze/20260616/global_insights_regeneration_summary.json`

Scope:

- Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`
- Canonical rows audited: 3,131,699
- Global Insights snapshot: `20260616T114556Z_global_insights`
- Source/sample/environment QA: pass
- Geography/date QA: pass
- Host QA: pass
- Deployment readiness gate: technical checks pass; deployment remains manual

## Final Quality Metrics

| Field or QA check | Final canonical result |
| --- | ---: |
| Country present | 2,739,312 rows; 87.47% |
| Continent present | 2,739,312 rows; 87.47% |
| Subcontinent present | 2,739,312 rows; 87.47% |
| Country-continent mismatch | 0 |
| Country-subcontinent mismatch | 0 |
| Non-country values in `Country` | 0 |
| Collection_Year present | 2,581,763 rows; 82.44% |
| Invalid/future/impossible collection years | 0 |
| Isolation_Source_SD present | 1,843,868 rows; 58.88% |
| Isolation_Source_SD_Broad present | 1,739,314 rows; 55.54% |
| Isolation Source raw-present standardization | 80.97% |
| Sample_Type_SD present | 1,302,642 rows; 41.6% |
| Environment_Medium_SD present | 497,354 rows; 15.88% |
| Environment Broad Scale present | 981,225 rows; 31.33% |
| Environment Local Scale present | 621,795 rows; 19.85% |
| Isolation_Site_SD present | 392,233 rows; 12.52% |
| Host_Disease_SD present | 57,831 rows; 1.85% |
| Host_Health_State_SD present | 70,328 rows; 2.25% |
| Source hard exact leakage rows | 0 |
| Source broad vocabulary leakage rows | 0 |
| Controlled-category duplicate keys | 0 |
| Controlled-category conflict keys | 0 |
| Host microbial leakage | 0 |
| Admin-review blockers | 0 |

## Field Policy

FetchM WEB preserves original NCBI/BioSample metadata and writes standardized values into separate derived columns. Standardization does not overwrite raw metadata.

- `Host_SD` is a resolved taxonomic host with `Host_TaxID` when available. Bacterial, archaeal, and viral host-field values do not populate `Host_SD` unless allowlisted. Eukaryotic algae, fungi, protists, plants, and animals remain valid hosts when lineage supports them.
- `Country`, `Continent`, `Subcontinent`, and `Collection_Year` are conservative derived fields from public repository metadata. They represent submitted metadata, not biological prevalence.
- Sample materials route to `Sample_Type_SD`. Anatomical/body-site terms route to `Isolation_Site_SD`. Environmental media route to `Environment_Medium_SD`.
- Food, agricultural, healthcare, host-associated, disease, health-state, surveillance, and outbreak contexts are separated conservatively instead of being forced into a single source field.
- Ambiguous values stay in admin review unless deterministic context supports a rule. Remaining review signals are triage signals, not error counts.

## Major Improvements Implemented

- Host standardization now uses committed synonym, negative, context, and microbial-allowlist rule files with lineage-aware QA.
- Geography/date standardization has canonical QA with 0 non-country values, 0 country-continent/subcontinent mismatches, and 0 invalid/future/impossible collection years.
- Source/sample/environment standardization now separates exact source context from specimen material, environment medium, body site, food/commodity context, source-derived host context, disease, and health state.
- Batch 8 resolved the high-priority disease/clinical/source-context admin-review values without converting generic clinical, outbreak, surveillance, contaminated-food, carrier, or colonized labels into false disease calls.
- Global Insights was regenerated from the refreshed canonical standardization snapshot and records source/sample/environment provenance.

## QA Gates And Regression Tests

The final gate fails on hard semantic regressions, including microbial host leakage, geography mismatches, invalid collection years, source hard leakage, broad vocabulary leakage, duplicate/conflicting controlled-category keys, and missing Global Insights regeneration.

Final deployment gate result:

- Controlled-category audit: pass
- Canonical refresh: pass
- Global Insights regenerated: pass
- Hard leakage: 0
- Broad leakage: 0
- Duplicate/conflict keys: 0 / 0
- Admin-review blockers: 0
- Deployment: manual by policy

## Historical Genus-Level Evaluation

The May 2026 genus-level audit remains useful as historical provenance, but it is no longer the final production checkpoint. The final production checkpoint is the canonical 3,131,699-row freeze above.

Historical artifacts:

- `final_audit/20260505_100958/final_metadata_standardization_dashboard.md`
- `final_audit/20260505_100958/production_readiness_gate.json`
- `quality_audit/20260505_100143/standardization_quality_summary.csv`
- `source_sample_environment_audit/20260505_100143/field_coverage_summary.csv`

## Recommended Wording

Possible manuscript statement:

> FetchM WEB preserves original NCBI/BioSample metadata while generating standardized derived metadata fields through reproducible rule files, taxonomy validation, and conservative deterministic curation. In the final canonical bacterial metadata audit of 3,131,699 rows, the standardization pipeline produced zero non-country values in the standardized country field, zero country-continent/subcontinent mismatches, zero invalid/future/impossible collection years, zero non-allowlisted microbial host leakage, zero source hard exact leakage, zero broad-source vocabulary leakage, and zero duplicate or conflicting controlled-category keys. Ambiguous metadata values were retained in an admin-review queue rather than forced into potentially incorrect standardized categories.

## Remaining Future Work

- Continue optional low-priority admin review for ambiguous source/context terms with row-level examples.
- Use embeddings or language models only as review assistants for clustering unresolved free text, not as automatic production standardization.
- Run a new canonical refresh and Global Insights regeneration only when rule files or source metadata change.
