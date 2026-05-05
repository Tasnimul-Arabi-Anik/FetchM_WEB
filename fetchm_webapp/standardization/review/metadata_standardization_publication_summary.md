# Metadata Standardization Publication Summary

Last updated: 2026-05-05

This file summarizes the manuscript-relevant FetchM WEB metadata standardization improvements. Detailed audit CSVs and Markdown reports are preserved under `fetchm_webapp/standardization/review/`.

## Final Genus-Level Evaluation

Latest final audit:

- `final_audit/20260505_100958/final_metadata_standardization_dashboard.md`
- `final_audit/20260505_100958/production_readiness_gate.json`
- `quality_audit/20260505_100143/standardization_quality_summary.csv`
- `source_sample_environment_audit/20260505_100143/field_coverage_summary.csv`

Scope:

- Genus metadata files scanned: 5,066
- Genus metadata rows scanned: 2,598,486
- File errors: 0
- Production readiness gate: passed
- Regression tests: 8 passed, 0 failed

## Final Quality Metrics

| Field or QA check | Final result |
| --- | ---: |
| Country present | 2,263,560 rows; 87.11% |
| Country-continent mismatch | 0 |
| Country-subcontinent mismatch | 0 |
| Non-country values in `Country` | 0 |
| Collection year present | 2,170,236 rows; 83.52% |
| Host TaxID mapped | 1,560,044 rows; 60.04% |
| Host context recovered | 159,883 rows |
| Host still requiring review | 484 rows |
| Sample_Type_SD present | 1,312,920 rows; 50.53% |
| Invalid host-like Sample_Type_SD rows | 0 |
| Isolation_Source_SD present | 1,488,608 rows; 57.29% |
| Isolation Source raw-present standardization | 85.73% |
| Isolation_Site_SD present | 833,794 rows; 32.09% |
| Environment_Medium_SD present | 206,569 rows; 7.95% |
| Environment Broad Scale present | 575,837 rows; 22.16% |
| Environment Local Scale present | 372,678 rows; 14.34% |
| Host Disease raw-present standardization | 90.59% |
| Unique Isolation_Source_SD_Broad values | 43 |
| Noisy/non-approved broad-source rows | 0 |
| Body-site leakage values | 0 |
| Disease/source leakage values | 0 |
| Raw code/text leakage values | 0 |
| Controlled-category duplicate keys | 0 |
| Controlled-category conflict keys | 0 |

## Major Improvements Implemented

### 1. Raw Metadata Preservation Plus Standardized Derived Columns

FetchM WEB preserves original NCBI/BioSample values and writes standardized values into separate derived columns such as `Host_SD`, `Host_TaxID`, `Sample_Type_SD`, `Isolation_Source_SD`, `Environment_Medium_SD`, `Country`, `Continent`, `Subcontinent`, and `Collection_Year`.

This design preserves traceability while enabling downstream comparative analyses across inconsistent BioSample metadata.

### 2. Geography Standardization

Final geography standardization uses controlled country/territory/marine-region validation and false-positive blocking.

Resolved failure modes:

- Hospital/clinic strings no longer enter standardized `Country`.
- `ground turkey` no longer maps to Turkey.
- `Guinea pig` no longer maps to Guinea.
- `Norway rat` no longer maps to Norway.
- `Aspergillus niger` no longer maps to Niger.

Final geography result:

- Country coverage: 87.11%
- Continent/subcontinent mismatch: 0
- Non-country values in `Country`: 0

### 3. Collection Year Recovery

Collection-year standardization handles primary collection-date aliases, partial dates, and conservative secondary text recovery. It avoids protocol/publication/accession strings and future/impossible dates.

Final result:

- Collection year coverage: 83.52%

### 4. Host Standardization

Host standardization now combines:

- exact and synonym dictionary rules
- NCBI/TaxonKit-validated taxonomy rules
- broad host-rank mappings for common names where species-level inference is unsafe
- negative host rules for non-host source/sample/environment terms
- source-like host routing review outputs
- host lineage columns for taxonomic structure analysis

Host output supports species-level and broad-rank host assignments without forcing all values to species level.

Final result:

- Host TaxID mapped: 1,560,044 rows
- Host review needed: 484 rows
- Remaining review values are low-frequency and mostly ambiguous, abbreviated, mixed-taxonomic, or TaxonKit-unresolved values.

### 5. Source, Sample, Environment, And Site Separation

FetchM WEB now distinguishes:

- sample material, such as blood, urine, sputum, feces/stool, rectal swab, nasal swab, tissue, and milk
- environmental medium, such as soil, water, wastewater, sediment, seawater, freshwater, air, and biofilm
- source context, such as healthcare facility, food/food product, agricultural environment, and host-associated context
- anatomical site, such as rectum/perianal region, nasal cavity, oral cavity, urogenital tract, gastrointestinal tract, skin/body surface, bronch/lung, and pleural cavity

Important corrected leakage:

- `feces/stool` is no longer treated as `Environment_Medium_SD` unless environmental/manure context is explicit.
- host-only words such as human, patient, animal, cattle, pig, chicken, poultry, and plant are blocked from `Sample_Type_SD`.
- body-site terms are routed away from broad source fields.
- disease terms are routed away from sample/source fields.
- raw codes and spreadsheet errors are excluded from standardized broad categories.

### 6. Controlled Broad Vocabulary

Broad standardized fields are constrained by an approved broad vocabulary. The final audit reports:

- Unique `Isolation_Source_SD_Broad` values: 43
- Noisy/non-approved broad-source rows: 0
- Body-site leakage values: 0
- Disease/source leakage values: 0
- Raw code/text leakage values: 0

### 7. Reproducible Rule Files

Approved deterministic rules are stored in committed CSV files:

- `standardization/host_synonyms.csv`
- `standardization/host_negative_rules.csv`
- `standardization/controlled_categories.csv`
- `standardization/approved_broad_categories.csv`
- `standardization/geography_reviewed_rules.csv`

This makes the standardization reproducible from code and rule files rather than dependent on local SQLite state.

### 8. QA Gates And Regression Tests

The production-readiness gate fails on hard semantic regressions, including:

- file errors
- non-country values in `Country`
- geography mismatches
- invalid host-like sample types
- noisy broad-source values
- body-site leakage
- disease/source leakage
- raw code/text leakage
- controlled-category duplicate or conflicting keys
- regression-test failures

Final gate:

- Production ready: yes
- Hard failures: 0
- Warnings: 0
- Regression tests: 8 passed

## Before/After Milestones

| Milestone | Result |
| --- | --- |
| Source-like broad-source noise cleanup | noisy broad-source rows reduced from 14,781 to 0 |
| Host long-tail curation after final production pass | host review needed reduced from 550 to 484 |
| Deterministic curation batches before long-tail pass | host review needed reduced from 1,139 to 776, then to 550 |
| Environment medium policy cleanup | `feces/stool` removed from Environment_Medium_SD leakage |
| Country false-positive cleanup | hospital/clinic and animal/food false-positive country values removed; mismatch count stayed 0 |
| Controlled-category reproducibility | `controlled_categories.csv` exported and committed; duplicate/conflict keys 0 |

## Interpretation For Manuscript

FetchM WEB demonstrates that large-scale BioSample metadata can be standardized reproducibly using a deterministic, auditable workflow. The system improves usability without overwriting raw metadata, supports provenance-aware derived fields, and avoids common false-positive mappings caused by ambiguous biological, geographic, clinical, food, and environmental text.

The remaining unresolved values are low-frequency long-tail cases. They are intentionally left as review-needed when deterministic evidence is insufficient, rather than being forced into potentially incorrect standardized categories.

## Recommended Wording

Possible manuscript statement:

> FetchM WEB preserves original NCBI/BioSample metadata while generating standardized derived metadata fields through reproducible dictionary, ontology, taxonomy, and conservative rule-based curation. In the final genus-level audit of 5,066 metadata files and 2,598,486 rows, the standardization pipeline produced zero file errors, zero country-continent/subcontinent mismatches, zero non-country values in the standardized country field, zero invalid host-like sample-type assignments, and zero noisy/non-approved broad-source values. The production-readiness gate passed with all regression tests successful.

## Remaining Future Work

- Continue low-frequency host curation for the remaining 484 review-needed rows.
- Use embeddings or BGE only as a review-assistant for clustering unresolved free-text values, not as automatic production standardization.
- Extend the same final audit/gate framework to species-level outputs if species-level publication claims are needed.
