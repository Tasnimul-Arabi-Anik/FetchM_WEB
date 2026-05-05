# Final Metadata Standardization Dashboard

Generated: 2026-05-05T10:12:04.919606+00:00
Git commit SHA: `637443fd59b9d67cabce9248387a0550ef7faddc`
Git branch: `main`
Docker image tag: `unknown`
Code version: `637443fd59b9`
Worker state: `standardization_completed`

## Scope

- Files scanned: 5,066
- Rows scanned: 2,598,486
- File errors: 0

## Geography

- Country coverage: 87.11%
- Non-country values in `Country`: 0
- Country-continent mismatches: 0
- Country-subcontinent mismatches: 0

## Time

- Collection year coverage: 83.52%
- Invalid/future collection year count: not_available

## Host

- Host TaxID mapped: 1560044 rows (60.04%)
- Host context recovered: 159883
- Host review needed: 484
- Host `non_host_source`: 7808
- Host `not_identifiable`: 545
- Source-like mapped host spot-check rows: 17944
- Source-like unmapped host/source rows: 4461

## Source/Sample/Environment

- `Sample_Type_SD` coverage: 50.53%
- Invalid host-like `Sample_Type_SD`: 0
- `Isolation_Source_SD` coverage: 57.29%
- Isolation Source raw-present standardization: 85.73%
- `Isolation_Site_SD` coverage: 32.09%
- `Environment_Medium_SD` coverage: 7.95%
- Environment Broad Scale coverage: 22.16%
- Environment Local Scale coverage: 14.34%
- Host Disease raw-present standardization: 90.59%
- Host Health State recovery/raw-present ratio: 2562.71%

## Broad Vocabulary QA

- Unique `Isolation_Source_SD_Broad` values: 43
- Noisy/non-approved broad values: 0
- Rows represented by noisy broad values: 0
- Body-site leakage values: 0
- Disease/source leakage values: 0
- Raw code/text leakage values: 0

## Rule QA

- Controlled-category duplicate keys: 0
- Controlled-category conflict keys: 0
- Controlled-category suspicious rows: 20

## Regression

- Status: passed
- Tests run: 8
- Tests failed: 0

## Final Judgement

- Production-ready major fields: yes
- Remaining curation: host low-frequency/source-like review

## Review Files

- `bad_isolation_source_broad_values.csv`
- `body_site_misrouted_rules.csv`
- `host_disease_source_leakage_audit.csv`
- `raw_code_leakage_audit.csv`
- `broad_vocabulary_compression_report.csv`
- `final_refinement_summary.csv`
- `production_readiness_gate.md`
- `production_readiness_gate.json`

## Release Note

Metadata standardization is production-ready for genus-level FetchM outputs. Remaining issues are long-tail curation of broad source categories and low-frequency host names.
