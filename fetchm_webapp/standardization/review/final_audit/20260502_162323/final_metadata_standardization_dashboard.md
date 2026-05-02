# Final Metadata Standardization Dashboard

Generated: 2026-05-02T16:25:17.826744+00:00
Git commit: `unknown`

## Scope

- Files scanned: 5,066
- Rows scanned: 2,598,486
- File errors: 0

## Broad Vocabulary QA

- Unique `Isolation_Source_SD_Broad` values: 109
- Noisy/non-approved broad values: 62
- Rows represented by noisy broad values: 14,781
- Body-site leakage values: 520
- Disease/source leakage values: 209
- Raw code/text leakage values: 374

## Review Files

- `bad_isolation_source_broad_values.csv`
- `body_site_misrouted_rules.csv`
- `host_disease_source_leakage_audit.csv`
- `raw_code_leakage_audit.csv`
- `broad_vocabulary_compression_report.csv`
- `final_refinement_summary.csv`

## Release Note

Metadata standardization is production-ready for genus-level FetchM outputs. Remaining issues are long-tail curation of broad source categories and low-frequency host names.
