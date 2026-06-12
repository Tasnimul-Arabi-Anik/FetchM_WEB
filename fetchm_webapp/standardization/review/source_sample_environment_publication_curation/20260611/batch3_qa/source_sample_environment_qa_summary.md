# Source, Sample, and Environment QA Summary

- Status: **pass**
- Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`
- QA timestamp: `2026-06-12T16:04:00.596909+00:00`
- Rows audited: 3,131,699
- Raw isolation source coverage: 2,248,471 (71.8%)
- Isolation_Source_SD coverage: 1,839,616 (58.74%)
- Isolation_Source_SD_Broad coverage: 1,731,858 (55.3%)
- Raw-present standardization: 80.78%
- Raw-only unresolved isolation source rows: 432,090
- Non-approved broad rows: 0
- Controlled-category duplicate/conflict keys: 0/0
- Review-signal exact cross-field values: 1,609 unique; 150,659 rows
- Hard exact-source leakage rows: 0
- Intentional cross-field context rows: 1,814,998
- Material routed to Sample_Type_SD: 1,054,998
- Environment medium routed: 422,911
- Site routed: 3,952
- Metadata descriptor suppressed: 132,452
- Food context preserved: 168,924

Review-signal classifications are vocabulary triage, not error counts. Hard exact leakage is limited to metadata descriptors or exact source values duplicated in a dedicated sample, environment-medium, or isolation-site field. Successful routing is counted separately from intentional source context.
