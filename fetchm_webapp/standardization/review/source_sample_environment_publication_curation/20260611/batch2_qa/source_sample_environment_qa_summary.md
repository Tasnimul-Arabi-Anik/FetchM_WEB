# Source, Sample, and Environment QA Summary

- Status: **pass**
- Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`
- QA timestamp: `2026-06-12T08:14:24.548115+00:00`
- Rows audited: 3,131,699
- Raw isolation source coverage: 2,248,471 (71.8%)
- Isolation_Source_SD coverage: 1,843,535 (58.87%)
- Isolation_Source_SD_Broad coverage: 1,735,530 (55.42%)
- Raw-present standardization: 80.96%
- Raw-only unresolved isolation source rows: 428,195
- Non-approved broad rows: 0
- Controlled-category duplicate/conflict keys: 0/0
- Review-signal exact cross-field values: 1,615 unique; 147,632 rows
- Hard exact-source leakage rows: 0
- Intentional cross-field context rows: 1,811,342
- Material routed to Sample_Type_SD: 1,054,829
- Environment medium routed: 422,172
- Site routed: 3,996
- Metadata descriptor suppressed: 133,782
- Food context preserved: 164,796

Review-signal classifications are vocabulary triage, not error counts. Hard exact leakage is limited to metadata descriptors or exact source values duplicated in a dedicated sample, environment-medium, or isolation-site field. Successful routing is counted separately from intentional source context.
