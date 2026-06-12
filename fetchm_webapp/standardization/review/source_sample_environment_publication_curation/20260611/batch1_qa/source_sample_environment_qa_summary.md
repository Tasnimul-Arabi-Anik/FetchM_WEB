# Source, Sample, and Environment QA Summary

- Status: **pass**
- Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`
- QA timestamp: `2026-06-12T05:11:28.489747+00:00`
- Rows audited: 3,131,699
- Raw isolation source coverage: 2,248,471 (71.8%)
- Isolation_Source_SD coverage: 1,849,780 (59.07%)
- Isolation_Source_SD_Broad coverage: 1,738,396 (55.51%)
- Raw-present standardization: 81.23%
- Raw-only unresolved isolation source rows: 421,948
- Non-approved broad rows: 0
- Controlled-category duplicate/conflict keys: 0/0
- Review-signal exact cross-field values: 2,081 unique; 190,295 rows
- Hard exact-source leakage rows: 0
- Intentional cross-field context rows: 1,760,211
- Material routed to Sample_Type_SD: 1,002,246
- Environment medium routed: 422,173
- Site routed: 4,449
- Metadata descriptor suppressed: 135,112
- Food context preserved: 164,462

Review-signal classifications are vocabulary triage, not error counts. Hard exact leakage is limited to metadata descriptors or exact source values duplicated in a dedicated sample, environment-medium, or isolation-site field. Successful routing is counted separately from intentional source context.
