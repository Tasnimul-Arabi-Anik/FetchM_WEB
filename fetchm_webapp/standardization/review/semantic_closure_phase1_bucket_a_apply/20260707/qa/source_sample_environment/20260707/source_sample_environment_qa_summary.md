# Source, Sample, and Environment QA Summary

- Status: **pass**
- Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`
- QA timestamp: `2026-07-07T10:26:45.401741+00:00`
- Rows audited: 3,131,699
- Raw isolation source coverage: 2,248,471 (71.8%)
- Isolation_Source_SD coverage: 1,843,868 (58.88%)
- Isolation_Source_SD_Broad coverage: 1,739,314 (55.54%)
- Raw-present standardization: 80.97%
- Raw-only unresolved isolation source rows: 427,838
- Non-approved broad rows: 0
- Controlled-category duplicate/conflict keys: 0/0
- Review-signal exact cross-field values: 1,600 unique; 149,680 rows
- Hard exact-source leakage rows: 0
- Intentional cross-field context rows: 1,823,226
- Material routed to Sample_Type_SD: 1,057,286
- Environment medium routed: 429,763
- Site routed: 3,158
- Metadata descriptor suppressed: 127,799
- Food context preserved: 176,590

Review-signal classifications are vocabulary triage, not error counts. Hard exact leakage is limited to metadata descriptors or exact source values duplicated in a dedicated sample, environment-medium, or isolation-site field. Successful routing is counted separately from intentional source context.
