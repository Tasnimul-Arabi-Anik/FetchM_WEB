# Exact Isolation-Source Cleanup Comparison

Canonical snapshot: `20260602T140414Z_genbank_bacteria_root`

| Metric | Before cleanup | Final checkpoint |
| --- | ---: | ---: |
| Rows audited | 3,131,699 | 3,131,699 |
| Hard exact-source leakage rows | 1,215,170 | 0 |
| Review-signal exact cross-field rows | 1,244,575 | 190,284 |
| Review-signal unique values | 2,332 | 2,080 |
| Isolation_Source_SD coverage | 63.38% | 59.07% |
| Isolation_Source_SD_Broad coverage | 58.48% | 55.51% |
| Raw-present standardization | 87.24% | 81.23% |
| Non-approved broad rows | 0 | 0 |
| Controlled-category duplicate/conflict keys | 0/0 | 0/0 |

The coverage reduction is intentional. Sample materials, environmental media,
anatomical sites, and metadata descriptors were removed from exact source fields
and routed or suppressed according to field policy. Review signals remain
vocabulary-triage candidates and are not counted as hard leakage.
