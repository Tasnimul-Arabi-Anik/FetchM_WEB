# Batch 2 Sample Material Curation

Batch 2 refreshed and audited all 3,131,699 canonical bacterial metadata rows using implementation commit `81b836d`.

## Result

- QA status: pass
- Regression tests: 70 passed
- Hard exact-source leakage: 0 before and after
- Broad vocabulary leakage: 0 before and after
- Rule duplicate/conflict keys: 0/0
- File errors: 0

## Before And After

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Sample_Type_SD rows | 1,215,095 (38.80%) | 1,406,917 (44.93%) | +191,822 |
| Material routed to sample type | 1,002,246 | 1,054,829 | +52,583 |
| Isolation_Site_SD rows | 402,225 | 404,239 | +2,014 |
| Review-signal rows | 190,295 | 147,632 | -42,663 |
| Review-signal unique values | 2,081 | 1,615 | -466 |
| Isolation_Source_SD rows | 1,849,780 (59.07%) | 1,843,535 (58.87%) | -6,245 |
| Isolation_Source_SD_Broad rows | 1,738,396 (55.51%) | 1,735,530 (55.42%) | -2,866 |
| Raw-present source standardization | 81.23% | 80.96% | -0.27 pp |

The small source-coverage reduction is intentional: material-only values now populate `Sample_Type_SD` rather than being counted as exact biological sources.

## Review Decisions

The refreshed post-routing reviewer artifact contains 101 remaining values affecting 14,260 rows:

- 88 values / 13,490 rows remain sample-type review signals.
- 8 values / 39 rows remain split sample-type and anatomical-site signals.
- 2 values / 727 rows preserve explicit food context.
- 1 value / 1 row preserves plant-material context.
- 2 values / 3 rows remain unresolved.

`cerebrospinal` and `Cerebrospinal` remain unresolved because neither explicitly identifies cerebrospinal fluid. No speculative rule was added.

## Context Preservation

Clinical and respiratory aggregate context remains available. Explicit food and dairy phrases retain food context, while `milk` alone is treated as a specimen. Plant tissue retains plant-associated context. Environmental material remains deferred to the environmental-medium batch.

Review signals remain vocabulary triage and must not be interpreted as error counts.
