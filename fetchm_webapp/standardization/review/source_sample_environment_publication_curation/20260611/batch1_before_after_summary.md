# Batch 1 metadata descriptor curation

- Canonical rows audited: 3,131,699
- QA commit: `8b140eb`
- QA status: **pass**
- Regression tests: **69 passed**

## Review decisions

- Batch 1 review-signal candidates: 1,180 values / 3,911 rows
- Safe suppression rules: 4 values / 28,304 exact raw occurrences (preventive counts, not review-signal rows)
- Context-bearing keep/review: 3 values / 2,250 rows
- Ontology-code manual review: 19 values / 165 rows
- Unresolved low priority: 1,158 values / 1,496 rows

## QA before and after

| Metric | Before | After |
|---|---:|---:|
| Hard exact leakage | 0 | 0 |
| Broad leakage | 0 | 0 |
| Review-signal rows | 190,284 | 190,295 |
| Review-signal values | 2,080 | 2,081 |
| Metadata descriptors suppressed | 135,111 | 135,112 |
| Sample type present | 1,252,191 | 1,215,095 |

The 11 additional review-signal rows are legitimate sample-material signals exposed after generic descriptor sentinels were removed. They are not hard leakage. Clinical, respiratory, environmental, ontology, and culture/process context was preserved for later context-aware review.
