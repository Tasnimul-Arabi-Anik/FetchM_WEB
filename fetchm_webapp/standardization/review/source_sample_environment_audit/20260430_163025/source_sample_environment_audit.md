# Source/Sample/Environment Audit

Generated: 2026-04-30T16:36:22.454139+00:00

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,890
- File errors: 0

## Coverage

The raw-present denominator is calculated from the same raw field group only. Values above 100% mean the standardized column was also recovered from other metadata fields.

| Field | Raw present / all rows | Standardized / all rows | Standardized / same-field raw-present rows |
|---|---:|---:|---:|
| Isolation Source | 1,735,465 (66.83%) | 1,488,358 (57.31%) | 85.76% |
| Isolation Site | 31,317 (1.21%) | 781,785 (30.1%) | 2496.36% |
| Sample Type | 249,434 (9.61%) | 1,553,621 (59.83%) | 622.86% |
| Environment Medium | 114,288 (4.4%) | 458,393 (17.65%) | 401.09% |
| Environment (Broad Scale) | 150,908 (5.81%) | 574,408 (22.12%) | 380.63% |
| Environment (Local Scale) | 112,433 (4.33%) | 369,631 (14.23%) | 328.76% |
| Host Disease | 61,404 (2.36%) | 22,236 (0.86%) | 36.21% |
| Host Health State | 2,872 (0.11%) | 2,524 (0.1%) | 87.88% |

## Review Files

- `field_coverage_summary.csv`: standardized coverage by metadata field.
- `top_values_by_field.csv`: high-count values, including unmapped values.
- `suggested_high_confidence_rules.csv`: deterministic suggestions to review before appending rules.