# Source/Sample/Environment Audit

Generated: 2026-04-30T15:01:51.124462+00:00

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,889
- File errors: 0

## Coverage

The raw-present denominator is calculated from the same raw field group only. Values above 100% mean the standardized column was also recovered from other metadata fields.

| Field | Raw present / all rows | Standardized / all rows | Standardized / same-field raw-present rows |
|---|---:|---:|---:|
| Isolation Source | 1,735,463 (66.83%) | 1,489,326 (57.35%) | 85.82% |
| Isolation Site | 31,317 (1.21%) | 781,750 (30.1%) | 2496.25% |
| Sample Type | 249,435 (9.61%) | 1,553,634 (59.83%) | 622.86% |
| Environment Medium | 114,289 (4.4%) | 457,879 (17.63%) | 400.63% |
| Environment (Broad Scale) | 150,909 (5.81%) | 574,390 (22.12%) | 380.62% |
| Environment (Local Scale) | 112,434 (4.33%) | 369,589 (14.23%) | 328.72% |
| Host Disease | 61,404 (2.36%) | 22,236 (0.86%) | 36.21% |
| Host Health State | 2,872 (0.11%) | 2,524 (0.1%) | 87.88% |

## Review Files

- `field_coverage_summary.csv`: standardized coverage by metadata field.
- `top_values_by_field.csv`: high-count values, including unmapped values.
- `suggested_high_confidence_rules.csv`: deterministic suggestions to review before appending rules.