# Source/Sample/Environment Audit

Generated: 2026-04-30T13:26:04.449918+00:00

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,841
- File errors: 0

## Coverage

The raw-present denominator is calculated from the same raw field group only. Values above 100% mean the standardized column was also recovered from other metadata fields.

| Field | Raw present / all rows | Standardized / all rows | Standardized / same-field raw-present rows |
|---|---:|---:|---:|
| Isolation Source | 1,735,415 (66.83%) | 1,489,208 (57.35%) | 85.81% |
| Isolation Site | 31,317 (1.21%) | 782,649 (30.14%) | 2499.12% |
| Sample Type | 249,435 (9.61%) | 1,579,301 (60.82%) | 633.15% |
| Environment Medium | 114,289 (4.4%) | 455,568 (17.54%) | 398.61% |
| Environment (Broad Scale) | 150,909 (5.81%) | 795,363 (30.63%) | 527.05% |
| Environment (Local Scale) | 112,434 (4.33%) | 369,580 (14.23%) | 328.71% |
| Host Disease | 61,404 (2.36%) | 22,236 (0.86%) | 36.21% |
| Host Health State | 2,872 (0.11%) | 2,524 (0.1%) | 87.88% |

## Review Files

- `field_coverage_summary.csv`: standardized coverage by metadata field.
- `top_values_by_field.csv`: high-count values, including unmapped values.
- `suggested_high_confidence_rules.csv`: deterministic suggestions to review before appending rules.