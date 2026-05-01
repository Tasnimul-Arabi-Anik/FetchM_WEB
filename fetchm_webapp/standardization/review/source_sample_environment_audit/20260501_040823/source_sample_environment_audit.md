# Source/Sample/Environment Audit

Generated: 2026-05-01T04:14:21.229588+00:00

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,923
- File errors: 0

## Coverage

The raw-present denominator is calculated from the same raw field group only. Values above 100% mean the standardized column was also recovered from other metadata fields.

| Field | Raw present / all rows | Standardized / all rows | Standardized / same-field raw-present rows |
|---|---:|---:|---:|
| Isolation Source | 1,735,193 (66.82%) | 1,488,231 (57.31%) | 85.77% |
| Isolation Site | 31,315 (1.21%) | 781,787 (30.1%) | 2496.53% |
| Sample Type | 249,436 (9.61%) | 1,553,730 (59.83%) | 622.9% |
| Environment Medium | 114,285 (4.4%) | 458,473 (17.65%) | 401.17% |
| Environment (Broad Scale) | 150,739 (5.8%) | 574,409 (22.12%) | 381.06% |
| Environment (Local Scale) | 112,429 (4.33%) | 371,958 (14.32%) | 330.84% |
| Host Disease | 61,402 (2.36%) | 22,793 (0.88%) | 37.12% |
| Host Health State | 2,872 (0.11%) | 2,524 (0.1%) | 87.88% |

## Review Files

- `field_coverage_summary.csv`: standardized coverage by metadata field.
- `top_values_by_field.csv`: high-count values, including unmapped values.
- `suggested_high_confidence_rules.csv`: deterministic suggestions to review before appending rules.