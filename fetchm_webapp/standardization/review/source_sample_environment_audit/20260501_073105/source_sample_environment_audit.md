# Source/Sample/Environment Audit

Generated: 2026-05-01T07:36:57.756595+00:00

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,995
- File errors: 0

## Coverage

The raw-present denominator is calculated from the same raw field group only. Values above 100% mean the standardized column was also recovered from other metadata fields.

| Field | Raw present / all rows | Standardized / all rows | Standardized / same-field raw-present rows |
|---|---:|---:|---:|
| Isolation Source | 1,735,254 (66.82%) | 1,490,254 (57.38%) | 85.88% |
| Isolation Site | 31,246 (1.2%) | 781,820 (30.1%) | 2502.14% |
| Sample Type | 249,326 (9.6%) | 1,556,524 (59.94%) | 624.29% |
| Environment Medium | 114,283 (4.4%) | 457,845 (17.63%) | 400.62% |
| Environment (Broad Scale) | 150,740 (5.8%) | 575,521 (22.16%) | 381.8% |
| Environment (Local Scale) | 112,430 (4.33%) | 372,450 (14.34%) | 331.27% |
| Host Disease | 61,401 (2.36%) | 39,424 (1.52%) | 64.21% |
| Host Health State | 2,872 (0.11%) | 2,524 (0.1%) | 87.88% |

## Review Files

- `field_coverage_summary.csv`: standardized coverage by metadata field.
- `top_values_by_field.csv`: high-count values, including unmapped values.
- `suggested_high_confidence_rules.csv`: deterministic suggestions to review before appending rules.