# Source/Sample/Environment Audit

Generated: 2026-05-01T05:38:01.017149+00:00

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,985
- File errors: 0

## Coverage

The raw-present denominator is calculated from the same raw field group only. Values above 100% mean the standardized column was also recovered from other metadata fields.

| Field | Raw present / all rows | Standardized / all rows | Standardized / same-field raw-present rows |
|---|---:|---:|---:|
| Isolation Source | 1,735,249 (66.82%) | 1,490,468 (57.39%) | 85.89% |
| Isolation Site | 31,246 (1.2%) | 781,819 (30.1%) | 2502.14% |
| Sample Type | 249,326 (9.6%) | 1,556,982 (59.95%) | 624.48% |
| Environment Medium | 114,283 (4.4%) | 448,841 (17.28%) | 392.75% |
| Environment (Broad Scale) | 150,740 (5.8%) | 575,516 (22.16%) | 381.79% |
| Environment (Local Scale) | 112,430 (4.33%) | 372,447 (14.34%) | 331.27% |
| Host Disease | 61,401 (2.36%) | 39,424 (1.52%) | 64.21% |
| Host Health State | 2,872 (0.11%) | 2,524 (0.1%) | 87.88% |

## Review Files

- `field_coverage_summary.csv`: standardized coverage by metadata field.
- `top_values_by_field.csv`: high-count values, including unmapped values.
- `suggested_high_confidence_rules.csv`: deterministic suggestions to review before appending rules.