# Source/Sample/Environment Audit

Generated: 2026-05-02T12:23:11.008794+00:00

- Genus metadata files scanned: 5,066
- Rows scanned: 2,597,897
- File errors: 0

## Coverage

The raw-present denominator is calculated from the same raw field group only. Values above 100% mean the standardized column was also recovered from other metadata fields.

| Field | Raw present / all rows | Standardized / all rows | Standardized / same-field raw-present rows |
|---|---:|---:|---:|
| Isolation Source | 1,736,045 (66.83%) | 1,490,899 (57.39%) | 85.88% |
| Isolation Site | 31,246 (1.2%) | 782,068 (30.1%) | 2502.94% |
| Sample Type | 249,325 (9.6%) | 1,357,582 (52.26%) | 544.5% |
| Environment Medium | 114,283 (4.4%) | 204,135 (7.86%) | 178.62% |
| Environment (Broad Scale) | 150,740 (5.8%) | 575,706 (22.16%) | 381.92% |
| Environment (Local Scale) | 112,430 (4.33%) | 372,572 (14.34%) | 331.38% |
| Host Disease | 61,401 (2.36%) | 39,424 (1.52%) | 64.21% |
| Host Health State | 2,872 (0.11%) | 2,524 (0.1%) | 87.88% |

## Review Files

- `field_coverage_summary.csv`: standardized coverage by metadata field.
- `top_values_by_field.csv`: high-count values, including unmapped values.
- `suggested_high_confidence_rules.csv`: deterministic suggestions to review before appending rules.