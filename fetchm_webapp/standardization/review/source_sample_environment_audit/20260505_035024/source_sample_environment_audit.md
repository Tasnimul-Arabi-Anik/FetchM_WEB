# Source/Sample/Environment Audit

Generated: 2026-05-05T03:56:14.762972+00:00

- Genus metadata files scanned: 5,066
- Rows scanned: 2,598,486
- File errors: 0

## Coverage

The raw-present denominator is calculated from the same raw field group only. Values above 100% mean the standardized column was also recovered from other metadata fields.

| Field | Raw present / all rows | Standardized / all rows | Standardized / same-field raw-present rows |
|---|---:|---:|---:|
| Isolation Source | 1,736,490 (66.83%) | 1,488,604 (57.29%) | 85.72% |
| Isolation Site | 31,246 (1.2%) | 833,794 (32.09%) | 2668.48% |
| Sample Type | 249,325 (9.6%) | 1,312,914 (50.53%) | 526.59% |
| Environment Medium | 114,283 (4.4%) | 204,173 (7.86%) | 178.66% |
| Environment (Broad Scale) | 150,740 (5.8%) | 575,837 (22.16%) | 382.01% |
| Environment (Local Scale) | 112,430 (4.33%) | 372,678 (14.34%) | 331.48% |
| Host Disease | 61,401 (2.36%) | 55,624 (2.14%) | 90.59% |
| Host Health State | 2,872 (0.11%) | 73,601 (2.83%) | 2562.71% |

## Review Files

- `field_coverage_summary.csv`: standardized coverage by metadata field.
- `top_values_by_field.csv`: high-count values, including unmapped values.
- `suggested_high_confidence_rules.csv`: deterministic suggestions to review before appending rules.