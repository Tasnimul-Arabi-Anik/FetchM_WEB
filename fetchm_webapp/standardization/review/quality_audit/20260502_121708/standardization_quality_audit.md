# Standardization Quality Audit

Generated: 2026-05-02T12:20:42.230312+00:00

## Scope

- Genus metadata files scanned: 5,066
- Rows scanned: 2,597,897
- File errors: 0

## Host Standardization

- Host TaxID mapped rows: 1,559,238 (60.02%).
- Host context-recovered rows: 159,648.
- Host rows still requiring review: 780.
- Source-like host values mapped to a host and needing spot-check: 18,149.
- Source-like host values still unmapped and needing routing/review: 4,443.

## Source, Sample, And Environment Routing

- `Sample_Type_SD` present: 1,357,582 (52.26%).
- `Isolation_Source_SD` present: 1,490,899 (57.39%).
- `Environment_Medium_SD` present: 204,135 (7.86%).

## Geography And Time

- Country present: 2,262,820 (87.1%).
- Collection year present: 2,169,741 (83.52%).
- Country-continent mismatch rows: 0.
- Country-subcontinent mismatch rows: 0.

## Priority Review Files

- `suspicious_source_like_mapped_hosts.csv`: mapped host rows where the original host text looks like source/sample material.
- `source_like_unmapped_hosts_for_review.csv`: source/sample/environment-like host values still not routed.
- `top_host_review_needed.csv`: most frequent remaining host values needing review.
- `geography_mismatches.csv`: rows where country disagrees with assigned continent/subcontinent.
- `standardization_quality_summary.csv`: full metric table.
