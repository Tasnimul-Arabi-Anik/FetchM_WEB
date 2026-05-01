# Standardization Quality Audit

Generated: 2026-05-01T07:34:42.629707+00:00

## Scope

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,995
- File errors: 0

## Host Standardization

- Host TaxID mapped rows: 1,557,341 (59.97%).
- Host context-recovered rows: 159,579.
- Host rows still requiring review: 776.
- Source-like host values mapped to a host and needing spot-check: 18,133.
- Source-like host values still unmapped and needing routing/review: 4,458.

## Source, Sample, And Environment Routing

- `Sample_Type_SD` present: 1,556,524 (59.94%).
- `Isolation_Source_SD` present: 1,490,254 (57.38%).
- `Environment_Medium_SD` present: 457,845 (17.63%).

## Geography And Time

- Country present: 2,261,258 (87.07%).
- Collection year present: 2,168,863 (83.51%).
- Country-continent mismatch rows: 0.
- Country-subcontinent mismatch rows: 0.

## Priority Review Files

- `suspicious_source_like_mapped_hosts.csv`: mapped host rows where the original host text looks like source/sample material.
- `source_like_unmapped_hosts_for_review.csv`: source/sample/environment-like host values still not routed.
- `top_host_review_needed.csv`: most frequent remaining host values needing review.
- `geography_mismatches.csv`: rows where country disagrees with assigned continent/subcontinent.
- `standardization_quality_summary.csv`: full metric table.
