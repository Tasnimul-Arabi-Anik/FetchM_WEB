# Standardization Quality Audit

Generated: 2026-04-30T10:09:45.584376+00:00

## Scope

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,647
- File errors: 0

## Host Standardization

- Host TaxID mapped rows: 1,559,323 (60.05%).
- Host context-recovered rows: 162,473.
- Host rows still requiring review: 1,341.
- Source-like host values mapped to a host and needing spot-check: 19,009.
- Source-like host values still unmapped and needing routing/review: 4,448.

## Source, Sample, And Environment Routing

- `Sample_Type_SD` present: 1,578,165 (60.78%).
- `Isolation_Source_SD` present: 1,489,498 (57.36%).
- `Environment_Medium_SD` present: 416,607 (16.04%).

## Geography And Time

- Country present: 2,260,930 (87.07%).
- Collection year present: 2,168,602 (83.52%).
- Country-continent mismatch rows: 0.
- Country-subcontinent mismatch rows: 0.

## Priority Review Files

- `suspicious_source_like_mapped_hosts.csv`: mapped host rows where the original host text looks like source/sample material.
- `source_like_unmapped_hosts_for_review.csv`: source/sample/environment-like host values still not routed.
- `top_host_review_needed.csv`: most frequent remaining host values needing review.
- `geography_mismatches.csv`: rows where country disagrees with assigned continent/subcontinent.
- `standardization_quality_summary.csv`: full metric table.
