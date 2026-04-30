# Standardization Quality Audit

Generated: 2026-04-30T16:34:01.000822+00:00

## Scope

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,890
- File errors: 0

## Host Standardization

- Host TaxID mapped rows: 1,558,196 (60.0%).
- Host context-recovered rows: 160,127.
- Host rows still requiring review: 1,139.
- Source-like host values mapped to a host and needing spot-check: 18,159.
- Source-like host values still unmapped and needing routing/review: 4,439.

## Source, Sample, And Environment Routing

- `Sample_Type_SD` present: 1,553,621 (59.83%).
- `Isolation_Source_SD` present: 1,488,358 (57.31%).
- `Environment_Medium_SD` present: 458,393 (17.65%).

## Geography And Time

- Country present: 2,261,154 (87.07%).
- Collection year present: 2,168,791 (83.51%).
- Country-continent mismatch rows: 0.
- Country-subcontinent mismatch rows: 0.

## Priority Review Files

- `suspicious_source_like_mapped_hosts.csv`: mapped host rows where the original host text looks like source/sample material.
- `source_like_unmapped_hosts_for_review.csv`: source/sample/environment-like host values still not routed.
- `top_host_review_needed.csv`: most frequent remaining host values needing review.
- `geography_mismatches.csv`: rows where country disagrees with assigned continent/subcontinent.
- `standardization_quality_summary.csv`: full metric table.
