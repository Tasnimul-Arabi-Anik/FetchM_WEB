# Standardization Quality Audit

Generated: 2026-05-05T03:49:55.279188+00:00

## Scope

- Genus metadata files scanned: 5,066
- Rows scanned: 2,598,486
- File errors: 0

## Host Standardization

- Host TaxID mapped rows: 1,560,116 (60.04%).
- Host context-recovered rows: 160,033.
- Host rows still requiring review: 578.
- Source-like host values mapped to a host and needing spot-check: 18,176.
- Source-like host values still unmapped and needing routing/review: 4,442.

## Source, Sample, And Environment Routing

- `Sample_Type_SD` present: 1,312,914 (50.53%).
- `Isolation_Source_SD` present: 1,488,604 (57.29%).
- `Environment_Medium_SD` present: 204,173 (7.86%).

## Geography And Time

- Country present: 2,263,560 (87.11%).
- Collection year present: 2,170,236 (83.52%).
- Country-continent mismatch rows: 0.
- Country-subcontinent mismatch rows: 0.

## Priority Review Files

- `suspicious_source_like_mapped_hosts.csv`: mapped host rows where the original host text looks like source/sample material.
- `source_like_unmapped_hosts_for_review.csv`: source/sample/environment-like host values still not routed.
- `top_host_review_needed.csv`: most frequent remaining host values needing review.
- `geography_mismatches.csv`: rows where country disagrees with assigned continent/subcontinent.
- `standardization_quality_summary.csv`: full metric table.
