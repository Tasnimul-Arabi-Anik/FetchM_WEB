# Standardization Quality Audit

Generated: 2026-04-30T12:51:41.710961+00:00

## Scope

- Genus metadata files scanned: 5,066
- Rows scanned: 2,596,841
- File errors: 0

## Host Standardization

- Host TaxID mapped rows: 1,556,775 (59.95%).
- Host context-recovered rows: 159,392.
- Host rows still requiring review: 1,360.
- Source-like host values mapped to a host and needing spot-check: 18,032.
- Source-like host values still unmapped and needing routing/review: 4,478.

## Source, Sample, And Environment Routing

- `Sample_Type_SD` present: 1,579,301 (60.82%).
- `Isolation_Source_SD` present: 1,489,208 (57.35%).
- `Environment_Medium_SD` present: 455,568 (17.54%).

## Geography And Time

- Country present: 2,261,105 (87.07%).
- Collection year present: 2,168,776 (83.52%).
- Country-continent mismatch rows: 0.
- Country-subcontinent mismatch rows: 0.

## Priority Review Files

- `suspicious_source_like_mapped_hosts.csv`: mapped host rows where the original host text looks like source/sample material.
- `source_like_unmapped_hosts_for_review.csv`: source/sample/environment-like host values still not routed.
- `top_host_review_needed.csv`: most frequent remaining host values needing review.
- `geography_mismatches.csv`: rows where country disagrees with assigned continent/subcontinent.
- `standardization_quality_summary.csv`: full metric table.
