# Host Final Review Batch 2

Generated: 2026-05-04T18:59:45.716436+00:00
Applied rules: yes

## Scope

- Unique values reviewed: 619
- Rows represented: 782
- Host synonym rules proposed: 126
- Host negative rules proposed: 12
- Host synonym rules appended: 126
- Host negative rules appended: 12

## Cluster Summary

| Cluster | Unique values | Rows represented |
|---|---:|---:|
| ambiguous_manual_review | 481 | 587 |
| exact_taxonkit_scientific_name | 107 | 118 |
| common_animal_or_broad_host_name | 19 | 65 |
| microbial_or_viral_not_host | 11 | 11 |
| food_or_product_not_host | 1 | 1 |

## Policy

Only exact Eukaryota TaxonKit matches, reviewed broad/common host names, and reviewed negative host descriptors are applied.
Bacterial, archaeal, and viral exact matches are blocked from Host_SD because they are usually isolate/pathogen/lab descriptors in this bacterial-genome metadata context.
Ambiguous, malformed, hybrid, or lineage-unresolved values remain in manual review.
