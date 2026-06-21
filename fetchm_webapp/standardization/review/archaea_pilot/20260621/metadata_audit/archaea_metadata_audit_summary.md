# Hidden Archaea Metadata Standardization Audit

Snapshot ID: `20260621T020000Z_genbank_archaea_root`
Generated: 2026-06-21T07:27:28.884438+00:00

## Result

This audit standardized metadata for a bounded hidden Archaea pilot snapshot using the existing FetchM metadata machinery. It did not change production rules, public UI, Global Insights, or deployment state.

## Metrics

| Metric | Value |
| --- | ---: |
| Domain profile | `archaea` |
| NCBI taxon root | `2157` |
| Root-unique assemblies | 200 |
| Standardized assemblies | 200 |
| Missing standardized assemblies | 0 |
| Rule-reuse review signals | 0 |
| High-risk rule-reuse signals | 0 |
| Audit pass | `true` |

## Boundaries

- Archaea remains hidden.
- No standardization rules were changed.
- No production bacterial dataset database was used.
- No canonical refresh, Global Insights regeneration, public UI exposure, or deployment was run.

## Outputs

- `standardized_field_coverage.tsv`
- `top_raw_metadata_values.tsv`
- `source_sample_environment_review.tsv`
- `rule_reuse_risk.tsv`

## Recommended Next Step

Review audit signals before any Archaea-specific rule curation or public exposure.
