# Hidden Archaea Metadata Standardization Audit

Snapshot ID: `20260621T100000Z_genbank_archaea_root`
Generated: 2026-06-21T10:46:39.918438+00:00

## Result

This audit standardized metadata for a hidden Archaea snapshot using the existing FetchM metadata machinery. The audit run did not touch public UI, Global Insights, or deployment state.

## Metrics

| Metric | Value |
| --- | ---: |
| Domain profile | `archaea` |
| NCBI taxon root | `2157` |
| Root-unique assemblies | 41,956 |
| Standardized assemblies | 41,956 |
| Missing standardized assemblies | 0 |
| Rule-reuse review signals | 0 |
| High-risk rule-reuse signals | 0 |
| Audit pass | `true` |
| Analysis scope | `hidden_full` |

## Boundaries

- Archaea remains hidden.
- The audit command is read-only with respect to standardization rules; any curation changes are tracked in git.
- Hidden Archaea outputs remain non-public and separate from the NAR-facing bacterial release.
- No canonical refresh, Global Insights regeneration, public UI exposure, or deployment was run.

## Outputs

- `standardized_field_coverage.tsv`
- `top_raw_metadata_values.tsv`
- `source_sample_environment_review.tsv`
- `rule_reuse_risk.tsv`

## Recommended Next Step

Review audit signals before any Archaea-specific rule curation or public exposure.
