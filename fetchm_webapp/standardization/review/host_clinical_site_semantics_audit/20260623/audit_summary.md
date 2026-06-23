# Host, Clinical, And Site Semantics Audit

Snapshot ID: `20260602T140414Z_genbank_bacteria_root`
Generated: 2026-06-23T10:03:02.347897+00:00
Git commit: `28ff0c440ebc0a7351140a69c01427355b7b4fe9`

## Scope

Phase 1 audit only. No production standardization rules, canonical metadata, Global Insights, or deployment state were changed.

## Dataset

| Metric | Value |
| --- | ---: |
| Canonical rows audited | 3,131,699 |
| Distinct standardized field values classified | 24,333 |
| Incompatible value-field pairs | 334 |
| Affected rows represented by incompatibility flags | 1,182,423 |

## Top Incompatibility Queues

| Queue | Values | Affected rows |
| --- | ---: | ---: |
| `non_material_values_in_sample_type` | 156 | 1,046,572 |
| `processing_values_in_sample_type` | 16 | 107,364 |
| `conflicting_anatomical_site_fields` | 9 | 53,569 |
| `ambiguous_broad_source_categories` | 17 | 40,041 |
| `material_values_in_isolation_source` | 140 | 32,213 |
| `environment_values_in_sample_type` | 13 | 26,797 |
| `non_site_values_in_isolation_site` | 9 | 22,361 |
| `non_health_values_in_host_health_state` | 12 | 11,766 |
| `non_disease_values_in_host_disease` | 2 | 5,660 |
| `care_setting_values_in_health_state` | 2 | 1,680 |
| `study_group_values_in_health_state` | 2 | 1,172 |
| `colonization_values_in_health_state` | 2 | 940 |
| `vital_status_values_in_health_state` | 2 | 329 |
| `disease_stage_values_in_health_state` | 2 | 49 |

## Interpretation

The existing release gate remains useful for source leakage and broad-vocabulary control, but this audit evaluates a different question: whether host clinical status, specimen material, source context, site, and environment fields are semantically orthogonal.

Large queues should be reviewed before rule changes. Some rows are legacy compatibility labels or broad context umbrellas, not necessarily immediate errors.

## Recommended Next Step

Review `recommended_migration_decisions.tsv` and approve a small high-confidence correction batch before changing production rules.
