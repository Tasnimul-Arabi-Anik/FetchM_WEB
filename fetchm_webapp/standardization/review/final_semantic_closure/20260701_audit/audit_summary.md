# Host, Clinical, And Site Semantics Audit V2

Snapshot ID: `20260602T140414Z_genbank_bacteria_root`
Generated: 2026-07-01T06:45:29.503137+00:00
Input standardization commit: `28ff0c440ebc0a7351140a69c01427355b7b4fe9`
Audit code commit: `48dedba5809d45bccfb2a11a78a0f155ab4105b2`

## Scope

Phase 1 audit only. No production standardization rules, canonical metadata, Global Insights, or deployment state were changed.

## Dataset

| Metric | Value |
| --- | ---: |
| Canonical rows audited | 3,131,699 |
| Distinct standardized field values classified | 24,351 |
| Strict-field value pairs classified | 310 |
| Strict-field violation assignment occurrences | 910,373 |
| Ambiguous/review assignment occurrences | 4,199,315 |
| Preserved-conflict/compatibility assignment occurrences | 190,318 |
| Rows with semantic-axis provenance | 842,467 |
| All candidate assignment occurrences | 8,240,516 |
| Field-value pairs with queue signals | 401 |
| Field-value pairs with any semantic candidate signal | 18,692 |
| Unique assemblies with any semantic candidate signal | 2,021,385 |
| Unique assemblies with confirmed high-confidence signal | 2 |
| Unique assemblies with review/uncertain signal | 1,510,055 |

## Action Classes

| Action class | Assignment occurrences |
| --- | ---: |
| `additive_axis_enrichment` | 3,850,881 |
| `classifier_uncertain` | 679,348 |
| `composite_requires_split` | 3,110,620 |
| `confirmed_high_confidence_fix` | 2 |
| `legacy_compatibility_label` | 190,318 |
| `manual_review` | 409,347 |

## Queue Summary

| Queue | Values | Assignment count |
| --- | ---: | ---: |
| `non_local_values_in_environment_local_scale` | 18 | 482,536 |
| `non_broad_values_in_environment_broad_scale` | 11 | 256,685 |
| `non_material_values_in_sample_type` | 75 | 229,550 |
| `processing_values_in_sample_type` | 17 | 118,792 |
| `material_values_in_isolation_source` | 163 | 67,619 |
| `non_medium_values_in_environment_medium` | 23 | 64,489 |
| `remaining_non_site_values` | 12 | 58,970 |
| `non_medium_values_in_environment_medium_broad` | 11 | 56,305 |
| `conflicting_anatomical_site_fields` | 6 | 42,092 |
| `environment_values_in_sample_type` | 23 | 31,705 |
| `non_site_values_in_isolation_site` | 4 | 8,866 |
| `invalid_sampling_context_values` | 2 | 8,032 |
| `ambiguous_broad_source_categories` | 6 | 1,214 |
| `composite_health_state_values` | 2 | 1,127 |
| `non_health_values_in_host_health_state` | 2 | 1,127 |
| `study_group_values_in_health_state` | 1 | 1,077 |
| `invalid_sample_material_values` | 1 | 234 |
| `care_setting_values_in_health_state` | 0 | 0 |
| `colonization_values_in_health_state` | 0 | 0 |
| `disease_stage_values_in_health_state` | 0 | 0 |
| `invalid_anatomical_material_values` | 0 | 0 |
| `invalid_colonization_status_values` | 0 | 0 |
| `invalid_vital_status_values` | 0 | 0 |
| `non_disease_values_in_host_disease` | 0 | 0 |
| `vital_status_values_in_health_state` | 0 | 0 |

## Interpretation

This V2 audit uses compositional classification. Counts are candidate signals and decomposition opportunities, not confirmed erroneous records. `confirmed_high_confidence_fix` is intentionally limited to reviewed strict host-health, host-disease, and isolation-site violations; environment and legacy compatibility candidates remain review or additive queues. Legacy umbrella fields such as `Isolation_Source_SD` and `Sample_Type_SD` are not treated as strict ontologies; strict derived axes are proposed separately.

## Recommended Next Step

Review confirmed high-confidence fixes and composite split candidates. Do not start broad remapping until the field contract is approved.
