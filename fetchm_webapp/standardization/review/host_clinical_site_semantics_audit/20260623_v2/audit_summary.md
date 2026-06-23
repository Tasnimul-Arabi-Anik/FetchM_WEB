# Host, Clinical, And Site Semantics Audit V2

Snapshot ID: `20260602T140414Z_genbank_bacteria_root`
Generated: 2026-06-23T12:56:28.420799+00:00
Input standardization commit: `28ff0c440ebc0a7351140a69c01427355b7b4fe9`
Audit code commit: `5ac81e2635e379f48dc039a4fa8f4434a31c343f`

## Scope

Phase 1 audit only. No production standardization rules, canonical metadata, Global Insights, or deployment state were changed.

## Dataset

| Metric | Value |
| --- | ---: |
| Canonical rows audited | 3,131,699 |
| Distinct standardized field values classified | 24,333 |
| Field-value assignment signals | 442,506 |
| Unique assemblies with any semantic signal | 354,881 |
| Unique assemblies with confirmed high-confidence signal | 30,645 |
| Unique assemblies with review/uncertain signal | 222,582 |

## Action Classes

| Action class | Assignment occurrences |
| --- | ---: |
| `additive_axis_enrichment` | 21,578 |
| `classifier_uncertain` | 41,380 |
| `composite_requires_split` | 153,555 |
| `confirmed_high_confidence_fix` | 30,758 |
| `legacy_compatibility_label` | 162,009 |
| `manual_review` | 33,226 |

## Queue Summary

| Queue | Values | Assignment count |
| --- | ---: | ---: |
| `non_material_values_in_sample_type` | 80 | 218,727 |
| `processing_values_in_sample_type` | 17 | 118,792 |
| `conflicting_anatomical_site_fields` | 6 | 45,714 |
| `environment_values_in_sample_type` | 19 | 34,028 |
| `material_values_in_isolation_source` | 158 | 32,347 |
| `non_site_values_in_isolation_site` | 5 | 14,504 |
| `non_health_values_in_host_health_state` | 13 | 11,816 |
| `non_disease_values_in_host_disease` | 2 | 5,660 |
| `care_setting_values_in_health_state` | 2 | 1,680 |
| `ambiguous_broad_source_categories` | 7 | 1,215 |
| `study_group_values_in_health_state` | 1 | 1,077 |
| `colonization_values_in_health_state` | 2 | 940 |
| `vital_status_values_in_health_state` | 2 | 329 |
| `disease_stage_values_in_health_state` | 2 | 49 |

## Interpretation

This V2 audit uses compositional classification. Counts are review signals and decomposition candidates, not confirmed erroneous records. Legacy umbrella fields such as `Isolation_Source_SD` and `Sample_Type_SD` are not treated as strict ontologies; strict derived axes are proposed separately.

## Recommended Next Step

Review confirmed high-confidence fixes and composite split candidates. Do not start broad remapping until the field contract is approved.
