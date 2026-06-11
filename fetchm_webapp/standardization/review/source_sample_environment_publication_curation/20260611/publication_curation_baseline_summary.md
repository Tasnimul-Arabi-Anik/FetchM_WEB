# Source, Sample, and Environment Publication Curation Baseline

- Canonical rows audited: 3,131,699
- Isolation_Source_SD coverage: 59.07%
- Isolation_Source_SD_Broad coverage: 55.51%
- Raw-present standardization: 81.23%
- Hard exact leakage rows: 0
- Broad leakage rows: 0
- Review-signal rows: 190,284
- Review-signal unique values: 2,080

Review signals are triage candidates, not errors. Candidate `raw_value` entries mirror the
aggregated exact standardized source label; each batch must inspect contributing raw values
and surrounding metadata before adding rules.

## Proposed Batches

| Batch | Unique values | Affected rows |
| --- | ---: | ---: |
| Batch 5 - food and commodity context | 19 | 98,965 |
| Batch 2 - sample materials | 526 | 56,359 |
| Batch 6 - host context | 268 | 22,324 |
| Batch 4 - environmental media | 8 | 7,305 |
| Batch 1 - metadata descriptors | 1,180 | 3,911 |
| Batch 3 - anatomical sites | 34 | 1,225 |
| Batch 7 - disease and health state | 45 | 195 |

## Top 100 Review Signals

| Value | Rows | Signal | Action |
| --- | ---: | --- | --- |
| poultry meat/product | 77,718 | host_taxon_or_context | route_to_food_source |
| feces/stool | 36,138 | sample_type | route_to_sample_type |
| plant-associated material | 20,751 | host_taxon_or_context | route_to_host_context |
| turkey meat/product | 14,060 | host_taxon_or_context | route_to_food_source |
| blood | 9,768 | sample_type | route_to_sample_type |
| plant/produce food product | 6,446 | host_taxon_or_context | route_to_food_source |
| healthcare-associated environment | 5,060 | environment_medium | route_to_environment_medium |
| urine | 2,975 | sample_type | route_to_sample_type |
| clinical sample | 2,020 | sample_type|metadata_descriptor | route_to_non_source_descriptor |
| agricultural environment | 2,015 | environment_medium | route_to_environment_medium |
| Tissue | 1,895 | sample_type | route_to_sample_type |
| tissue | 1,327 | sample_type | route_to_sample_type |
| canal | 1,054 | body_site | route_to_isolation_site |
| body fluid | 813 | sample_type | route_to_sample_type |
| ground chicken | 669 | host_taxon_or_context | route_to_food_source |
| food/animal-production swab | 663 | sample_type|host_taxon_or_context | route_to_sample_type |
| milk | 304 | sample_type | route_to_sample_type |
| soft tissue | 280 | sample_type | route_to_sample_type |
| respiratory sample | 221 | sample_type|metadata_descriptor | route_to_non_source_descriptor |
| terrestrial environment | 213 | environment_medium | route_to_environment_medium |
| Animal-Calf-Bob Veal | 137 | host_taxon_or_context | route_to_host_context |
| Wild Animal | 123 | host_taxon_or_context | route_to_host_context |
| Tissue (Specify:) | 114 | sample_type | route_to_sample_type |
| animal by-product | 112 | host_taxon_or_context | route_to_host_context |
| ENVO_00005801 | 112 | raw_code_or_artifact | route_to_non_source_descriptor |
| pooled tissue | 110 | sample_type | route_to_sample_type |
| TISSUE | 95 | sample_type | route_to_sample_type |
| Connective tissue | 91 | sample_type | route_to_sample_type |
| Animal-Calf-Formula-fed Veal | 90 | host_taxon_or_context | route_to_host_context |
| cat | 90 | host_taxon_or_context | route_to_host_context |
| animal feed | 85 | host_taxon_or_context | route_to_host_context |
| Broncho-alveolar lavage | 72 | sample_type | route_to_sample_type |
| Gastric tissue | 65 | sample_type | route_to_sample_type |
| fermented milk/dairy product | 64 | sample_type | route_to_sample_type |
| Mouse embryonic cells | 64 | host_taxon_or_context | route_to_host_context |
| Tick tissue | 63 | sample_type | route_to_sample_type |
| tissue pool | 62 | sample_type | route_to_sample_type |
| Bladder | 61 | body_site | route_to_isolation_site |
| Infected tissue | 55 | sample_type|disease | route_to_disease_or_health_state |
| periprosthetic tissue | 55 | sample_type | route_to_sample_type |
| Gall bladder tissue | 54 | body_site|sample_type | route_to_sample_type |
| Domestic animal | 47 | host_taxon_or_context | route_to_host_context |
| Animal-Goat | 43 | host_taxon_or_context | route_to_host_context |
| plant root/rhizosphere | 42 | host_taxon_or_context | route_to_host_context |
| plasma | 40 | sample_type | route_to_sample_type |
| Soft tissue | 37 | sample_type | route_to_sample_type |
| broncho-alveolar lavage | 35 | sample_type | route_to_sample_type |
| Animal feed | 34 | host_taxon_or_context | route_to_host_context |
| Tissue fragment | 30 | sample_type | route_to_sample_type |
| Animal-Calf-Non Formula-fed Veal | 29 | host_taxon_or_context | route_to_host_context |
| Deep Tissue | 29 | sample_type | route_to_sample_type |
| Tissue Pool | 29 | sample_type | route_to_sample_type |
| coral tissue | 26 | sample_type | route_to_sample_type |
| infected vegetal samples | 26 | disease | route_to_disease_or_health_state |
| poultry meat | 26 | host_taxon_or_context | route_to_food_source |
| bladder | 25 | body_site | route_to_isolation_site |
| oyster product | 25 | host_taxon_or_context | route_to_food_source |
| Nasopharygneal lavage | 24 | sample_type | route_to_sample_type |
| serum | 24 | sample_type | route_to_sample_type |
| Plasma | 23 | sample_type | route_to_sample_type |
| Computer keyboard and mouse | 22 | host_taxon_or_context | route_to_host_context |
| lavage | 22 | sample_type | route_to_sample_type |
| citrus infected tissue | 21 | sample_type|disease | route_to_disease_or_health_state |
| plant stem | 21 | host_taxon_or_context | route_to_host_context |
| tissue composite | 21 | sample_type | route_to_sample_type |
| animal feed canola meal | 20 | host_taxon_or_context | route_to_host_context |
| Domestic cat | 19 | host_taxon_or_context | route_to_host_context |
| Wild animal | 19 | host_taxon_or_context | route_to_host_context |
| dog chew | 18 | host_taxon_or_context | route_to_host_context |
| Mouse ES Cells | 17 | host_taxon_or_context | route_to_host_context |
| tissue samples | 17 | sample_type | route_to_sample_type |
| dog treat | 16 | host_taxon_or_context | route_to_host_context |
| hot dog | 16 | host_taxon_or_context | route_to_host_context |
| Deep tissue | 15 | sample_type | route_to_sample_type |
| ENVO_01000248 | 15 | raw_code_or_artifact | route_to_non_source_descriptor |
| Gall bladder | 15 | body_site | route_to_isolation_site |
| Tracheal lavage | 15 | sample_type | route_to_sample_type |
| Homogenized tissue | 14 | sample_type | route_to_sample_type |
| combination of 24 samples (8 from lake Croche, 8 from Montjoie and 8 from Simoncouche) collected from 2013 through 2014 | 13 | environment_medium | route_to_environment_medium |
| gall bladder | 13 | body_site | route_to_isolation_site |
| guttural pouch lavage | 13 | sample_type | route_to_sample_type |
| mouse spinal cord microglia | 13 | host_taxon_or_context | route_to_host_context |
| Pooled Tissue | 13 | sample_type | route_to_sample_type |
| Soft Tissue | 13 | sample_type | route_to_sample_type |
| BLDP1 | 12 | raw_code_or_artifact | route_to_non_source_descriptor |
| Cat nape | 12 | host_taxon_or_context | route_to_host_context |
| curd_Producer_2 | 12 | raw_code_or_artifact | route_to_non_source_descriptor |
| curd_Producer_3 | 12 | raw_code_or_artifact | route_to_non_source_descriptor |
| Infected person | 12 | disease | route_to_disease_or_health_state |
| periprosthetic tissue specimen | 12 | sample_type | route_to_sample_type |
| Pig product | 12 | host_taxon_or_context | route_to_host_context |
| sputum | 12 | sample_type | route_to_sample_type |
| Toe Tissue | 12 | sample_type | route_to_sample_type |
| animal feed wheat | 11 | host_taxon_or_context | route_to_host_context |
| biopsy | 11 | sample_type | route_to_sample_type |
| EFB infected honey bee larva | 11 | disease | route_to_disease_or_health_state |
| Guttural pouch lavage | 11 | sample_type | route_to_sample_type |
| Hip Tissue | 11 | sample_type | route_to_sample_type |
| Tissue, Ankle | 11 | sample_type | route_to_sample_type |
| Tissue, Finger | 11 | sample_type | route_to_sample_type |
