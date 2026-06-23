# Metadata Semantic Field Contract

Status: draft field contract for reviewed Phase 2A planning. This document does not change production standardization rules by itself.

## Purpose

FetchM standardization currently exposes several compatibility fields that mirror broad NCBI BioSample source and sample concepts. Those fields are useful for users, but they can contain multi-axis concepts such as `rectal swab`, `healthy control`, `sink biofilm`, or `metagenomic assembly`. Phase 2A must preserve those compatibility fields while adding stricter derived axes for scientific interpretation.

## Field Classes

Strict fields should contain only the concept named by the field. Legacy compatibility fields may retain NCBI-style broad labels while stricter derived fields are added.

| Field | Role | Contract |
| --- | --- | --- |
| `Host_SD` | strict existing | Biological host taxon only. |
| `Host_Context_SD` | strict existing | Broad host-associated context where a source-derived host context is useful but not a taxon assignment. |
| `Host_Disease_SD` | strict existing | Named disease or defensible disease class only. Do not store `healthy`, `diseased`, `patient`, `carrier`, `colonized`, or care setting here. |
| `Host_Health_State_SD` | strict existing | Health state only: `healthy`, `diseased`, `asymptomatic`, `symptomatic`, or reviewed equivalent. |
| `Host_Production_Context_SD` | existing to expand | Production, husbandry, or rearing context such as `specific pathogen free`, `broiler`, `dairy cow`, `farmed`, or `laboratory-reared`. |
| `Host_Anatomical_Site_SD` | existing to enforce | Anatomical part of a confirmed biological host, including plant parts when the host relationship is explicit. |
| `Isolation_Site_SD` | legacy/site field | Sampling or isolation site when host linkage is absent or uncertain. See site precedence below. |
| `Sample_Type_SD` | legacy compatibility | Backward-compatible NCBI-style sample type umbrella. Preserve during Phase 2A while adding strict axes. |
| `Sample_Type_SD_Broad` | legacy compatibility | Broad sample type/context umbrella. Preserve during Phase 2A. |
| `Isolation_Source_SD` | legacy compatibility | Backward-compatible normalized physical, environmental, or local source. Preserve during Phase 2A. |
| `Isolation_Source_SD_Broad` | legacy compatibility | Broad source-context umbrella. Preserve during Phase 2A. |
| `Environment_Medium_SD` | strict existing | Physical environmental medium only. |
| `Environment_Medium_SD_Broad` | strict existing | Broad environmental medium class only. |
| `Environment_Broad_Scale_SD` | strict existing | Broad environmental setting. |
| `Environment_Local_Scale_SD` | strict existing | Local environmental or facility feature. |

## New Additive Axes

These fields are proposed as additive derived axes. They should be populated only when evidence is explicit or a reviewed deterministic rule exists.

| Field | Meaning | Examples |
| --- | --- | --- |
| `Sample_Material_SD` | Physical specimen/material. | `blood`, `urine`, `pus`, `cerebrospinal fluid`, `manure` |
| `Sampling_Context_SD` | Sampling context when physical material is unspecified or supplemental. | `clinical`, `environmental`, `food`, `surveillance`, `laboratory` |
| `Sample_Processing_SD` | Preparation or processing state. | `culture`, `enrichment`, `DNA extract`, `single-cell preparation` |
| `Sample_Collection_Device_SD` | Device used to collect a specimen. | `swab`, `sponge`, `catheter`, `filter` |
| `Sample_Collection_Method_SD` | Method used to obtain the specimen. | `bronchoalveolar lavage`, `aspiration`, `biopsy` |
| `Sample_Entity_SD` | Biological material form/entity. | `microbial isolate`, `cell culture`, `mixed culture`, `single cell` |
| `Data_Product_SD` | Sequence-derived or computational product, not a biological sample entity. | `metagenomic assembly`, `sequence assembly` |
| `Host_Anatomical_Material_SD` | Host-derived material distinct from host part/site. | `stool`, `mucus`, `saliva`, `pus`, `blood`, `CSF` |
| `Host_Study_Group_SD` | Study-group assignment. | `case`, `control`, `exposed`, `contact` |
| `Host_Hospitalization_Status_SD` | Hospitalization status only. | `hospitalized`, `not hospitalized` |
| `Host_Care_Setting_SD` | Care setting or facility context. | `inpatient`, `outpatient`, `ICU`, `long-term care` |
| `Host_Vital_Status_SD` | Vital status. | `alive`, `deceased` |
| `Host_Colonization_Status_SD` | Carrier or colonization state. | `carrier`, `colonized`, `not colonized` |
| `Host_Disease_Stage_SD` | Disease stage or phase. | `acute`, `chronic`, `convalescent`, `exacerbation` |
| `Host_Disease_Outcome_SD` | Disease outcome. | `recovered`, `survived`, `fatal outcome` |
| `Host_Exposure_Context_SD` | Exposure/contact context independent of study group. | `household contact`, `close contact`, `exposure/contact context` |

## Multi-Axis Policy

Do not collapse composite phrases into one field when the phrase contains multiple valid concepts.

Examples:

| Raw/standardized value | Decomposition |
| --- | --- |
| `healthy control` | `Host_Health_State_SD=healthy` plus `Host_Study_Group_SD=control` |
| `rectal swab` | `Sample_Material_SD=swab specimen`, `Sample_Collection_Device_SD=swab`, host or isolation site `rectum` as appropriate |
| `sink biofilm` | `Environment_Medium_SD=biofilm`, `Environment_Local_Scale_SD=sink` |
| `gastric biopsy` | `Sample_Material_SD=biopsy/tissue`, `Sample_Collection_Method_SD=biopsy`, site `stomach/gastrointestinal tract` |
| `metagenomic assembly` | `Data_Product_SD=metagenomic assembly`, `Sample_Processing_SD=sequence-derived`; not `Sample_Entity_SD` |

## Site Precedence

Use `Host_Anatomical_Site_SD` when an anatomical part is explicitly associated with a biological host. Use `Isolation_Site_SD` when the submitted value is a sampling or isolation site and host linkage is absent, uncertain, or retained for legacy compatibility.

When both are supported, both may be populated with provenance. `Host_Anatomical_Site_SD` is preferred for host-linked interpretation and `Isolation_Site_SD` is retained only as a compatibility/sampling-site axis.

Do not place materials such as `cerebrospinal fluid`, `pus`, `manure`, `feces/stool`, or `gut content` in a site field unless a distinct anatomical-site component is also present.

## Legacy Compatibility Policy

Do not broadly remap or clear these fields in Phase 2A:

- `Sample_Type_SD`
- `Sample_Type_SD_Broad`
- `Isolation_Source_SD`
- `Isolation_Source_SD_Broad`

These fields remain user-facing compatibility labels until strict additive axes are validated and downstream exports/UI are updated. Values in these fields should be treated as decomposition candidates, not automatic errors.

## Missingness And Evidence

A strict field should remain blank when evidence is absent. Do not infer disease from generic clinical context, food contamination, outbreak labels, surveillance labels, carrier/colonized status, or patient-only labels.

Every new additive field must preserve provenance: source raw field, source raw value, rule or method, evidence type, confidence, and ruleset version.

## Phase 2A Boundary

Phase 2A may correct only confirmed strict-field violations and add additive derived axes. It must not change host taxonomy, geography/date standardization, canonical raw metadata, or legacy compatibility fields without a separate reviewed batch.
