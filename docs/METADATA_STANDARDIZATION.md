# FetchM Web Metadata Standardization

Version: 2026.05-genus-v1.1

## Purpose

FetchM Web standardizes inconsistent NCBI/BioSample metadata into reproducible derived fields for genus-level microbial genome exploration. Raw metadata are preserved; standardized values are written into separate columns.

The current production state is documented in:

- `docs/releases/2026.05-genus-v1.1.md`
- `fetchm_webapp/standardization/review/metadata_standardization_publication_summary.md`
- `fetchm_webapp/standardization/review/final_audit/20260505_100958/final_metadata_standardization_dashboard.md`
- `fetchm_webapp/standardization/review/final_audit/20260505_100958/production_readiness_gate.md`

## Standardized Field Groups

### Geography

Main fields:

- `Country`
- `Continent`
- `Subcontinent`
- `Country_Source`
- `Country_Confidence`
- `Country_Evidence`
- `Geo_Recovery_Status`

Policy:

- Trusted country/geography fields are used first.
- Reviewed secondary geography recovery is used only when safe.
- False positives such as `ground turkey`, `Guinea pig`, `Norway rat`, `Aspergillus niger`, hospital/clinic names, and food terms are blocked from becoming countries.
- If `Country` is assigned, continent and subcontinent must match the approved lookup.

### Collection Date

Main fields:

- `Collection_Year`
- `Collection_Date_Source`
- `Collection_Date_Evidence`
- `Collection_Date_Recovery_Status`

Policy:

- Partial and full dates can recover a year when evidence is clear.
- Protocol/publication/accession-like text and future/impossible years are blocked.

### Host

Main fields:

- `Host_SD`
- `Host_TaxID`
- `Host_Rank`
- `Host_Superkingdom`
- `Host_Phylum`
- `Host_Class`
- `Host_Order`
- `Host_Family`
- `Host_Genus`
- `Host_Species`
- `Host_Common_Name`
- `Host_Context_SD`
- `Host_Match_Method`
- `Host_Confidence`
- `Host_Review_Status`

Policy:

- Exact reviewed synonyms and NCBI/TaxonKit-backed rules are preferred.
- Broad host labels are allowed when species-level inference is unsafe.
- Lab strains, source materials, culture artifacts, people/institution labels, and missing/admin values are blocked from `Host_SD`.
- Ambiguous long-tail values remain review-needed rather than being forced.

### Source, Sample, Environment, And Site

Main fields:

- `Sample_Type_SD`
- `Sample_Type_SD_Broad`
- `Isolation_Source_SD`
- `Isolation_Source_SD_Broad`
- `Isolation_Site_SD`
- `Environment_Medium_SD`
- `Environment_Medium_SD_Broad`
- `Environment_Broad_Scale_SD`
- `Environment_Local_Scale_SD`

Policy:

- Materials/specimens go to sample type.
- Soil, water, sediment, wastewater, seawater, air, biofilm, and related media go to environment medium.
- Healthcare, food, agricultural, built-environment, and host-associated context go to isolation source.
- Anatomical terms go to isolation site or host anatomical context.
- Disease terms go to disease/health-state fields, not sample/source fields.
- Broad fields are constrained to approved broad vocabulary values.

### Disease And Health State

Main fields:

- `Host_Disease_SD`
- `Host_Health_State_SD`

Policy:

- Disease labels are standardized conservatively.
- Healthy/asymptomatic/control labels become health-state values.
- Sample material is not inferred from disease names unless a true specimen is explicitly present.

## Rule Files

Production standardization is deterministic and backed by committed files:

- `fetchm_webapp/standardization/host_synonyms.csv`
- `fetchm_webapp/standardization/host_negative_rules.csv`
- `fetchm_webapp/standardization/controlled_categories.csv`
- `fetchm_webapp/standardization/approved_broad_categories.csv`
- `fetchm_webapp/standardization/geography_reviewed_rules.csv`
- `fetchm_webapp/standardization/collection_date_reviewed_rules.csv`

## Production QA Gates

The production gate must remain clean for release claims:

- file errors: `0`
- non-country values in `Country`: `0`
- country-continent mismatches: `0`
- country-subcontinent mismatches: `0`
- invalid host-like `Sample_Type_SD` rows: `0`
- noisy/unapproved broad values: `0`
- body-site leakage values: `0`
- disease/source leakage values: `0`
- raw code/text leakage values: `0`
- controlled-category duplicate keys: `0`
- controlled-category conflict keys: `0`
- regression tests: passing

Latest gate:

- `fetchm_webapp/standardization/review/final_audit/20260505_100958/production_readiness_gate.md`

## Maintenance Workflow

When adding new standardization rules:

1. Add deterministic reviewed rules to the appropriate CSV.
2. Run controlled-category and targeted metadata audits.
3. Queue a genus-level standardization refresh.
4. Regenerate final audit outputs.
5. Confirm the production gate passes.
6. Commit rule changes and audit artifacts together.

Embeddings or BGE may be used only to cluster unresolved review queues. They must not directly write production standardized metadata.

