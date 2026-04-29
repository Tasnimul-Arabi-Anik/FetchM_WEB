# Metadata Standardization Improvement Report

Date: 2026-04-29

This document records the major FetchM_WEB metadata-standardization improvements
that are suitable for future methods/results writing. It summarizes the
engineering changes, curation workflow, and quantitative audit results.

## Scope

The standardization work focused on genus-level managed metadata. Species-level
standardization remains deferred because species metadata is derived from and
can be refreshed after genus-level rules stabilize.

Latest fully audited genus-level dataset before the final clustered-rule pass:

- Files scanned: 5,066 genus metadata files.
- Rows scanned: 2,596,095 genome metadata rows.
- File errors: 0.

## Major Improvements

### Host Standardization

Implemented a layered host-standardization system:

- Original host values are preserved in `Host_Original`.
- Cleaned values are recorded in `Host_Cleaned`.
- Standardized outputs are written to `Host_SD` and `Host_TaxID`.
- Taxonomic lineage is added through `Host_Rank`, `Host_Superkingdom`,
  `Host_Phylum`, `Host_Class`, `Host_Order`, `Host_Family`, `Host_Genus`, and
  `Host_Species`.
- `Host_Common_Name`, `Host_Context_SD`, `Host_Match_Method`,
  `Host_Confidence`, and `Host_Review_Status` provide interpretability.

Host-rule files now separate positive and negative rules:

- `host_synonyms.csv` stores approved positive host mappings.
- `host_negative_rules.csv` stores values that should remain blank because they
  are non-host source descriptors, lab/person descriptors, missing values, or
  not identifiable.

### Microbial Self-Descriptor Protection

Added protection against incorrectly treating bacterial/lab descriptors as
hosts. Examples such as `E. coli`, `E-coli`, `Ecoli`, `E. coli BL21`, `JM109`,
`DH5-alpha`, `ATCC strain`, `control strain`, `vaccine strain`,
`Pseudomonas culture`, and `Bacillus isolate` are routed away from `Host_SD`
when used as microbial self/lab culture descriptors.

### TaxonKit and AI-Assisted Curation

Used TaxonKit and external AI-assisted review to curate long-tail host values.
The workflow imported only conservative decisions:

- Direct species/subspecies host mappings.
- Genus-level host mappings with medium confidence.
- Selected broad host groups with medium confidence.
- Blank/non-host/not-identifiable values as negative rules.
- Undetermined values were intentionally left unresolved.

### Geography Standardization

Country, continent, and subcontinent recovery was improved while avoiding known
false positives such as animal/common names that resemble countries. The latest
full audit found:

- Country present: 87.07%.
- Country-continent mismatches: 0.
- Country-subcontinent mismatches: 0.

### Collection Date Standardization

Collection-date/year recovery was improved from primary and reviewed secondary
date fields. The latest full audit found:

- Collection year present: 83.51%.

## Quantitative Host Results

Host-review burden decreased in stages:

| Stage | Host Review-Needed Rows | Notes |
|---|---:|---|
| Initial post-lineage audit | 4,034 | After rank-aware host columns were introduced. |
| After microbial self-descriptor and context recovery improvements | 3,156 | E. coli/lab descriptors no longer mapped as hosts. |
| After AI-approved host rule imports | 1,714 | Positive and negative AI-reviewed rules imported. |

Latest fully audited host metrics before final clustered-rule pass:

- Host TaxID present: 1,557,821 rows.
- Host TaxID coverage: 60.01%.
- Host context recovered rows: 161,450.
- Host review-needed rows: 1,714.

## Final Clustered Rule Pass

The remaining 1,190 unique unresolved host values were clustered and processed
with conservative rules:

- Approved Host_SD rules added: 190.
- Approved blank/non-host/not-identifiable rules added: 23.
- Undetermined values left unresolved: 960.

The final clustered-rule standardization refresh was started after these rules
were imported. A final audit should be run after all genus tasks complete to
quantify the post-clustered-rule host-review count.

## Current Rule Assets

Production rule files:

- `fetchm_webapp/standardization/host_synonyms.csv`
- `fetchm_webapp/standardization/host_negative_rules.csv`
- `fetchm_webapp/standardization/controlled_categories.csv`
- `fetchm_webapp/standardization/geography_reviewed_rules.csv`
- `fetchm_webapp/standardization/collection_date_reviewed_rules.csv`

Supporting research logs:

- `fetchm_webapp/standardization/review/metadata_standardization_research_log.md`
- `fetchm_webapp/standardization/review/fetchm_web_research_paper_notes.md`

## Methods-Ready Statement

FetchM_WEB standardizes NCBI-derived genome metadata using a conservative,
rule-based and taxonomy-validated pipeline. Raw values are preserved, while
standardized fields are added as separate columns. Host metadata is normalized
through exact dictionaries, reviewed synonym rules, TaxonKit-supported taxonomy
resolution, broad-host group curation, and explicit negative rules for
non-host/source or not-identifiable values. Ambiguous values are not forced into
standardized host fields, preserving data provenance and reducing false-positive
host assignment.
