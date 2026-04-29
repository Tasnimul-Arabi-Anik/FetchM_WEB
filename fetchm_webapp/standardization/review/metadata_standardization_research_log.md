# FetchM WEB Metadata Standardization Research Log

This file records metadata-standardization decisions, audit results, and backfill outcomes that may be useful for manuscript writing, methods documentation, and future tool updates.

## Scope Policy

- Primary reporting and standardization evaluation should focus on genus-level metadata.
- Species-level metadata are retained for webapp use, but broad standardization updates should be developed and validated at genus level first because many species-level datasets are derived from, or overlap with, genus-level metadata.
- Raw metadata values should be preserved. Standardized or derived values should be added in separate fields rather than replacing original NCBI/BioSample values.

## Geography Standardization Update

Date: 2026-04-28

### Objective

Improve country, continent, and subcontinent completeness without introducing false positives from non-geographic text fields.

### Method

The webapp now derives geography in this order:

1. Trusted geography fields, including `Country` and `Geographic Location`.
2. Secondary geography recovery only when trusted fields are absent or unmapped.
3. Secondary recovery checks selected fields such as isolation source, BioSample source name, BioSample description/title, and ENV local/broad scale.
4. Known false-positive patterns are blocked, including examples such as `ground turkey`, `Guinea pig`, `Norway rat`, and `A. niger`.
5. New provenance fields are generated when possible: `Country_Source`, `Country_Confidence`, and `Country_Evidence`.

### Genus-Level Results

- Genus files scanned: 5,066.
- Genus rows scanned: 2,590,009.
- Genus-level mapped country rows improved from 2,257,375 to 2,258,435 after adding safe secondary recovery and reviewed secondary geography rules.
- Additional genus-level rows recovered by secondary geography logic: 1,060.
- Genus-level country missing or unmapped rows decreased from 332,634 to 331,574.
- Continent/subcontinent assignment errors after correction: 0 missing continent, 0 missing subcontinent, 0 continent mismatch, and 0 subcontinent mismatch when country is known.

### Whole Managed Metadata Context

- Total metadata files processed: 38,086.
- Total metadata rows processed: 4,980,609.
- Whole managed metadata rows recovered by secondary geography logic: 1,825.
- Whole managed metadata country missing/unmapped rows decreased from 582,300 to 580,475.

### Remaining Secondary Geography Candidates

The latest genus-level audit reported 591 remaining candidate rows where a country-like token appears in non-primary metadata fields but was not automatically accepted.

These are intentionally left for review because many are likely false positives. The largest examples include:

- `ground turkey`, `Turkey ground turkey`, and related food terms that contain `Turkey` but do not indicate the country.
- Host terms such as `Guinea pig`, where `Guinea` is not a geographic country value.
- Host terms such as `Norway rat`, where `Norway` is part of a common name.
- Terms involving `Niger` from organism names such as `A. niger` or `Aspergillus niger`.

Review file:

- `fetchm_webapp/standardization/review/geography_review/country_missing_recoverable_other_columns.csv`

Source-column summaries:

- `fetchm_webapp/standardization/review/geography_review/hidden_country_by_source_column.csv`
- `fetchm_webapp/standardization/review/geography_review/hidden_country_source_columns.csv`

Audit summary:

- `fetchm_webapp/standardization/review/geography_review/geography_audit_summary.csv`

Backfill summary:

- `fetchm_webapp/standardization/review/geography_backfill/geography_backfill_summary.csv`

### Interpretation For Manuscript Writing

The geography refinement substantially improved standardized country, continent, and subcontinent assignment while preserving conservative behavior for ambiguous metadata. The secondary recovery step added a smaller but valuable number of additional country assignments, and provenance fields now make the source of each recovered country auditable. The remaining country-like tokens are not automatically mapped because they include common biological, food, and host terms that can mimic geographic names.

## Secondary Geography False-Positive Filtering

Date: 2026-04-28

- Initial genus-level secondary geography candidates: 591.
- After excluding animal/common-name/food-style/protocol false positives: 20.
- Excluded examples include `ground turkey`, turkey meat/embryo/poult/sinus/food terms, `Guinea pig`, `guinea fowl`, `Norway rat`, `A. niger`, `Aspergillus niger`, `Cordylus niger`, `Lizard (Cordylus niger)`, `Honduras style queso fresco cheese`, and protocol text beginning with `Protocols:`.
- The remaining 20 candidates were manually reviewed and accepted as valuable source information from non-primary metadata fields.
- A genus-only backfill recovered all 20 reviewed rows. The final 3 exact `Host = USA` cases required a narrow reviewed host-field exception.
- Final genus-level audit result: `country_recoverable_from_other_metadata_columns = 0`.
- Reviewed secondary geography rules are now stored in `fetchm_webapp/standardization/geography_reviewed_rules.csv`.
- Geography provenance now includes `Geo_Recovery_Status` with values such as `trusted_primary`, `reviewed_secondary`, `rule_secondary`, `absent`, and `unknown`.
- Future audit runs also write an admin-review-friendly queue to `fetchm_webapp/standardization/review/geography_review/geography_secondary_review_queue.csv`.

## Pending Review Items

- Keep false-positive blocking rules conservative.
- Add future "Refinement" workflows after tool updates so old metadata can be reprocessed without rerunning the full NCBI metadata fetch.

## Collection Date Standardization

Date: 2026-04-28

- Added collection-date provenance fields: `Collection_Date_Source`, `Collection_Date_Evidence`, and `Collection_Date_Recovery_Status`.
- Added trusted primary alias recovery for collection-date fields such as collection timestamp, sampling event date, host collection date, isolation date, harvest date, specimen collection date, and DNA isolation date.
- Added conservative secondary text recovery only when collection/isolation/sampling context is explicit.
- Added false-positive guards for protocol/publication/submission/sequencing text, accession-like strings, ontology IDs, malformed dates, and `missing: data agreement` text.
- Initial refined genus-level audit found 118 rows recoverable from primary aliases and 379 rows recoverable from secondary text fields.
- Genus-only backfill recovered 497 collection-year rows total.
- Final genus-level audit: 2,165,462 rows with mapped collection year, 424,547 missing/unmapped rows, 0 remaining recoverable primary candidates, and 0 remaining recoverable secondary candidates under the current conservative rules.

Evidence files:

- `fetchm_webapp/standardization/review/collection_date_review/collection_date_audit_summary.csv`
- `fetchm_webapp/standardization/review/collection_date_review/collection_date_secondary_review_queue.csv`
- `fetchm_webapp/standardization/review/collection_date_backfill/collection_date_backfill_summary.csv`

## Host Standardization Audit

Date: 2026-04-28

- Genus files scanned: 5,066.
- Genus rows scanned: 2,590,009.
- Raw host present: 1,408,740 rows.
- Raw host missing: 1,181,269 rows after strengthened missing-token handling.
- Stored `Host_SD` mapped: 1,505,422 rows.
- Recomputed raw-host-only mappings using current rules: 1,357,159 rows.
- Raw present but still unmapped after recomputation: 45,362 rows.
- Non-host source values detected in host fields after recomputation: 6,219 rows.

Interpretation:

- The large host-missing count is mostly true absence in NCBI/BioSample source metadata.
- Some missing host rows can still be inferred from safe context fields, which is why stored `Host_SD` mapped rows exceed raw host-present rows.
- Remaining unmapped values are largely long-tail Latin host names, broad biological groups, and source/sample terms.
- It is acceptable to keep true host terms unchanged when no confident TaxID-backed rule is available.
- Missing-token detection was strengthened to catch values such as `Not Applicable [GENEPIO:0001619]`.
- Non-host source detection was expanded for stool/feces/meat/food/environment/metagenome/algae/source terms while preserving true host matches such as `human stool`.

Evidence files:

- `fetchm_webapp/standardization/review/host_review/host_audit_summary.csv`
- `fetchm_webapp/standardization/review/host_review/host_unmapped_review_queue.csv`
- `fetchm_webapp/standardization/review/host_review/host_non_host_source_values.csv`
- `fetchm_webapp/standardization/review/host_review/host_mapped_values.csv`

## Host Taxonomy Resolution Workflow

Date: 2026-04-28

- The host review queue now uses explicit decision labels: `approve_host`, `non_host_source`, `missing`, `broad_host`, and `needs_manual_review`.
- Added a taxonomy-resolution candidate export: `fetchm_webapp/standardization/review/host_review/host_taxonomy_name_candidates.csv`.
- The candidate export contains scientific-name-like unmapped host values such as `Sporobolus alterniflorus`, `Myodes glareolus`, `Ifremeria nautilei`, `Acmispon wrangelianus`, `Apis cerana`, `Cervus elaphus`, and `Ixodes ricinus`.
- Added `fetchm_webapp/tools/merge_host_taxonomy_resolution.py` to merge reviewed NCBI Taxonomy name-to-taxid results into `fetchm_webapp/standardization/host_synonyms.csv`.
- Confirmed host names should enter `host_synonyms.csv` with `synonym`, `canonical`, `taxid`, `confidence`, and `note`.
- Confirmed non-host source, sample, and environment terms should enter `fetchm_webapp/standardization/controlled_categories.csv`.
- TaxonKit has not been installed or run yet in this environment; the workflow is ready for local TaxonKit output once approved.

## Host Unresolved-Class Split

Date: 2026-04-28

- Updated host audit output so unresolved host values are split into practical action classes rather than one generic unmapped group.
- `host_unmapped_review_queue.csv` now includes `unresolved_class`, `review_decision`, and a suggested review note.
- `host_unresolved_classification.csv` summarizes the unresolved classes.

Latest genus-level split among 45,362 raw-present unresolved host rows:

- `taxonomic_name_needing_taxid_lookup`: 35,316 rows.
- `ambiguous_free_text`: 9,344 rows.
- `broad_host`: 501 rows.
- `sample_source_misplaced_in_host`: 201 rows.

Interpretation:

- Most remaining host work is not model fine-tuning; it is NCBI Taxonomy lookup for scientific names.
- Source/sample/environment values in host fields should be routed to controlled categories rather than forced into `Host_SD`.
- Fuzzy matching and embedding/AI should remain review-assistive only until a value is approved.

## Host TaxonKit Resolution

Date: 2026-04-28

- Installed `taxonkit v0.20.0` locally and downloaded the NCBI taxonomy dump into `/home/ai-pc/.taxonkit`.
- Resolved `7,306` host taxonomy candidate names from `host_taxonomy_name_candidates.csv`.
- TaxonKit output contained `7,324` rows because some names returned multiple TaxIDs.
- Resolution summary:
  - Resolved rows: 5,417.
  - Blank/unresolved rows: 1,907.
  - Ambiguous names with multiple TaxIDs: 16.
  - Non-scientific/common-name matches filtered out: 213.
  - High-confidence scientific-name-like imports added to `host_synonyms.csv`: 5,169.
- Created a backup before import: `fetchm_webapp/standardization/host_synonyms.before_taxonkit_2026-04-28.csv`.
- Added a safety filter to `merge_host_taxonomy_resolution.py` so ambiguous names and non-scientific/common-name matches are skipped by default when `--scientific-only` is used.
- Optimized host matching by limiting substring matching to curated host aliases (`HOST_SUBSTRING_SYNONYMS`) while keeping TaxonKit imports as exact-match rules. This avoids slow refreshes and reduces over-matching risk.

Post-TaxonKit genus-level audit:

- Recomputed raw-host mappings increased from 1,357,159 to 1,386,512 rows.
- Raw-present unresolved host rows decreased from 45,362 to 16,009.
- Remaining unresolved split:
  - `ambiguous_free_text`: 9,344 rows.
  - `taxonomic_name_needing_taxid_lookup`: 5,963 rows.
  - `broad_host`: 501 rows.
  - `sample_source_misplaced_in_host`: 201 rows.
- Four standardization refresh workers were started to apply the new rules to queued genus metadata outputs.

## Host Standardization Refresh Completion

Date: 2026-04-28

- Genus standardization refresh completed successfully.
- Refresh tasks completed: 5,065.
- Refresh task failures: 0.
- Clean rows rewritten by refresh: 2,582,251.
- The website remained healthy after refresh.

Final post-refresh host audit:

- Genus files scanned: 5,066.
- Genus rows scanned: 2,590,072.
- Raw host present: 1,408,789 rows.
- Raw host missing: 1,181,283 rows.
- Stored `Host_SD` mapped after refresh: 1,535,148 rows.
- Recomputed raw-host-only mappings with current rules: 1,386,561 rows.
- Raw-present unresolved host rows: 16,009.
- Non-host source values detected in host fields: 6,219 rows.
- File errors: 0.

Remaining unresolved host classes:

- `ambiguous_free_text`: 9,344 rows.
- `taxonomic_name_needing_taxid_lookup`: 5,963 rows.
- `broad_host`: 501 rows.
- `sample_source_misplaced_in_host`: 201 rows.

## Host Broad-Group And Misplaced-Source Rules

Date: 2026-04-28

- Added conservative broad-host rules for remaining values such as `wild bird`, `bird`, `plant`, `animals`, `reptile`, `insect`, `mammal`, and `rodents`.
- Added conservative controlled-category rules for obvious host-field source/sample values such as dairy/milk products, sediment/sludge, hospital surfaces, manure, gut/intestinal material, blood, culture media, and culture collection terms.
- Created backups before editing:
  - `fetchm_webapp/standardization/host_synonyms.before_broad_host_2026-04-28.csv`
  - `fetchm_webapp/standardization/controlled_categories.before_host_source_2026-04-28.csv`
- Created a focused taxonomy review file:
  - `fetchm_webapp/standardization/review/host_review/host_remaining_taxonomy_candidates_for_review.csv`

Post-rule recomputation before refresh:

- Recomputed raw-host mappings: 1,387,401 rows.
- Raw-present unresolved host rows decreased from 16,009 to 15,172.
- Remaining unresolved classes:
  - `ambiguous_free_text`: 9,344 rows.
  - `taxonomic_name_needing_taxid_lookup`: 5,628 rows.
  - `sample_source_misplaced_in_host`: 200 rows.
- Broad-host unresolved class was eliminated under the current audit rules.
- A new genus standardization refresh was queued for 5,066 eligible genus metadata outputs.

## Host Broad-Group Refresh Completion

Date: 2026-04-28

- Genus standardization refresh completed for the broad-host/source cleanup.
- Refresh tasks completed: 5,066.
- Refresh task failures: 0.
- Clean rows rewritten: 2,595,107.
- Total rows processed by refresh: 2,597,418.
- Website remained healthy after refresh.

Final post-refresh host audit:

- Genus files scanned: 5,066.
- Genus rows scanned: 2,597,471.
- Raw host present: 1,411,817 rows.
- Raw host missing: 1,185,654 rows.
- Stored `Host_SD` mapped after refresh: 1,524,906 rows.
- Recomputed raw-host-only mappings with current rules: 1,390,138 rows.
- Raw-present unresolved host rows: 15,459.
- Non-host source values detected in host fields: 6,220 rows.
- File errors: 0.

Remaining unresolved classes:

- `ambiguous_free_text`: 9,620 rows.
- `taxonomic_name_needing_taxid_lookup`: 5,639 rows.
- `sample_source_misplaced_in_host`: 200 rows.

## ChatGPT Host Manual Review Suggestions

Date: 2026-04-28

- Imported and reviewed `/home/ai-pc/Work/dulab206/host_manual_review_suggestions.csv`.
- The file contained 9,359 suggested rows, but no TaxIDs, so it is a triage/review aid rather than a directly importable rule file.
- Current unresolved overlap: 3,749 unique values representing 15,184 rows.
- Overlapping suggestion decisions:
  - `taxonomy_candidate`: 1,843 unique values.
  - `ambiguous`: 1,812 unique values.
  - `broad_host`: 57 unique values.
  - `non_host_source`: 33 unique values.
  - `missing`: 4 unique values.
- Created filtered review files under:
  - `fetchm_webapp/standardization/review/host_review/chatgpt_suggestions/`

Safety review:

- Do not import the file blindly. Some suggestions are incorrect or too broad, such as `Waterfowl` marked as `non_host_source` and `laboratory` marked as `broad_host`.
- Applied only a conservative subset:
  - Common host aliases with clear TaxIDs: `zebrafish`, `cows`, `Rattus`, `rats`, `frog`, `catfish`, `Vertebrata`, and `Plantae`.
  - Obvious environmental/source terms: seawater variants, groundwater, freshwater, and soil-label variants.
  - Missing-value parser updated for leading-number variants such as `10not applicable`.

Post-import recomputation:

- Raw-present unresolved host rows decreased from 15,459 to 15,135.
- Recomputed raw-host mappings increased to 1,390,460 rows.
- Remaining unresolved classes:
  - `ambiguous_free_text`: 9,423 rows.
  - `taxonomic_name_needing_taxid_lookup`: 5,512 rows.
  - `sample_source_misplaced_in_host`: 200 rows.
- Regenerated focused taxonomy review file:
  - `fetchm_webapp/standardization/review/host_review/host_remaining_taxonomy_candidates_for_review.csv`

## Context-Aware Host Refinement Tool

Date: 2026-04-28

- Added `fetchm_webapp/tools/build_host_context_refinement.py`.
- The tool scans unresolved Host values across genus metadata and uses nearby metadata columns as evidence:
  - `Isolation Source`
  - `Isolation Site`
  - `Sample Type`
  - `Environment Medium`
  - `Environment (Broad Scale)`
  - `Environment (Local Scale)`
- Output file:
  - `fetchm_webapp/standardization/review/host_review/context_refinement/host_context_refinement_suggestions.csv`
- Summary file:
  - `fetchm_webapp/standardization/review/host_review/context_refinement/host_context_refinement_summary.csv`

Latest context-aware review output:

- Files scanned: 5,066.
- Suggested unresolved values found: 3,731.
- Rows represented: 15,135.
- `taxonomy_lookup`: 1,826 unique values.
- `needs_manual_review`: 1,676 unique values.
- `non_host_source`: 227 unique values.
- `missing`: 2 unique values.

Implementation decision:

- AI/embedding should not auto-approve mappings.
- Primary embedding model for ambiguous text review should be `BAAI/bge-small-en-v1.5` or another BGE-small variant because it is compact, fast, and general-purpose.
- SapBERT can be tested later for biomedical/entity-heavy terms, but should be optional because many FetchM values are environmental, food, host, and free-text source labels rather than strictly biomedical names.
- The immediate pipeline remains: exact rules, NCBI Taxonomy/TaxonKit, controlled source/sample/environment mapping, fuzzy spelling suggestions, then BGE-small assisted clustering/review for ambiguous free text.
- Added app-level missing handling for `no collected`, `not collect`, and `unidentified` variants.

## Host Fuzzy Spelling Review Layer

Date: 2026-04-28

- Added `rapidfuzz>=3.9` to the web app requirements.
- Added fuzzy host typo review to `fetchm_webapp/tools/build_host_context_refinement.py`.
- Fuzzy matching is intentionally placed before embeddings and after exact host synonym lookup.
- The fuzzy layer uses direct edit-distance ratio rather than partial/weighted matching. This is safer for biological names because partial matching can produce false matches such as broad common names being forced to a specific species.
- Thresholds:
  - `approve_host_fuzzy`: score >= 94, still review before converting into a permanent synonym.
  - `fuzzy_review`: score >= 88, manual review required.
- The tightened scan produced:
  - Files scanned: 5,066.
  - Suggested unresolved values found: 3,728.
  - Rows represented: 15,015.
  - `approve_host_fuzzy`: 94 unique values.
  - `fuzzy_review`: 128 unique values.
  - `taxonomy_lookup`: 1,627 unique values.
  - `non_host_source`: 225 unique values.
  - `needs_manual_review`: 1,654 unique values.
- Good high-confidence examples include:
  - `Homos sapiens` -> `Homo sapiens` / 9606.
  - `Hom sapiens` -> `Homo sapiens` / 9606.
  - `Oncorhnychus tshawytscha` -> `Oncorhynchus tshawytscha` / 74940.
  - `Chlrocebus sabaeus` -> `Chlorocebus sabaeus` / 60711.
  - `Arabodopsis thaliana` -> `Arabidopsis thaliana` / 3702.
- Review caution:
  - Medium-score suggestions can still be wrong, for example broad or genus-level names may be incorrectly matched to a specific species.
  - Do not auto-import `fuzzy_review` rows. Use them as review candidates only.

Manual review result:

- Reviewed all 222 fuzzy host candidates.
- Accepted 196 candidates as defensible host synonym rules.
- Added 193 new entries to `fetchm_webapp/standardization/host_synonyms.csv`; 3 accepted mappings were already present.
- Rejected 20 unsafe fuzzy mappings, including examples such as:
  - `Caprinae` -> `Capra hircus`
  - `Anser domesticus` -> `Passer domesticus`
  - `Botaurus` -> `Bos taurus`
  - `Ulmus sp.` -> `Mus sp.`
  - `Japanese Beetle` -> `Japanese eel`
  - `Morus sp.` -> `Orius sp.`
- Deferred 6 isolate-level `sp.` mappings for later context review.
- Decision audit file:
  - `fetchm_webapp/standardization/review/host_review/context_refinement/host_fuzzy_review_decisions.csv`
- CSV validation after import:
  - `host_synonyms.csv` rows: 5,407.
  - Missing required fields: 0.
- Follow-up correction:
  - `Botaurus` was verified with local NCBI Taxonomy as a valid genus-level bird host, TaxID 110660.
  - The original fuzzy suggestion `Botaurus` -> `Bos taurus` was correctly rejected as biologically wrong.
  - Added `Botaurus` -> `Botaurus` / 110660 as a reviewed genus-level host synonym.

Pipeline correction:

- Updated `fetchm_webapp/tools/build_host_context_refinement.py` so host review now follows the intended order:
  - Missing-value detection.
  - Exact reviewed host synonym match.
  - Direct local NCBI Taxonomy/TaxonKit `name2taxid` lookup when TaxonKit is available.
  - Fuzzy spelling/spacing match against already TaxID-backed host references.
  - Controlled source/sample/environment routing.
  - Taxonomy lookup/manual review or later BGE-small embedding review for unresolved semantic cases.
- Fuzzy suggestions now explicitly state that the candidate target came from an NCBI-validated reference.
- Direct TaxonKit lookup is optional and fail-safe: if `taxonkit` is unavailable in an execution environment, the tool continues with the existing reviewed-reference/fuzzy workflow.
- Container support:
  - Added TaxonKit v0.20.0 to the FetchM web Docker image.
  - Mounted the local NCBI Taxonomy database from `/home/ai-pc/.taxonkit` into the web container at `/root/.taxonkit` as read-only.
  - This lets the web-container review tooling run direct `taxonkit name2taxid` before fuzzy matching without copying the ~550 MB taxonomy database into the image.

## Context-Aware Host Recovery

Date: 2026-04-28

- Expanded `fetchm_webapp/tools/build_host_context_refinement.py` from unresolved non-empty Host review into context-aware recovery.
- The tool now performs a second review pass for rows where Host is missing, absent, ambiguous, or appears to contain non-host source text.
- Secondary fields scanned for recovery evidence:
  - `BioSample Host`
  - `BioSample Specific Host`
  - `BioSample NAT Host`
  - `BioSample LAB Host`
  - `BioSample Host Common Name`
  - `BioSample Common Name`
  - `Isolation Source`
  - `Isolation Site`
  - `Sample Type`
  - `Environment Medium`
  - `Environment (Broad Scale)`
  - `Environment (Local Scale)`
- New audit output:
  - `host_context_recovery_candidates.csv`
- Output purpose:
  - Recover true host candidates from secondary fields when primary Host is missing or unusable.
  - Route misplaced source/sample/environment evidence to the appropriate standardized destination instead of forcing it into `Host_SD`.
  - Keep all results review-gated before permanent rules are added.
- Added `--max-files` and `--max-rows` options for safe smoke testing because full context recovery scans many missing-host rows.
- Tightened context routing so values from environmental columns preferentially map to `Environment_Medium_SD`, avoiding cases where environmental fields were routed to `Sample_Type_SD` only because of broad shared terms.
- Full context-recovery audit completed:
  - Files scanned: 5,066.
  - Rows scanned: 2,597,471.
  - Context recovery candidates: 25,945.
  - Row-level opportunities represented: 660,023.
  - Candidate destinations: `Sample_Type_SD` 12,723, `Environment_Medium_SD` 11,220, `Host_SD` 1,985, `Isolation_Source_SD` 17.
- Conservative rule application:
  - Reviewed and approved 832 high-confidence context-recovery rules.
  - Approved 811 source/sample/environment routing rules.
  - Approved 21 host-recovery rules.
  - Added 783 new controlled-category rules.
  - Added 10 new host synonym rules; the remaining approved host mappings were already covered by existing rules.
  - Audit file: `fetchm_webapp/standardization/review/host_review/context_recovery_full/context_recovery_approved_rules.csv`.
- Safety decision:
  - Did not bulk-approve risky broad host mappings such as `animal` -> `Metazoa`, animal production labels, or age/context words like `infant`.
  - Corrected `waste water from culvert` to map to `wastewater` rather than generic `water`.

## Host Taxonomy Lineage Enrichment

Date: 2026-04-28

- Added rank-aware host enrichment columns to managed metadata:
  - `Host_Original`
  - `Host_Cleaned`
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
  - `Host_Age_Group_SD`
  - `Host_Production_Context_SD`
  - `Host_Anatomical_Site_SD`
  - `Host_Match_Method`
  - `Host_Confidence`
  - `Host_Review_Status`
- Existing columns `Host_SD`, `Host_TaxID`, `Host_SD_Method`, and `Host_SD_Confidence` were preserved for backward compatibility.
- Added TaxonKit lineage enrichment from `Host_TaxID`.
- Mounted `/home/ai-pc/.taxonkit` into standardization workers as read-only so future refreshes can populate lineage columns at scale.
- Corrected broad host behavior for rank-aware outputs:
  - `bird` -> `Aves` / 8782, rank `class`.
  - `mammal` -> `Mammalia` / 40674, rank `class`.
  - `rodent` -> `Rodentia` / 9989, rank `order`.
- Validation examples in the running web container:
  - `human` -> `Homo sapiens` / 9606, rank `species`, class `Mammalia`, order `Primates`, family `Hominidae`, genus `Homo`, species `Homo sapiens`, common name `human`.
  - `chicken` -> `Gallus gallus` / 9031, rank `species`, class `Aves`, order `Galliformes`.
  - `Botaurus` -> `Botaurus` / 110660, rank `genus`, class `Aves`, order `Pelecaniformes`, family `Ardeidae`, genus `Botaurus`.
  - `human stool` context recovery populates `Host_SD=Homo sapiens`, `Host_Anatomical_Site_SD=feces/stool`, and `Sample_Type_SD=feces/stool`.

## AI-Reviewed Host Import

Date: 2026-04-29

- External AI review of `remaining_host_review_detailed.csv` added `ai_recommendation` and `ai_review_note` columns.
- Imported only conservative decisions into permanent rule files:
  - `approve_direct` and `approve_genus_level` -> `standardization/host_synonyms.csv`.
  - `approve_blank` and `apply_only_if_host_empty` -> `standardization/host_negative_rules.csv`.
  - `manual_review` and `do_not_apply` were intentionally not imported.
- Imported rule counts:
  - Positive host rules added: 571 unique values.
  - Negative host/non-host/not-identifiable rules added: 168 unique values.
- Safety checks after import:
  - `Coffea`, `Bathyopsurus nybelini specimen AT50-02-018`, and `village weaver` map to Host_SD/TaxID.
  - AI-rejected microbial value `Pseudomonas` remains `review_needed` instead of becoming a host mapping.
  - Lab/person/source values such as `N. Ennis, Tisa lab UNH`, `Instituto de Productos Lacteos de Asturias (IPLA)-CSIC`, and `Morbier Cheese` are treated as non-host source values.

### Ready-to-import host rule supplement

- Reviewed `/home/ai-pc/Work/dulab206/host_rules_AI_approved_ready_to_import.csv`.
- The file contained 557 approved Host_SD mappings.
- 459 were already represented exactly in `host_synonyms.csv`.
- 95 additional non-conflicting rules were appended to `host_synonyms.csv`; three apparent new rows were already covered after normalization/equivalent-key handling.
- No conflicting mappings were found.
- Representative new checks:
  - `nestling stork` -> `Aves` / 8782.
  - `seabird` -> `Aves` / 8782.
  - `Pogona` -> `Pogona` / 103695.
  - `pufferfish` -> `Tetraodontiformes` / 31031.
  - `Albino Bristlenose Pleco` -> `Ancistrus` / 52070.
