# FetchM WEB Research Paper Notes

This file is a running evidence log for results, methods, and interpretation points that may be useful when writing a manuscript about FetchM WEB. Add important audit results here whenever the tool, metadata model, or standardization workflow improves.

## Current Analysis Scope

- Main evaluation level: genus-level metadata.
- Reason: species-level records are often derived from, nested under, or overlapping with genus-level metadata. For tool-development decisions, genus-level updates reduce redundant evaluation while still covering broad taxonomic and metadata diversity.
- Species-level metadata should remain available to users, but broad standardization logic should be validated at genus level first, then propagated to species-level data when needed.

## Managed Metadata Scale

- Total managed metadata files processed in the latest geography backfill: 38,086.
- Total managed metadata rows processed in the latest geography backfill: 4,980,609.
- Genus-level files scanned in the latest geography audit: 5,066.
- Genus-level rows scanned in the latest geography audit: 2,590,009.

Evidence files:

- `fetchm_webapp/standardization/review/geography_backfill/geography_backfill_summary.csv`
- `fetchm_webapp/standardization/review/geography_review/geography_audit_summary.csv`

## Metadata Standardization Principles

- Preserve original metadata values exactly as collected from NCBI/BioSample.
- Add standardized values in separate derived columns instead of overwriting original columns.
- Use rule-based standardization first because it is auditable, reproducible, and fast.
- Use ontology/dictionary mapping for stable domains such as host, geography, and environmental categories.
- Use conservative recovery from messy text fields only when false-positive risk is controlled.
- Keep provenance columns when values are inferred from non-primary fields.

## Geography Standardization

### Implemented Behavior

Country, continent, and subcontinent are derived using a conservative hierarchy:

1. Trusted geography fields, including `Country` and `Geographic Location`.
2. Secondary recovery only when trusted geography fields are absent or unmapped.
3. Secondary fields include isolation source, BioSample source name, BioSample description/title, and ENV local/broad scale.
4. False-positive blockers prevent incorrect mapping from terms such as `ground turkey`, `Guinea pig`, `Norway rat`, and `A. niger`.
5. Provenance fields are produced where possible: `Country_Source`, `Country_Confidence`, and `Country_Evidence`.
6. `Geo_Recovery_Status` records whether the assignment came from `trusted_primary`, `reviewed_secondary`, `rule_secondary`, `absent`, or `unknown` evidence.

Reviewed secondary geography rules are stored in:

- `fetchm_webapp/standardization/geography_reviewed_rules.csv`

Future audit review queues are written to:

- `fetchm_webapp/standardization/review/geography_review/geography_secondary_review_queue.csv`

### Genus-Level Result

- Genus-level mapped country rows improved from 2,257,375 to 2,258,435 after safe secondary recovery plus reviewed secondary geography rules.
- Additional genus-level rows recovered by secondary geography logic: 1,060.
- Genus-level country missing or unmapped rows decreased from 332,634 to 331,574.
- Continent/subcontinent assignment errors after correction: 0 missing continent, 0 missing subcontinent, 0 continent mismatch, and 0 subcontinent mismatch when country is known.

### Whole Managed Metadata Context

- Whole managed metadata rows recovered by secondary geography logic: 1,825.
- Whole managed metadata country missing/unmapped rows decreased from 582,300 to 580,475.

### Interpretation

The geography update improves completeness while remaining conservative. The secondary recovery step adds useful geographic information from non-primary metadata fields, but avoids automatic acceptance of ambiguous country-like words that often occur in host names, food terms, organism names, or environmental descriptions.

## Remaining Geography Review Items

Initial secondary geography review found 591 genus-level cases where a country-like token appeared in non-primary metadata fields but was not automatically accepted. After adding false-positive blockers for animal/common-name/food-style/protocol terms, the remaining review set decreased to 20 candidate rows.

These candidates are here:

- `fetchm_webapp/standardization/review/geography_review/country_missing_recoverable_other_columns.csv`

Related summaries:

- `fetchm_webapp/standardization/review/geography_review/hidden_country_by_source_column.csv`
- `fetchm_webapp/standardization/review/geography_review/hidden_country_source_columns.csv`

Why they remain unresolved:

- The following false-positive classes are now excluded from the review candidate set: `ground turkey`, turkey meat/embryo/poult/sinus/food terms, `Guinea pig`, `guinea fowl`, `Norway rat`, `A. niger`, `Aspergillus niger`, `Cordylus niger`, `Lizard (Cordylus niger)`, country-plus-style food descriptors such as `Honduras style queso fresco cheese`, and protocol text such as `Protocols: ...`.
- The remaining 20 candidates were manually reviewed and accepted as plausible location evidence.
- A genus-only backfill accepted all 20 reviewed rows. The final 3 rows corresponded to exact `Host = USA` cases and were recovered after adding a narrow reviewed host-field exception.
- Final genus-level audit result: `country_recoverable_from_other_metadata_columns = 0`.
- Current examples include `soil around the Arctic Ocean`, `Blood - animal United Kingdom`, `Soil, Moscow, USSR`, `Gulf of Mexico`, `Blue Lagoon, Iceland at 20 cm`, and clinical/source strings with explicit country names.

## Host Standardization

Current strategy:

- Keep original host values.
- Add standardized host fields such as `Host_SD`.
- Use rule-based cleaning and synonym mapping first.
- Validate true host names using NCBI Taxonomy where possible.
- Avoid forcing non-host source terms into host fields.
- Leave true host terms unchanged when no confident TaxID-backed rule is available.

Important interpretation:

- Values such as food, water, soil, wastewater, and environmental materials should not be forced into `Host_SD`.
- If a non-host value appears in a host column, it should be copied or mapped into an appropriate source/environment standardized field, not treated as a biological host.

Latest genus-level host audit:

- Genus files scanned: 5,066.
- Genus rows scanned: 2,590,009.
- Raw host present: 1,408,913 rows.
- Raw host missing: 1,181,269 rows after strengthened missing-token handling.
- Stored `Host_SD` mapped: 1,505,422 rows.
- Recomputed raw-host-only mappings using current rules: 1,357,159 rows.
- Raw present but still unmapped after recomputation: 45,362 rows.
- Non-host source values detected in host fields after recomputation: 6,219 rows.

Interpretation:

- The large missing host count is mostly true source absence in NCBI/BioSample metadata, not only a FetchM parsing failure.
- Stored mapped host rows are higher than raw host-present rows because the webapp can infer host from safe context fields in addition to the raw `Host` field.
- The main remaining gap is the long tail of true Latin host names, broad biological groups, and source/sample terms present in host-like fields.
- Strengthened missing-token detection now treats values such as `Not Applicable [GENEPIO:0001619]` as missing.
- Expanded non-host source detection now moves stool/feces/meat/food/environment/metagenome/algae/source terms out of the true-host unmapped pool while preserving true host matches such as `human stool`.

Evidence files:

- `fetchm_webapp/standardization/review/host_review/host_audit_summary.csv`
- `fetchm_webapp/standardization/review/host_review/host_unmapped_review_queue.csv`
- `fetchm_webapp/standardization/review/host_review/host_non_host_source_values.csv`
- `fetchm_webapp/standardization/review/host_review/host_mapped_values.csv`

## Isolation Source And Sample-Type Standardization

Current strategy:

- Avoid over-generalizing broad terms such as `water`, `food`, `swab`, `culture`, and `feces/stool`.
- Preserve meaningful subcategories where possible.
- Use broad categories plus more specific standardized labels when needed.

Examples of desired specificity:

- Swab terms should retain meaningful site information where available, such as rectal swab, nasal swab, oral swab, wound swab, skin swab, cloacal swab, environmental swab, and food-contact swab.
- Water terms should distinguish river water, lake water, pond water, freshwater, seawater, estuarine water, wastewater, hospital wastewater, hot spring water, and irrigation water when possible.
- Food terms should distinguish food product, food processing environment, food-contact surface, non-food-contact surface, pet food, ready-to-eat food, dairy food, meat, and seafood where possible.
- Culture terms should distinguish pure culture, mixed culture, cell culture, microbial culture, enrichment culture, liquid culture, plate culture, and metagenomic assembly where possible.
- Fecal/gut terms should distinguish feces/stool, fecal swab, rectal swab, gut content, intestinal content, manure, and cloacal sample where possible.

Evidence and review files:

- `fetchm_webapp/standardization/review/isolation_source_comprehensive_review.csv`
- `fetchm_webapp/standardization/review/isolation_source_remaining_review.csv`

## Collection Date And Collection Year

Current standardization goal:

- Extract collection year whenever possible from collection-date fields.
- Handle misformatted or partial date values conservatively.
- Keep original date strings for traceability.
- Add provenance fields: `Collection_Date_Source`, `Collection_Date_Evidence`, and `Collection_Date_Recovery_Status`.
- Use trusted primary date aliases first, then conservative secondary text recovery only when explicit collection/isolation/sampling context is present.
- Avoid false positives from protocol/publication/submission/sequencing text, accession-like strings, ontology IDs, malformed dates, and `missing: data agreement` text.

Evidence file:

- `fetchm_webapp/standardization/review/collection_date_comprehensive_review.csv`
- `fetchm_webapp/standardization/review/collection_date_review/collection_date_audit_summary.csv`
- `fetchm_webapp/standardization/review/collection_date_review/collection_date_secondary_review_queue.csv`
- `fetchm_webapp/standardization/review/collection_date_backfill/collection_date_backfill_summary.csv`

Latest genus-level result:

- Genus files scanned: 5,066.
- Genus rows scanned: 2,590,009.
- Collection date/year present after backfill: 2,165,462 rows.
- Collection date/year missing or unmapped after backfill: 424,547 rows.
- Additional rows recovered by the collection-date backfill: 497 total.
- Primary alias recovery: 118 rows.
- Conservative secondary text recovery: 379 rows.
- Final recoverable primary candidates after backfill: 0.
- Final recoverable secondary candidates after backfill: 0.

## Manuscript-Relevant Claims To Support Later

- FetchM WEB can build large-scale taxon-specific metadata resources from NCBI/BioSample.
- The webapp preserves raw metadata while adding standardized, auditable derived fields.
- Geography standardization can be improved with conservative secondary recovery from non-primary metadata fields.
- Provenance fields make inferred geographic assignments traceable.
- Conservative false-positive blocking is necessary because country names frequently appear in non-geographic biological and food terms.
- Genus-level validation is an efficient development strategy before propagating standardized logic to species-level datasets.

## Pending Paper-Focused Analyses

- Quantify host standardization completeness before and after synonym and NCBI Taxonomy mapping.
- Quantify isolation-source and sample-type standardization completeness before and after controlled-category refinement.
- Quantify collection-year recovery before and after date parsing improvements.
- Produce tables for metadata completeness by field.
- Produce examples of raw-to-standardized mappings for geography, host, isolation source, sample type, and collection year.
- Record runtime and scalability metrics for metadata building, standardization backfill, and sequence download.

## Running Notes

- Add future important results here immediately after audits or backfills.
- Prefer exact counts, before/after values, and paths to evidence files.
- If a value is inferred rather than directly observed, record the source field and the confidence/provenance logic.

## Host Review Workflow

Date: 2026-04-28

FetchM WEB now exports an auditable host review workflow for genus-level metadata standardization. The review queue uses explicit decisions: `approve_host`, `non_host_source`, `missing`, `broad_host`, and `needs_manual_review`.

Latest genus-level host audit:

- Genus files scanned: 5,066.
- Genus rows scanned: 2,590,009.
- Raw host present: 1,408,740 rows.
- Raw host missing: 1,181,269 rows.
- Stored `Host_SD` mapped: 1,505,422 rows.
- Recomputed raw-host-only mappings using current rules: 1,357,159 rows.
- Raw present but still unmapped after recomputation: 45,362 rows.
- Non-host source values detected in host fields after recomputation: 6,219 rows.

New workflow files:

- `fetchm_webapp/standardization/review/host_review/host_unmapped_review_queue.csv`
- `fetchm_webapp/standardization/review/host_review/host_taxonomy_name_candidates.csv`
- `fetchm_webapp/standardization/review/host_review/host_non_host_source_values.csv`
- `fetchm_webapp/standardization/review/host_review/README.md`

Planned next step:

- Resolve `host_taxonomy_name_candidates.csv` with NCBI Taxonomy/TaxonKit, then merge confirmed scientific names into `fetchm_webapp/standardization/host_synonyms.csv`.
- Route confirmed non-host source/sample/environment terms into `fetchm_webapp/standardization/controlled_categories.csv`.

Host unresolved-class split:

- Taxonomic names needing TaxID lookup: 35,316 rows.
- Ambiguous free text: 9,344 rows.
- Broad host groups: 501 rows.
- Sample/source values misplaced in the host column: 201 rows.

This supports a rule-first standardization hierarchy: exact dictionary rules, NCBI Taxonomy lookup for scientific names, controlled source/sample/environment mapping, fuzzy matching for spelling errors, and embedding/AI only for ambiguous free-text review support.

## Host Taxonomy Resolution Result

Date: 2026-04-28

TaxonKit/NCBI Taxonomy resolution was applied to the genus-level host taxonomy candidate list.

Key result:

- 5,169 high-confidence scientific-name-like host rules were added to `fetchm_webapp/standardization/host_synonyms.csv`.
- Recomputed raw-host mappings improved from 1,357,159 to 1,386,512 rows.
- Raw-present unresolved host rows decreased from 45,362 to 16,009 rows.
- Ambiguous names with multiple TaxIDs and non-scientific/common-name matches were excluded from automatic import.

Remaining host standardization work:

- Ambiguous free text: 9,344 rows.
- Taxonomic names still needing TaxID lookup or manual review: 5,963 rows.
- Broad host groups: 501 rows.
- Sample/source values misplaced in host fields: 201 rows.

Genus refresh completion:

- 5,065 genus refresh tasks completed with 0 failures.
- 2,582,251 clean metadata rows were rewritten.
- Stored `Host_SD` mapped rows after refresh: 1,535,148.
- Final host audit file errors: 0.

Broad-host and source cleanup:

- Conservative broad-host rules were added for bird/plant/animal/reptile/insect/mammal-style groups.
- Conservative controlled-category rules were added for obvious dairy/milk, sediment/sludge, hospital surface, manure, gut/intestinal, blood, and culture-medium/source terms misplaced in host fields.
- Raw-present unresolved host rows decreased further from 16,009 to 15,172 in recomputation.
- The remaining taxonomy-candidate review file is `fetchm_webapp/standardization/review/host_review/host_remaining_taxonomy_candidates_for_review.csv`.

Broad-host/source refresh completion:

- 5,066 genus refresh tasks completed with 0 failures.
- 2,595,107 clean metadata rows were rewritten.
- Stored `Host_SD` mapped rows after refresh: 1,524,906.
- Raw-present unresolved host rows after refresh: 15,459.
- Final host audit file errors: 0.

ChatGPT host suggestion review:

- Reviewed `/home/ai-pc/Work/dulab206/host_manual_review_suggestions.csv` as a triage file, not a directly importable rules file because it lacks TaxIDs.
- Created filtered review files in `fetchm_webapp/standardization/review/host_review/chatgpt_suggestions/`.
- Applied only conservative, manually defensible suggestions.
- Raw-present unresolved host rows decreased from 15,459 to 15,135 in recomputation.
- Remaining taxonomy-candidate rows decreased to 5,512.

Context-aware host refinement:

- Added a context-aware review-assist tool that uses nearby metadata fields to suggest whether unresolved Host values are missing, non-host source/sample/environment values, taxonomy lookup candidates, or manual-review items.
- Output: `fetchm_webapp/standardization/review/host_review/context_refinement/host_context_refinement_suggestions.csv`.
- Latest scan represented 15,135 unresolved rows and suggested 227 non-host/source candidates plus 2 missing-value candidates.
- BGE-small is the preferred first embedding model for later ambiguous-text clustering; SapBERT should remain an optional biomedical upgrade after the rule/context/TaxonKit pipeline is exhausted.

Fuzzy host typo refinement:

- RapidFuzz was added as a rule-first spelling correction layer before embeddings.
- The tool now flags high-confidence typo variants such as `Homos sapiens` and `Hom sapiens` as candidate mappings to `Homo sapiens` / 9606.
- Direct edit-distance ratio is used instead of partial matching to reduce false biological assignments.
- Latest tightened scan produced 94 high-confidence fuzzy candidates and 128 manual-review fuzzy candidates among the remaining unresolved host values.
- This supports the paper-level claim that FetchM preserves original metadata while adding standardized fields through a conservative hierarchy: exact dictionary, taxonomy validation, controlled vocabulary routing, fuzzy typo review, and optional embedding-assisted review for the ambiguous long tail.
- Manual review accepted 196 of 222 fuzzy candidates and added 193 new host synonym rules. Twenty unsafe fuzzy matches were rejected and six isolate-level `sp.` mappings were deferred.
- This review demonstrates why fuzzy matching is useful but should remain review-gated: it captured true variants such as `Hom sapiens` and `Oncorhnychus tshawytscha`, while preventing false mappings such as `Botaurus` to `Bos taurus` and `Japanese Beetle` to `Japanese eel`.
- A follow-up taxonomy check confirmed `Botaurus` is a valid bird genus, so it was added as a genus-level host mapping to TaxID 110660 rather than the incorrect fuzzy target `Bos taurus`.
- The host review pipeline was corrected so direct NCBI/TaxonKit lookup is attempted before fuzzy matching when TaxonKit is available. Fuzzy matching is now framed as typo correction against TaxID-backed references, not as semantic inference. Embedding review remains reserved for unresolved free-text cases.
- TaxonKit v0.20.0 was added to the web container and the local taxonomy database is mounted read-only, allowing the containerized review workflow to perform direct NCBI Taxonomy lookup before fuzzy review.
- Context-aware host recovery was added as a review/audit stage. It scans secondary BioSample host/source/sample/environment fields when primary Host is missing, ambiguous, or contains non-host source text. The output separates recoverable host evidence from source/sample/environment evidence, preserving original metadata and avoiding forced host assignments.
- The full context-recovery audit scanned 2,597,471 genus metadata rows and identified 25,945 unique recovery/routing candidates representing 660,023 row-level opportunities. A conservative first review approved 832 rules, adding 783 controlled source/sample/environment mappings and 10 new host synonym mappings. Broad or risky host inferences were intentionally not bulk-approved.
- Host standardization was expanded with taxonomy lineage enrichment. FetchM now preserves `Host_Original` and `Host_Cleaned`, keeps `Host_SD` and `Host_TaxID`, and adds rank/lineage fields from TaxonKit (`Host_Rank`, `Host_Superkingdom`, `Host_Phylum`, `Host_Class`, `Host_Order`, `Host_Family`, `Host_Genus`, `Host_Species`) plus common name, host context, age group, production context, anatomical site, match method, confidence, and review status. This allows broad hosts such as bird, mammal, and rodent to be represented as class/order-level taxa instead of being forced into species-level assignments.
- Host enrichment refresh was queued for 5,066 genus-level metadata files. To make the refresh feasible, runtime caches were added for TaxonKit lineage lookup and controlled-vocabulary context matching; dynamic per-row regex compilation was replaced with precompiled cached patterns. A Neisseria benchmark improved from approximately 43 seconds per 1,000 rows to approximately 9 seconds per 1,000 rows including one-time TaxonKit cache warm-up.

Post-refresh standardization quality audit:

- Genus-level audit scanned 5,066 clean metadata files and 2,595,189 rows with 0 file errors and 0 missing required standardized-column file hits.
- Host TaxID-standardized rows: 1,544,472 (59.51% of all genus rows). Context-recovered host rows: 157,696. Rows still requiring host review: 18,410.
- Geography quality was strong: country present in 2,259,546 rows (87.07%), with 0 country-continent mismatch rows and 0 country-subcontinent mismatch rows.
- Collection year was recoverable from standardized collection-date fields in 2,167,556 rows (83.52%).
- Source/sample/environment routing remains the next quality target: `Sample_Type_SD` present in 1,573,828 rows (60.64%), `Isolation_Source_SD` present in 1,491,426 rows (57.47%), and `Environment_Medium_SD` present in 188,845 rows (7.28%).
- Priority review files from this audit are stored under `fetchm_webapp/data/standardization_quality_audit/20260428_141854/`, especially `suspicious_source_like_mapped_hosts.csv`, `source_like_unmapped_hosts_for_review.csv`, and `top_host_review_needed.csv`.
- Follow-up non-host/source cleanup changed host review semantics so values classified as `non_host_source` keep `Host_SD` and `Host_TaxID` blank and use `Host_Review_Status=non_host_source` instead of `review_needed`. Host-context values are now also eligible to populate `Sample_Type_SD` when source/sample columns are absent. Additional routing rules were added for high-frequency non-host/geologic/source values such as `Subsurface shale`, `P-trap`, `U-Bend`, `natural / free-living`, `Dairy Farm, Water`, `Dairy Farm, Faecal`, `Hospital environment`, `laboratory`, `In vitro`, and shrimp/seafood-style source terms. A genus-level standardization refresh was queued to rewrite existing clean metadata with these updates.
- A taxonomy/broad-host mapping batch was added for high-frequency remaining review values. New mappings include explicit genus/species names such as `Chroicocephalus`, `Hordeum vulgare Golden Promise`, `Indigofera argentea`, `Milvus lineatus lineatus`, `Gasterosteus aculeatus`, and `Theobroma grandiflorum`, plus conservative broad/common host groups such as mink, marmot, turtle/Testudines, primates, Anatidae, seagull/Laridae, bison/Bison, monkey/Simiiformes, bat/Chiroptera, snake/Serpentes, sponge/Porifera, mollusc/Mollusca, and multiple common plant/crop hosts. Broad/common names were added with medium confidence so downstream reports preserve their taxonomic rank rather than treating them as species-level assignments. A genus-level standardization refresh was queued to rewrite the stored metadata with these mappings.
- Post-refresh audit after this taxonomy/broad-host batch scanned 5,066 genus files and 2,595,505 rows with 0 file errors. Host TaxID-standardized rows increased to 1,561,280 (60.15%), accepted host rows increased to 1,562,106, and `review_needed` rows decreased from 12,472 to 6,859. The next quality target is false-positive context host recovery from source text: examples include environmental swab sponge mapping to Porifera and sludge/wastewater source phrases mapping to Viridiplantae. These should be blocked or routed as source/sample/environment context, not host evidence.

False-positive source-as-host correction:

- Source-dominant host recovery was tightened so wastewater/sludge/treatment-plant, environmental sponge/swab, water/plankton, soil/sediment/rhizosphere, and plant-environment phrases no longer become host taxa unless explicit host-material evidence is present.
- Direct Host-field standardization now applies the same source-dominant block before dictionary/broad taxonomy matching. This prevents values such as `environmental swab sponge`, `Activated sludge of municipal wastewater treatment plants`, `Wastewater treatment plant`, `saline water ... plankton`, and `plant environment` from becoming Porifera, Viridiplantae, plankton, or other false host assignments.
- Positive host-material controls were preserved: `human feces` still maps to `Homo sapiens` / 9606 and `cattle feces` still maps to `Bos taurus` / 9913.
- Genus-level refresh completed for 5,066 taxa with 0 failures, processing 2,597,816 rows and rewriting 2,595,505 rows.
- Post-correction audit `fetchm_webapp/data/standardization_quality_audit/20260428_173719/` scanned 5,066 genus files and 2,595,570 rows with 0 file errors. Host TaxID-standardized rows were 1,548,058 (59.64%), accepted host rows were 1,548,343, `non_host_source` rows increased to 6,657, and `review_needed` rows were 6,914.
- Geography remained internally consistent: country present in 2,259,902 rows (87.07%), continent/subcontinent present in 2,260,594 rows, and both country-continent and country-subcontinent mismatch rows were 0.
- Collection year remained available in 2,167,803 rows (83.52%).
- Source/sample/environment coverage after correction: `Sample_Type_SD` present in 1,575,392 rows (60.70%), `Isolation_Source_SD` present in 1,489,898 rows (57.40%), and `Environment_Medium_SD` present in 190,985 rows (7.36%).
- Remaining priority review files are `top_host_review_needed.csv`, `suspicious_source_like_mapped_hosts.csv`, and `source_like_unmapped_hosts_for_review.csv` in the 20260428_173719 audit folder. Remaining review examples include `pet`, `Indigofera argentea Burm.f.`, `DH10B`, `Tique`, `coral`, `Invasive pneumococcal disease`, and dairy/bootsock/source-style values.

Conservative remaining-host review round:

- The 6,914 remaining `Host_Review_Status=review_needed` rows were bucketed into actionable classes in `fetchm_webapp/standardization/review/host_review/remaining_host_review_bucket_proposals_20260428.csv`.
- The review separated taxonomy candidates, broad/common host candidates, source/sample/environment values misplaced in Host, lab strain/identifier values, explicit missing/not-identifiable values, and ambiguous long-tail terms.
- A stricter TaxonKit-backed rule batch was generated in `conservative_host_synonyms_to_add_20260428.csv`; 72 conservative host synonym rules were added to `host_synonyms.csv`, representing 569 rows. Risky common-name species assignments and generic terms such as `pet`, `Bacteria`, `Fungi`, disease labels, and ambiguous common names were excluded.
- A conservative source-routing batch was generated in `conservative_controlled_categories_to_add_20260428.csv`; 27 controlled-category rules were added to route obvious food/source/environment/sample terms from Host into `Isolation_Source_SD`, `Sample_Type_SD`, or `Environment_Medium_SD`, representing 295 rows.
- Code-level `not_identifiable` handling was added for lab strain/identifier-like Host values such as `DH10B`, `SOLR`, `XL1-Blue`, `Parent`, numeric IDs, and collection-center identifiers. These keep `Host_SD` and `Host_TaxID` blank but receive `Host_Review_Status=not_identifiable` instead of remaining in manual host review.
- Exact controlled source/sample/environment terms appearing in Host are now marked `Host_Review_Status=non_host_source`, while still allowing their information to populate the appropriate standardized source/sample/environment columns.
- Validation examples before refresh: `DH10B` and `Parent` become `not_identifiable`; `Dairy Farm, Bootsock` becomes `non_host_source` and contributes `bootsock/environmental swab`; `Indigofera argentea Burm.f.` maps to `Indigofera argentea` / 198858; `Tilapia` maps to genus-level `Tilapia` / 8126; ambiguous `pet` and `Wolf` remain `review_needed` for later manual review.
- A genus-only standardization refresh was queued for 5,066 taxa so stored metadata can be rewritten with these conservative rules.
- Follow-up broad-host review mapped additional defensible higher-rank host terms. Examples include `Wolf`/`wolf` to `Canis lupus` / 9612, `Bacteria` to domain-level `Bacteria` / 2, `Fungi` and fungal phrases to kingdom-level `Fungi` / 4751, `coral` to `Anthozoa` / 6101, `crab` to `Brachyura` / 6752, `prawn` to `Dendrobranchiata` / 6684, `cockroach` to `Blattodea` / 85823, `salmon` to `Salmonidae` / 8015, `eel` to `Anguilliformes` / 7933, `dolphin` to `Delphinidae` / 9726, `flounder` to `Pleuronectiformes` / 8252, `bivalve` to `Bivalvia` / 6544, `Ant` to `Formicidae` / 36668, and `Starfish` to `Asteroidea` / 7588.
- `pet`, `Pets`, and `companion pet` were intentionally not mapped to a taxon. They now receive `Host_Review_Status=not_identifiable` while preserving `Host_Context_SD=pet/companion animal`, because pet is a companion-animal context rather than a biological taxon.
- A small host-taxonomy priority list was added so reviewed terms such as `Bacteria`, `prawn`, and `salmon` can map to their approved broad taxa instead of being intercepted by source/sample routing.
- A post-refresh audit exposed an over-aggressive source-routing regression: exact controlled source/sample terms were being checked before host dictionaries, causing true host phrases such as `Homo sapiens`, `human feces`, `cattle feces`, `Chicken feces`, `water deer`, and `water buffalo` to be marked `non_host_source`. The rule order was corrected so host dictionaries and host-priority mappings run before controlled source routing, and blank controlled-category entries no longer trigger non-host classification. Validation confirmed these true host examples map correctly again while `Dairy Farm, Bootsock` and `Food` remain `non_host_source`.
- Standardization throughput was improved before the corrective refresh by increasing dedicated standardization workers from 8 to 16 and adding large-taxon chunked standardization. Large taxa are now standardized and upserted in batches controlled by `FETCHM_WEBAPP_STANDARDIZATION_CHUNK_MIN_ROWS` and `FETCHM_WEBAPP_STANDARDIZATION_CHUNK_SIZE`, then the final TSV/CSV output is written once to avoid concurrent output-file corruption.

Additional source-context and host-review cleanup:

- The source-dominant host blocker was extended for false-positive source phrases such as `preprocessing plant water`, `Air sample from blueberry fieds`, `drag swab watermelon field`, `Milk powder plant`, `powdered infant formula production environment`, `Surface patient room`, and `Swab sample from powdered infant formula factory`. These now remain blank for `Host_SD`/`Host_TaxID` with `Host_Review_Status=non_host_source`.
- Positive host-material controls were rechecked and preserved: `human feces`, `cattle feces`, `Chicken feces`, `cattle manure`, `raw cow milk`, and `infant feces` still map to their host taxa.
- Missing/not-identifiable host handling was expanded for misspelled missing tokens (`mising`, `misisng`), disease labels such as `Invasive pneumococcal disease`, and geographic labels such as `Canada: Saskatchewan`.
- A new conservative reviewed-host batch was added for high-frequency remaining host values, including `dairy cows` to `Bos taurus`, `Wolbachia` to genus-level `Wolbachia`, `Chichen` to `Gallus gallus`, `Homme` to `Homo sapiens`, `Sable` to `Martes zibellina`, and additional validated plant/animal/common-host terms such as `douglas fir`, `sunflower`, `yellowtail`, `silkworm`, `sparrow`, `koala`, `housefly`, `gorilla`, `lion`, `Nematodes`, `dragonfly`, `Ostreidae`, and `Glicyne max`.
- `Nectomys melanius` was retained conservatively at genus level (`Nectomys` / 29116) because the local taxonomy database did not resolve the submitted species epithet.

Second remaining-host refinement round:

- After the 20260428_211128 audit, `Host_Review_Status=review_needed` was reduced to 4,034 rows. The remaining list was dominated by low-frequency common names, spelling variants, strain/lab labels, and source/location strings.
- A second conservative batch added rank-aware mappings for high-confidence host terms such as `Lasioglossum leviense` to genus-level `Lasioglossum`, `Great scallop` to `Pecten maximus`, `mongoose` to `Herpestidae`, `Kite` to `Milvus`, `Hairtail` to `Trichiuridae`, `Lepidochyelys olivacea` to `Lepidochelys olivacea`, `Thraupis gaucocolpa` to `Thraupis glaucocolpa`, plus many plant, bird, mammal, insect, mollusc, algal, and fish terms from the top review rows.
- Lab/identifier/noise values such as `E.coli`, `Uliege`, `TBG`, `lab`, `EMDH10B`, and several lab-strain labels were moved to `not_identifiable`.
- Non-host/source values such as `WW outflow Samariterstift`, `free living`, `Dairy waste`, `Rivers/natural pond`, `marl pit`, `seeds`, and `leafy green` were routed away from host assignment as non-host source/context.
- Validation controls confirmed the new rules map `Lasioglossum leviense`, `Great scallop`, `mongoose`, `Kite`, `Hairtail`, and common misspellings correctly, while preserving `human feces` as `Homo sapiens` and `preprocessing plant water` as `non_host_source`.

E. coli host-field handling:

- E. coli-style values in the `Host` field are now treated as source/context rather than biological host assignments. This avoids falsely reporting the sampled organism as `Escherichia coli` when the metadata likely describes an E. coli lab host, culture background, isolate context, or expression/culture system.
- Variants such as `E.coli`, `E. coli`, `E coli`, `E, coli`, `Escherichia coli`, `E. coli culture`, and `E. coli isolate` keep `Host_SD` and `Host_TaxID` blank with `Host_Review_Status=non_host_source`.
- The original text is preserved, `Host_Context_SD` records `Escherichia coli/lab bacterial culture`, and secondary standardization can recover `Sample_Type_SD=bacterial culture` or `pure/single culture` when no better sample/source field is present.
- This handling was generalized into a punctuation-tolerant lab/microbial host-context detector. It now covers variants such as `E-coli`, `Ecoli`, `E coli JM109`, `E. coli BL21`, `DH5alpha`, `DH5-alpha`, `DH10B`, `TOP10`, `XL1-Blue MRF`, `SOLR`, `ATCC strain`, `Control Strain`, `Vaccine strain`, and `ZymoBIOMICS Microbial Community Standard Strain`. These values are routed away from `Host_SD`, retain a lab/culture context label, and populate `Sample_Type_SD` as `bacterial culture` or `pure/single culture` where appropriate.
- The detector was expanded into a broader microbial self-descriptor classifier. Bacterial taxon names combined with culture/isolate/strain language, such as `Salmonella enterica strain`, `Pseudomonas culture`, `Bacillus isolate`, and `Staphylococcus aureus culture`, are now treated as source/sample descriptors rather than biological hosts. They keep `Host_SD` blank, record `Host_Context_SD=microbial self/lab culture descriptor`, and preferentially populate `Sample_Type_SD=pure/single culture`.
- Broad microbial host terms without culture/self-descriptor language, such as `Bacteria` and `Wolbachia`, remain rank-aware host taxonomy mappings. This preserves defensible broad host assignments while preventing lab/culture descriptors from being misreported as sampled hosts.
