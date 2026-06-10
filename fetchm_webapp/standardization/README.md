# Metadata Standardization

FetchM metadata standardization is deterministic by design. Production outputs must be reproducible from committed code, committed CSV rule files, and committed audit outputs.

## Rule Sources

- `host_synonyms.csv` stores reviewed host synonym and taxonomy mappings.
- `host_negative_rules.csv` stores reviewed values that must not become `Host_SD`.
- `host_context_rules.csv` stores reviewed non-taxonomic host-context labels that remain outside `Host_SD`.
- `controlled_categories.csv` stores reviewed source, sample, environment, disease, health-state, and site mappings.
- `approved_broad_categories.csv` defines the controlled broad vocabulary used by final QA gates.
- `geography_reviewed_rules.csv` and `collection_date_reviewed_rules.csv` store reviewed recovery rules for geography and collection date.

## Embedding-Assisted Review

BGE or other embedding models are allowed only as review assistants. They must not directly write final standardized metadata.

Approved workflow:

1. Export unresolved or `review_needed` values.
2. Cluster long-tail phrases with BGE or another embedding model.
3. Have a human/AI reviewer propose deterministic rules from the clusters.
4. Add only reviewed rules to committed CSV rule files or explicit code paths.
5. Run controlled-category and final metadata audits.
6. Rerun standardization only for affected taxa when rules change.
7. Commit the rule changes and audit evidence together.

Policy:

- Embedding suggestions are not production rules.
- Every production mapping must be traceable to deterministic code or a reviewed CSV row.
- Audits must show which rules were approved, which were skipped, and which values remain manual review.
- BGE should be used to reduce manual review effort, not to bypass review.

Current remaining issues are low-frequency curation issues, not a reason to replace the deterministic pipeline with model classification.

Host context-only rules are loaded from `host_context_rules.csv` and preserve broad labels such as `fish` or `marine invertebrate` without forcing a taxonomic `Host_SD`.
Microbial host exceptions must be explicit in `host_microbial_allowlist.csv`; non-allowlisted Bacteria, Archaea, and Viruses are demoted from `Host_SD` by the host taxonomy audit while eukaryotic algae, fungi, protists, plants, and animals remain valid when TaxID lineage supports them.

## Host Review Decisions

- **Approve exact host**: use when the submitted value resolves to a specific
  reviewed NCBI taxon. Populate `Host_SD` and `Host_TaxID`.
- **Approve broad host**: use when a defensible higher-rank NCBI taxon is the
  most specific safe assignment. Populate `Host_SD` and its higher-rank TaxID.
- **Context-only host**: use for biological descriptions such as `fish`,
  `algae`, or `zooplankton` that are useful but not defensible taxonomic
  assignments. Populate `Host_Context_SD` only.
- **Mark non-host source**: use for material, environment, person, institution,
  microbial taxon leakage, or other source text that is not the host organism.
- **Mark missing**: use only for explicit missing-value statements.
- **Ignore / not identifiable**: use when the text is present but no defensible
  host or context assignment can be made.

Production QA is available with:

```bash
python tools/qa_host_standardization.py --fail-on-leakage
```
