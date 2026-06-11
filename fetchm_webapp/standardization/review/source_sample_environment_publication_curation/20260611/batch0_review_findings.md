# Batch 0 Review Findings

- The baseline contains 2,080 exact standardized source labels affecting 190,284 rows.
- These labels are QA aggregates, not necessarily literal raw metadata values. Every rule batch must inspect contributing raw variants before changing routing.
- `clinical sample` is frequently a valid clinical-context label and often coexists with `Sample_Type_SD = clinical material`; it must not be blanket-suppressed.
- `respiratory sample` is frequently derived from anatomical raw values such as `Bronch` and coexists with a resolved respiratory `Isolation_Site_SD`; it is intentional context, not automatically a metadata descriptor.
- ENVO-style identifiers such as `ENVO_00005801` are unresolved ontology codes. They require ontology resolution or manual review, not automatic descriptor suppression.
- Food and commodity context is the largest batch (98,965 rows) and remains deliberately late because animal words alone are insufficient evidence of food source.
- No production routing rules were changed in Batch 0. The canonical QA baseline remains hard leakage 0 and broad leakage 0.
