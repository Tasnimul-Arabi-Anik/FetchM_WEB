# Metadata Standardization

## Geography and collection-date production checkpoint

The current checkpoint audits the activated canonical bacterial assembly
dataset directly. It does not use legacy genus/species staging files.

The checkpoint records:

- QA commit and timestamp
- canonical dataset row count
- Country, Continent, Subcontinent, and collection-year coverage
- Country/Continent and Country/Subcontinent mismatch counts
- invalid and future collection-year counts
- checksums for the country lookup and collection-date parser
- all generated review and exception artifacts

Rule and lookup sources:

- `lib/fetchm_runtime/metadata.py` (`COUNTRY_MAPPING` and country aliases)
- `standardization/geography_reviewed_rules.csv`
- `standardization/collection_date_reviewed_rules.csv`
- `app.py` collection-date recovery and false-positive controls

Run the production gate with:

```bash
python tools/qa_geography_collection_date.py --fail-on-hard-errors
```

The canonical field `Collection Date` stores the normalized collection year.
Audit outputs label it `Collection_Year` for clarity without changing the
production schema.

Latest canonical checkpoint:

- Snapshot: `20260602T140414Z_genbank_bacteria_root`
- Rows audited: 3,131,699
- Country coverage: 2,739,312 (87.47%)
- Continent coverage: 2,739,312 (87.47%)
- Subcontinent coverage: 2,739,312 (87.47%)
- Collection-year coverage: 2,581,763 (82.44%)
- Country/Continent mismatches: 0
- Country/Subcontinent mismatches: 0
- Non-country values in `Country`: 0
- Invalid, future, or impossible collection years: 0
- QA commit: `570aac7`
- Versioned artifacts: `standardization/review/geography_collection_date_qa/20260611/`

Country and geography summarize representation in public repository metadata.
They must not be interpreted as biological prevalence.

## Source, sample, and environment production checkpoint

Source, sample, environment, anatomical site, disease, and health-state
standardization runs on the canonical bacterial dataset. The production QA
checkpoint audits exact and broad routing without automatically changing
reviewed rules.

Deterministic rule sources:

- `standardization/controlled_categories.csv`
- `standardization/approved_broad_categories.csv`
- committed routing code in `app.py`

Run the production gate with:

```bash
python tools/qa_source_sample_environment_standardization.py --fail-on-hard-errors
```

Exact-source classifications are review signals. A concept may legitimately be
retained as source context while also producing `Sample_Type_SD`,
`Environment_Medium_SD`, `Isolation_Site_SD`, `Host_Disease_SD`, or
`Host_Health_State_SD`. Hard failures are limited to missing provenance,
conflicting rules, unapproved broad vocabulary, empty canonical scope, and
raw-present standardization below the production threshold.

Latest canonical checkpoint:

- Snapshot: `20260602T140414Z_genbank_bacteria_root`
- Rows audited: 3,131,699
- Raw isolation-source coverage: 2,248,471 (71.80%)
- `Isolation_Source_SD` coverage: 1,984,892 (63.38%)
- `Isolation_Source_SD_Broad` coverage: 1,831,325 (58.48%)
- Raw-present isolation-source standardization: 87.24%
- Raw-only unresolved isolation-source rows: 286,950
- Non-approved broad source rows: 0
- Controlled-category duplicate/conflict keys: 0/0
