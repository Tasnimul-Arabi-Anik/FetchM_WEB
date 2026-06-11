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
