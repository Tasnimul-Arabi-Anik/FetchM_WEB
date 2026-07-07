# Hidden Archaea Release-Candidate Gate

- Status: `pass`
- Release candidate ready for admin review: true
- Safe to publicly release: false
- Manual public release required: true
- Snapshot: `20260706T163621Z_genbank_archaea_root`
- Root assemblies: 44,183
- Standardized assemblies: 44,183
- Missing standardized assemblies: 0
- Hard failures: 0
- Public enabled: false
- Release locked: true

## Admin Validation

- Search query: `Methano`
- Top result: `Methanobrevibacter` (genus), 968 genomes
- Taxon report rows: 968
- Metadata CSV rows: 968
- Metadata CSV: `archaea_genus_Methanobrevibacter_metadata.csv`

## Checks

- `profile_is_archaea_hidden_v1`: pass - profile=archaea_hidden_v1
- `public_release_disabled`: pass - public_enabled=False
- `release_locked`: pass - release_locked=True
- `prokaryote_record_model`: pass - primary_record_model=prokaryote_assembly
- `hidden_domain_qa_passes`: pass - status=pass; hard_failures=0
- `root_nonempty`: pass - root_unique_assemblies=44,183
- `standardization_complete`: pass - standardized=44,183; root=44,183; missing=0
- `admin_taxon_search_returns_results`: pass - query='Methano'; results=20
- `admin_taxon_report_available`: pass - rank=genus; name=Methanobrevibacter; row_count=968
- `admin_taxon_report_matches_search_count`: pass - search_count=968; report_rows=968
- `admin_taxon_report_hidden_locked`: pass - public_enabled=False; release_locked=True
- `admin_metadata_csv_available`: pass - row_count=968
- `admin_metadata_csv_matches_report`: pass - csv_rows=968; report_rows=968

## Hard Failures

- None

## Decision

Hidden Archaea release-candidate prechecks pass; public release remains locked. Public release remains locked; this gate does not enable public routes, deploy Archaea, or regenerate Global Insights.
