# Hidden Archaea Pipeline

This pipeline prepares Archaea in the background without changing the public bacterial release. It mirrors the bacterial canonical workflow only where the infrastructure is safely reusable. Biological rules, QA gates, readiness thresholds, and release approval remain domain-specific.

## Current Scope

Implemented in this milestone:

- Admin selector can choose `Archaea` as a hidden preparation lane.
- PostgreSQL storage uses separate `domain_*` tables and rejects `bacteria` on the hidden-domain path.
- Archaea root inventory can be built from NCBI Datasets REST API using TaxID `2157`.
- Hidden metadata can be fetched and standardized into `domain_assembly_standardization`.
- Hidden inventory and metadata coverage status are visible in Admin only.
- Admin-only Archaea genus/species search and compact taxon reports are available at `/admin/archaea`.
- Admin users can queue hidden inventory and metadata-fetch tasks for root-inventory workers.
- Admin users can pause or enable recurring hidden Archaea refreshes. Scheduled runs queue inventory with metadata fetch enabled.
- The admin page reports a locked release gate so readiness is visible without enabling public promotion.
- `tools/qa_hidden_domain_pipeline.py` verifies hidden-domain snapshot, metadata coverage, payload tags, task completion, and release-lock invariants.
- Public search, sequence/QC entrypoints, Global Insights, and release activation remain locked.

Not implemented yet:

- Public-release archaeal biological rule pack and manuscript/readiness thresholds.
- Manual public promotion.

## Inventory Command

Validation run:

```bash
python tools/build_domain_root_inventory.py --domain archaea --max-pages 1
```

Full hidden inventory run:

```bash
python tools/build_domain_root_inventory.py --domain archaea
```

Fetch missing hidden metadata after an inventory snapshot exists:

```bash
python tools/fetch_domain_missing_metadata.py \
  --domain archaea \
  --snapshot-id YYYYMMDDTHHMMSSZ_genbank_archaea_root
```

The same steps can also be queued from `/admin/archaea`; root-inventory workers claim those hidden tasks after canonical bacterial work.

## Recurring Hidden Refresh

The recurring schedule is managed from `/admin/archaea`. It stores three settings:

```text
archaea_pipeline_schedule_enabled
archaea_pipeline_interval_days
archaea_pipeline_schedule_hour_utc
```

When enabled, root-inventory workers check the hidden schedule once per minute, queue a hidden Archaea inventory task only if no hidden task is active, and set `continue_after=true` so metadata fetch follows the inventory. Scheduled runs do not unlock public routes or publish Global Insights.


Run a small metadata validation batch:

```bash
python tools/fetch_domain_missing_metadata.py \
  --domain archaea \
  --snapshot-id YYYYMMDDTHHMMSSZ_genbank_archaea_root \
  --max-batches 1
```

The default snapshot ID is formatted as:

```text
YYYYMMDDTHHMMSSZ_genbank_archaea_root
```

## Admin Search

Admin users can search hidden Archaea metadata at:

```text
/admin/archaea?q=Methanocaldococcus
```

Taxon reports currently support genus and species labels derived from organism names in hidden standardized metadata. Higher-rank archaeal browsing remains blocked until taxonomy lineage materialization is implemented for the hidden domain.

## Release Lock

Every hidden-domain snapshot is created with:

```text
visibility = admin_hidden
release_locked = true
```

Do not expose Archaea publicly until a separate reviewed release task explicitly unlocks it after metadata coverage, QA, and manuscript-readiness checks pass.

## QA Command

Run the hidden-domain QA gate against the latest or a specific hidden Archaea snapshot:

```bash
python tools/qa_hidden_domain_pipeline.py \
  --domain archaea \
  --snapshot-id YYYYMMDDTHHMMSSZ_genbank_archaea_root \
  --output-dir data/standardization/review/archaea_hidden_pipeline/YYYYMMDD_qa \
  --fail-on-hard-errors
```

The gate checks completed inventory and metadata tasks, full metadata coverage, `admin_hidden` visibility, `release_locked=true`, `archaea_hidden_v1` payload tags, `locked_admin_hidden` payload status, and hidden table domain isolation.


## Safety Rules

- Do not run bacterial canonical inventory tools with TaxID `2157`.
- Do not write Archaea rows into `bacterial_inventory_snapshot`, `bacterial_inventory_membership`, or `assembly_standardization`.
- Do not reuse bacterial biological thresholds as archaeal acceptance criteria without review.
- Treat `archaea_hidden_v1` as a background profile until archaeal QA is reviewed.
- Preserve bacterial public behavior while Archaea remains hidden.
