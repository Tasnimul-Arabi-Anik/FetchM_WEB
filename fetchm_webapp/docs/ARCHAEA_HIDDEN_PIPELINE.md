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
- Public search, sequence/QC entrypoints, Global Insights, and release activation remain locked.

Not implemented yet:

- Archaeal rule pack and QA gate.
- Recurring schedule automation for hidden Archaea runs.
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

## Safety Rules

- Do not run bacterial canonical inventory tools with TaxID `2157`.
- Do not write Archaea rows into `bacterial_inventory_snapshot`, `bacterial_inventory_membership`, or `assembly_standardization`.
- Do not reuse bacterial biological thresholds as archaeal acceptance criteria without review.
- Treat `archaea_hidden_v1` as a background profile until archaeal QA is reviewed.
- Preserve bacterial public behavior while Archaea remains hidden.
