# Bacterial Automatic Updates

The existing root-inventory worker runs the bacterial pipeline every 14 days
at 18:00 UTC (00:00 the following day in Bangladesh). Settings persist in
`app_settings` in the shared application database:

| Setting | Value |
| --- | --- |
| `canonical_pipeline_schedule_enabled` | `1` |
| `canonical_pipeline_interval_days` | `14` |
| `canonical_pipeline_schedule_hour_utc` | `18` |
| `canonical_pipeline_auto_publish_verified` | `1` |

Each run scans the current GenBank bacterial accession inventory with durable
page checkpoints. Existing standardized accession records are reused; full
metadata reports are fetched and standardized only for missing accessions.
Newly standardized rows receive the existing source/environment completion
and the five approved Semantic Closure Bucket A rules through the shared
transformers. Held environment rules are not applied.
The legacy discovery/catalog pipeline stays paused to avoid duplicate work.
No sequence files are downloaded by this pipeline.

An active canonical task prevents another scheduled run from starting. After
metadata coverage, taxonomy lineage and reconciliation checks pass, the new
snapshot is activated and Global Insights generation is queued. A blocked
release gate retains the active snapshot. Previous snapshot identifiers and
inventory memberships remain available for rollback.

The cadence is measured from the most recently requested inventory run, so
a manual catch-up run resets the next due date. A failed run needs operator
review; checkpointed inventory timeouts are retried automatically. Inspect
the canonical task history in Admin to confirm completion, rather than
assuming a healthy worker means the public data have advanced.

This efficient mode adds new accessions and accession versions. It does not
refetch every existing accession's BioSample metadata on each run; edits to
an unchanged accession require a separately requested metadata refresh.

Worker SQLite transaction contexts close their connections on exit, including
rollback, so the continuous scheduler does not accumulate database handles.
