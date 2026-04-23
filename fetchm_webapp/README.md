# fetchM Web App

This folder contains a separate Flask web application for submitting `fetchm` jobs online without changing the existing CLI package in [`../fetchm`](../fetchm).

## What it does

- supports either a managed taxon catalog or a direct TSV upload for `fetchm metadata` and `fetchm run`
- still accepts uploaded `ncbi_clean.csv` inputs for `fetchm seq`
- stores each submission in its own job directory under `fetchm_webapp/data/jobs/`
- stores managed taxon TSV files under `fetchm_webapp/data/species/`
- stores job metadata and queue state in SQLite
- runs the existing CLI from a dedicated background worker process
- shows job status, command, logs, and downloadable output files in the browser

## Layout

```text
fetchm_webapp/
  app.py
  requirements.txt
  templates/
  static/
  data/jobs/
```

## Run locally

1. Create a Python environment with the original `fetchm` dependencies plus the web app dependency.
2. Install the web dependency:

```bash
pip install -r fetchm/requirements.txt
pip install -r fetchm_webapp/requirements.txt
```

3. From this repository root, start the web app:

```bash
python fetchm_webapp/app.py
```

4. Open `http://127.0.0.1:8000`.

Set a real secret before starting:

```bash
export FETCHM_WEBAPP_SECRET=replace-this
export FETCHM_WEBAPP_ADMIN_USERS=your-admin-username
python fetchm_webapp/app.py
```

Then open `http://127.0.0.1:8000/register` to create the first user account.

To enable password reset emails, also set SMTP values:

```bash
export FETCHM_WEBAPP_MAIL_SERVER=smtp.example.com
export FETCHM_WEBAPP_MAIL_PORT=587
export FETCHM_WEBAPP_MAIL_USERNAME=mailer@example.com
export FETCHM_WEBAPP_MAIL_PASSWORD=replace-this
export FETCHM_WEBAPP_MAIL_USE_TLS=1
export FETCHM_WEBAPP_MAIL_FROM=mailer@example.com
export FETCHM_WEBAPP_SPECIES_REFRESH_HOURS=24
export FETCHM_WEBAPP_DISCOVERY_REFRESH_HOURS=168
export FETCHM_WEBAPP_DISCOVERY_LIMIT_PER_SCOPE=100
export FETCHM_WEBAPP_DISCOVERY_SCOPES=1279|species|Staphylococcus species,1279|genus|Staphylococcus genera
export FETCHM_WEBAPP_DEFAULT_ASSEMBLY_SOURCE=genbank
export FETCHM_WEBAPP_TAXON_RECENT_HOURS=168
export FETCHM_WEBAPP_TAXON_VERY_OLD_HOURS=720
export NCBI_API_KEY=your-ncbi-api-key
```

You can also use a local env file instead of exporting variables manually:

```bash
cp fetchm_webapp/.env.example fetchm_webapp/.env
```

Then edit `fetchm_webapp/.env` and start the app normally:

```bash
python fetchm_webapp/app.py
```

For your lab Gmail account, use:

```env
FETCHM_WEBAPP_MAIL_SERVER=smtp.gmail.com
FETCHM_WEBAPP_MAIL_PORT=587
FETCHM_WEBAPP_MAIL_USERNAME=dul68426@gmail.com
FETCHM_WEBAPP_MAIL_PASSWORD=your-gmail-app-password
FETCHM_WEBAPP_MAIL_USE_TLS=1
FETCHM_WEBAPP_MAIL_USE_SSL=0
FETCHM_WEBAPP_MAIL_FROM=dul68426@gmail.com
FETCHM_WEBAPP_MAIL_NOTIFY_JOB_SUBMITTED=1
FETCHM_WEBAPP_MAIL_NOTIFY_JOB_FINISHED=1
FETCHM_WEBAPP_MAIL_NOTIFY_JOB_FAILED=1
```

Important: Gmail requires an App Password for SMTP. Do not use the normal account password.

## Run with Docker

From the repository root:

```bash
docker compose -f fetchm_webapp/docker-compose.yml up --build
```

For production on `fetchm.dulab206.xyz`, Caddy now sits in front of the app and terminates HTTPS automatically.

Before starting:

1. Point the DNS record for `fetchm.dulab206.xyz` through the configured Cloudflare Tunnel.
2. If you have IPv6, point the `AAAA` record to the server's public IPv6 address.
3. Open inbound ports `80` and `443` on the server firewall.
4. Make sure `fetchm_webapp/.env` exists and contains your real secret and SMTP values.

Then start:

```bash
docker compose -f fetchm_webapp/docker-compose.yml up --build -d
```

After DNS is live, Caddy will obtain and renew the HTTPS certificate automatically.

Job uploads, logs, and outputs are persisted in `fetchm_webapp/data/`.

## Production notes

- Set `FETCHM_WEBAPP_SECRET` to a real secret before exposing this publicly.
- Set `FETCHM_WEBAPP_ADMIN_USERS` to a comma-separated list of usernames allowed to access `/admin`.
- Users are stored in `fetchm_webapp/data/fetchm_webapp.db`.
- Jobs are stored in the same SQLite database, while uploads, logs, and outputs remain on disk under `fetchm_webapp/data/jobs/`.
- Managed taxon TSV files are stored under `fetchm_webapp/data/species/`.
- Password reset emails are sent over SMTP when the mail env vars are configured.
- Job submission, completion, and failure emails use the same SMTP settings and can be toggled with `FETCHM_WEBAPP_MAIL_NOTIFY_JOB_SUBMITTED`, `FETCHM_WEBAPP_MAIL_NOTIFY_JOB_FINISHED`, and `FETCHM_WEBAPP_MAIL_NOTIFY_JOB_FAILED`.
- If SMTP is not configured, the forgot-password page will warn instead of sending mail.
- Caddy is configured for `fetchm.dulab206.org` and expects working DNS plus open ports `80` and `443`.
- Docker Compose now runs one hybrid worker plus two sync/job workers. The admin dashboard can pause discovery entirely or let the hybrid worker resume it on a daily or weekly cadence.
- The worker also refreshes any managed taxa whose sync is requested or older than `FETCHM_WEBAPP_SPECIES_REFRESH_HOURS`.
- The worker can also discover taxa automatically from configured NCBI taxonomy scopes using `FETCHM_WEBAPP_DISCOVERY_SCOPES`.
- Species and discovery scopes can target `all`, `genbank`, or `refseq` assemblies. `FETCHM_WEBAPP_DEFAULT_ASSEMBLY_SOURCE` sets the env-driven default for auto-configured scopes.
- Discovery scopes now use the format `<scope>|<rank>|<label>`, for example `2|species|Bacteria species` or `2|genus|Bacteria genera`.
- Large bacterial-wide species discovery is staged internally by genus so the worker does not rely on a single monolithic `taxon 2 --rank species --limit all` call.
- If `NCBI_API_KEY` is set, the `datasets` CLI can use NCBI's higher authenticated rate limit for discovery and TSV refresh traffic.
- Admins can add discovery scopes, add taxa directly, refresh discovery/taxon syncs, view all users, inspect all jobs, stop queued/running jobs, and clean old finished job data from the `/admin` page.

## Important notes

- `metadata` and `run` can use either a synced species or genus from the managed catalog or a direct `.tsv` upload.
- Managed taxon submissions can optionally request `Refresh metadata before running`; those jobs stay queued until the selected taxon's TSV refresh completes.
- Cached TSVs older than `FETCHM_WEBAPP_TAXON_VERY_OLD_HOURS` trigger a submit-time warning when the user chooses not to refresh first.
- `seq` requires a `.csv` upload, typically `ncbi_clean.csv`.
- Managed taxon TSV files are built from `datasets summary genome taxon ... --as-json-lines`, then written into the tab-separated column layout that `fetchm metadata` already expects.
- If an assembly source is selected, the sync uses `--assembly-source GenBank` or `--assembly-source RefSeq` so the catalog count matches that NCBI view.
- Automatic discovery uses `datasets summary taxonomy taxon <scope> --children --rank <species|genus>`, filtered to taxa with at least one assembly, then queues any new taxa for TSV sync.
- The web app sets `PYTHONPATH` so it can execute `python -m fetchm.cli` against the existing package under `../fetchm`.
- Uploaded files, logs, and outputs are stored on disk.
- Jobs are visible only to the account that created them.
