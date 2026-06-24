# Phase 2A Hardened Dry-Run Validation Notes

## Commands that passed

```bash
cd fetchm_webapp
python -m unittest tests.test_semantic_phase2a_dry_run tests.test_host_clinical_site_semantics_audit
python -m py_compile app.py tools/semantic_phase2a_dry_run.py tests/test_semantic_phase2a_dry_run.py
git diff --check
```

Result: 30 focused semantic/audit tests passed. Compile and whitespace checks passed.

## Full canonical dry-run

The hardened dry-run scanned `3,131,699` canonical bacterial rows from snapshot `20260602T140414Z_genbank_bacteria_root` and wrote artifacts to this directory.

Dry-run gate result: pass.

Key hard-gate metrics:

- legacy compatibility field changes: 0
- protected/raw field changes: 0
- destination overwrite conflicts: 0
- required evidence failures: 0
- unknown-condition failures: 0
- rows outside reviewed allowlist affected: 0
- patient environment-only context additions: 0
- plant context without plant evidence: 0
- catheter production changes: 0
- removals without provenance: 0

No canonical write, Global Insights regeneration, deployment, host taxonomy change, geography/date change, or legacy source/sample compatibility-field remapping was performed.

## Full regression attempt

A production-equivalent disposable Docker test attempt was run with the built FetchM image, `/opt/conda/bin/python`, an isolated temporary data mount, the service Docker network, and the existing taxonkit mount:

```bash
docker run --rm \
  --network fetchm_webapp_default \
  --env-file /home/ai-pc/Work/FetchM_WEB/fetchm_webapp/.env \
  -e PYTHONDONTWRITEBYTECODE=1 \
  -v /home/ai-pc/Work/FetchM_WEB/fetchm_webapp:/app/fetchm_webapp \
  -v /tmp/fetchm_phase2a_test_data:/home/ai-pc/Work/dulab206/fetchm_webapp/data \
  -v /home/ai-pc/.taxonkit:/home/fetchm/.taxonkit:ro \
  -w /app/fetchm_webapp \
  fetchm_webapp_fetchm-web \
  sh -lc '/opt/conda/bin/python -m py_compile app.py tools/semantic_phase2a_dry_run.py tests/test_semantic_phase2a_dry_run.py && /opt/conda/bin/python -m unittest tests.test_semantic_phase2a_dry_run tests.test_host_clinical_site_semantics_audit tests.test_metadata_standardization_regressions'
```

Result: not passing in the disposable isolated environment. The remaining failures were not Phase 2A semantic-rule failures:

- two public-route tests returned 500 because the isolated temporary SQLite data mount had no `species` table;
- one template asset test expected the older cache-bust string `20260512-saas-theme`, while the current template uses `20260618-ui-foundation`.

The full regression suite should be rerun in the repository's normal initialized app-test environment before any canonical write is authorized.
