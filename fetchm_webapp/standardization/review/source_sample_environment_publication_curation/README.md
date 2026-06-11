# Source, Sample, and Environment Publication Curation

This workspace processes canonical source/sample/environment review signals in controlled batches. Review signals are triage candidates, not errors, and are not expected to reach zero.

Current baseline: `20260611/`

Regenerate Batch 0 from the committed canonical QA checkpoint:

```bash
python tools/prepare_source_sample_environment_publication_curation.py \
  --qa-dir standardization/review/source_sample_environment_qa/20260611_exact_cleanup \
  --output-dir standardization/review/source_sample_environment_publication_curation/20260611
```

The optional `--enrichment-json` input adds canonical mode broad values and routing methods. It is generated from the production canonical store and is intentionally not committed because the resulting reviewed candidate CSV contains the required provenance fields.

Batch guardrails:

- Inspect contributing raw values before changing a rule.
- Preserve intentional cross-field context.
- Keep hard leakage and broad leakage at zero.
- Commit one semantic batch at a time with tests and before/after QA.
- Stop when high-impact signals are reviewed and remaining signals are documented as intentional, ambiguous, or low priority.
