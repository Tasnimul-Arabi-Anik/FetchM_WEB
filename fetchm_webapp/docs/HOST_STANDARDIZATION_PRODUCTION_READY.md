# Host Standardization Production Checkpoint

Host standardization is production-ready at commit `b92a591`. Changes after this
checkpoint should be limited to rules, provenance, monitoring, and tests unless a
regression test demonstrates a defect in the core standardization logic.

## Production Rule Files

- `standardization/host_synonyms.csv`
- `standardization/host_negative_rules.csv`
- `standardization/host_context_rules.csv`
- `standardization/host_microbial_allowlist.csv`

## Field Policy

- `Host_SD` contains only a resolved taxonomic host.
- `Host_TaxID` contains the NCBI TaxID for `Host_SD`.
- `Host_Context_SD` contains broad or context-only biological labels.
- Bacterial, archaeal, and viral values do not populate `Host_SD` unless they
  are explicitly reviewed in `host_microbial_allowlist.csv`.
- Eukaryotic algae, fungi, protists, plants, and animals remain valid hosts when
  NCBI lineage supports the assignment.

## Reference Examples

| Raw value | Production result |
| --- | --- |
| `waterlettuce` | `Host_SD=Pistia stratiotes` |
| `shorebird` | `Host_SD=Charadriiformes` |
| `Cuttloefish` | `Host_SD=Sepiidae` |
| `algae` | `Host_Context_SD=algae`; no `Host_SD` |
| `Sargassum hemiphyllum` | Valid eukaryotic `Host_SD` |
| `Microcystis`, `Nostoc`, `Klebsiella`, `Streptococcus` | No `Host_SD` unless allowlisted |

## Production QA

Run against the active canonical dataset:

```bash
python tools/qa_host_standardization.py --fail-on-leakage
```

The command validates rule taxonomy, reviewed precedence, context-only behavior,
eukaryotic algal preservation, and canonical database leakage. It exits nonzero
when a production invariant fails.
