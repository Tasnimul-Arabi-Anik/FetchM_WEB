# Post-Apply Host Taxonomy QA

- `tools/audit_host_synonym_taxonomy.py --report-dir /tmp/fetchm_microbial_host_audit_verify`: passed with `non_allowlisted_microbial_before_apply=0`.
- Runtime Docker QA passed for eukaryotic algae, context-only labels, demoted microbial names, and reviewed exact-host mappings.

Validated examples:

| Raw host | Expected outcome |
| --- | --- |
| Sargassum hemiphyllum | Host_SD=Sargassum hemiphyllum, TaxID=127544, Eukaryota |
| red marine alga | Host_SD=Rhodophyta, TaxID=2763, Eukaryota |
| algae / algea | Host_Context_SD=algae, no Host_SD |
| fish / Pisces | Host_Context_SD=fish, no Host_SD |
| Zooplatnkon | Host_Context_SD=zooplankton, no Host_SD |
| green alga | Host_Context_SD=green algae, no Host_SD |
| Microcystis aeruginosa | non_host_source, no Host_SD |
| Nostoc sp. | non_host_source, no Host_SD |
| Prochlorococcus | non_host_source, no Host_SD |
| Klebsiella pneumoniae | non_host_source, no Host_SD |
| Streptococcus agalactiae | non_host_source, no Host_SD |
| waterlettuce | Host_SD=Pistia stratiotes, TaxID=4477 |
| shore bird | Host_SD=Charadriiformes, TaxID=8906 |
| Cuttloefish | Host_SD=Sepiidae, TaxID=6608 |
