# Live Smoke Test

Status: pass.

- Recreated containers: fetchm-web and fetchm-insights-worker only.
- Container health: both healthy.
- /healthz commit: 74de51ebb9aaee37736882e20fffa0c4794f76be.
- Global Insights snapshot: 20260707T103702Z_global_insights.
- Global Insights row count: 3,131,699.
- Global Insights page: shows snapshot 20260707T103702Z_global_insights and app commit 74de51ebb9aaee37736882e20fffa0c4794f76be.
- Manifest download: HTTP 200 for /global-insights/snapshots/20260707T103702Z_global_insights/download/manifest.json.
- Known canonical examples: Host_Health_State_SD=healthy/control remaining 0; Host_Health_State_SD=diseased/patient remaining 0; Isolation_Site_SD=catheter remaining 0; Host_Study_Group_SD=control rows 1,077.

Deployment note: docker-compose 1.29 hit a ContainerConfig recreate bug. Recovery removed only the stopped web/insights containers and started fresh containers from the clean semantic-closure image; PostgreSQL, standardization workers, raw metadata and legacy fields were not touched.
