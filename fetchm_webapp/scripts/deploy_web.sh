#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$APP_DIR/data"
STATUS_FILE="$DATA_DIR/deployment_status.json"
SERVICE="${FETCHM_WEBAPP_DEPLOY_SERVICE:-fetchm-web}"
HEALTH_URL="${FETCHM_WEBAPP_DEPLOY_HEALTH_URL:-http://127.0.0.1/healthz}"
HEALTH_HOST="${FETCHM_WEBAPP_DEPLOY_HEALTH_HOST:-fetchm.dulab206.xyz}"
COMMIT="${FETCHM_WEBAPP_GIT_COMMIT:-$(git -C "$APP_DIR" rev-parse HEAD)}"
BRANCH="$(git -C "$APP_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
COMPOSE_VERSION="$(docker compose version --short 2>/dev/null || docker compose version 2>/dev/null || echo unavailable)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

mkdir -p "$DATA_DIR"

cd "$APP_DIR"
export FETCHM_WEBAPP_GIT_COMMIT="$COMMIT"
# env_file values are applied at container runtime and otherwise mask the
# build-time commit stamp exposed through /healthz. Update only this key.
if grep -q '^FETCHM_WEBAPP_GIT_COMMIT=' .env; then
  sed -i "s/^FETCHM_WEBAPP_GIT_COMMIT=.*/FETCHM_WEBAPP_GIT_COMMIT=$COMMIT/" .env
else
  printf '\nFETCHM_WEBAPP_GIT_COMMIT=%s\n' "$COMMIT" >> .env
fi

docker compose -f docker-compose.yml config --quiet
docker compose -f docker-compose.yml build "$SERVICE"
docker compose -f docker-compose.yml up -d --force-recreate --no-deps "$SERVICE"

for attempt in $(seq 1 30); do
  if curl -fs -H "Host: $HEALTH_HOST" "$HEALTH_URL" >/tmp/fetchm_webapp_healthz.json; then
    break
  fi
  sleep 2
  if [ "$attempt" = "30" ]; then
    echo "health check failed after deployment" >&2
    exit 1
  fi
done

HEALTH_JSON="$(cat /tmp/fetchm_webapp_healthz.json)"
DEPLOYED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
IMAGE_ID="$(docker compose -f docker-compose.yml images -q "$SERVICE" 2>/dev/null | head -n 1 || true)"
CONTAINER_ID="$(docker compose -f docker-compose.yml ps -q "$SERVICE" 2>/dev/null || true)"

python - "$STATUS_FILE" <<PY
import json, sys
payload = {
    "status": "deployed",
    "service": "$SERVICE",
    "commit": "$COMMIT",
    "branch": "$BRANCH",
    "compose_version": "$COMPOSE_VERSION",
    "started_at": "$STARTED_AT",
    "deployed_at": "$DEPLOYED_AT",
    "health_url": "$HEALTH_URL",
    "health_host": "$HEALTH_HOST",
    "health_response": json.loads('''$HEALTH_JSON'''),
    "image_id": "$IMAGE_ID",
    "container_id": "$CONTAINER_ID",
}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

echo "Deployed $SERVICE at $COMMIT"
cat "$STATUS_FILE"
