#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DATA_DIR="${FETCHM_WEBAPP_DATA_DIR:-${ROOT_DIR}/data}"
WORKFLOW_DIR="${FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW_HOST:-${DATA_DIR}/external_tools/workflows/PanResistome}"
ENV_PREFIX="${FETCHM_WEBAPP_QUALITY_GTDBTK_ENV_PREFIX:-${DATA_DIR}/external_tools/conda/envs/gtdbtk_env}"
DB_DIR="${FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH_HOST:-${DATA_DIR}/external_tools/databases/gtdbtk}"
DB_URL="${FETCHM_WEBAPP_QUALITY_GTDBTK_DB_URL:-https://data.ace.uq.edu.au/public/gtdb/data/releases/latest/auxillary_files/gtdbtk_package/full_package/gtdbtk_data.tar.gz}"
DB_ARCHIVE="${DB_DIR}/gtdbtk_r232_data.tar.gz"

if ! command -v conda >/dev/null 2>&1; then
  echo "ERROR: conda is required for GTDB-Tk setup." >&2
  exit 1
fi

mkdir -p "${DB_DIR}" "$(dirname "${ENV_PREFIX}")"

if [ ! -x "${ENV_PREFIX}/bin/gtdbtk" ]; then
  echo "[INFO] Creating GTDB-Tk 2.7.1 environment at ${ENV_PREFIX}"
  conda env create -p "${ENV_PREFIX}" -f "${WORKFLOW_DIR}/envs/gtdbtk.yaml"
fi

if [ -d "${DB_DIR}/markers" ] && [ -d "${DB_DIR}/taxonomy" ] && [ -d "${DB_DIR}/pplacer" ]; then
  echo "[INFO] GTDB-Tk database already appears complete at ${DB_DIR}"
else
  echo "[INFO] Downloading GTDB-Tk reference data to ${DB_ARCHIVE}"
  if command -v aria2c >/dev/null 2>&1; then
    aria2c -c -x 16 -s 16 -k 4M --file-allocation=none -d "${DB_DIR}" -o "$(basename "${DB_ARCHIVE}")" "${DB_URL}"
  else
    wget -c "${DB_URL}" -O "${DB_ARCHIVE}"
  fi

  echo "[INFO] Extracting GTDB-Tk reference data into ${DB_DIR}"
  tar -xzf "${DB_ARCHIVE}" -C "${DB_DIR}" --strip 1
  rm -f "${DB_ARCHIVE}" "${DB_ARCHIVE}.aria2"
fi

conda env config vars set -p "${ENV_PREFIX}" GTDBTK_DATA_PATH="${DB_DIR}" >/dev/null

echo "[INFO] GTDB-Tk setup complete."
echo "[INFO] Set this in Docker Compose for web/job-worker services:"
echo "FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH=/app/fetchm_webapp/data/external_tools/databases/gtdbtk"
