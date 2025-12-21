#!/usr/bin/env bash
set -euo pipefail

echo "[airflow-init] DB migrate..."
airflow db migrate

echo "[airflow-init] Creating admin user (idempotent)..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  || true

echo "[airflow-init] Done."
