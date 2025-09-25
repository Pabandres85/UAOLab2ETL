#!/usr/bin/env bash
set -euo pipefail

echo ">>> Installing Python deps..."
pip install --no-cache-dir -r /opt/airflow/requirements.txt

echo ">>> Initializing Airflow DB..."
airflow db init

echo ">>> Creating admin user (idempotent)..."
airflow users create \
  --username airflow \
  --password airflow \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

echo ">>> Listing /opt/airflow/state:"
ls -l /opt/airflow/state || true

echo ">>> INIT OK"
