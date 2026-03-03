#!/usr/bin/env bash
set -euo pipefail

# Initialize Airflow DB and create an admin user.
# Assumes:
#   - You already created and activated a Python venv with Airflow installed.
#   - AIRFLOW_HOME and AIRFLOW__CORE__DAGS_FOLDER are set (e.g. via airflow_env.sh).
#
# Typical usage on the server:
#   source venv_airflow/bin/activate
#   cd /home/ubuntu/fm_mcp_chatbot_comprehensive
#   source airflow/airflow_env.sh
#   bash airflow/airflow_init.sh

echo "Using AIRFLOW_HOME=${AIRFLOW_HOME:-'(not set)'}"
echo "Using DAGS_FOLDER=${AIRFLOW__CORE__DAGS_FOLDER:-'(not set)'}"

if [ -z "${AIRFLOW_HOME:-}" ]; then
  echo "ERROR: AIRFLOW_HOME is not set. Did you source airflow_env.sh?" >&2
  exit 1
fi

echo "Initializing Airflow DB at $AIRFLOW_HOME ..."
airflow db init

echo "Creating Airflow admin user (change credentials if needed) ..."
airflow users create \
  --username admin \
  --role Admin \
  --email admin@example.com \
  --firstname Admin \
  --lastname User \
  --password admin

echo
echo "Airflow initialized."
echo "You can now start the scheduler and webserver with:"
echo "  airflow scheduler &"
echo "  airflow webserver --port 8080"

