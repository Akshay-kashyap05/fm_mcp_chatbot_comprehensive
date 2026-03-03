#!/usr/bin/env bash
# Airflow environment for Sanjaya Analytics reports
#
# Usage on the server (from your shell after activating the Airflow venv):
#   source airflow_env.sh
#
# To make it permanent, you can append these exports to ~/.bashrc:
#   cat airflow_env.sh >> ~/.bashrc
#   source ~/.bashrc

# Where Airflow stores its DB, logs, config, etc.
export AIRFLOW_HOME="$HOME/airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=false

# Tell Airflow where your DAGs live.
# IMPORTANT: Update this path if your project root is different on the server.
export AIRFLOW__CORE__DAGS_FOLDER="/home/ati/fm_mcp_chatbot_comprehensive/airflow/dags"

echo "AIRFLOW_HOME=$AIRFLOW_HOME"
echo "AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW__CORE__DAGS_FOLDER"

