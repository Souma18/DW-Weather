#!/usr/bin/env bash
set -e

echo "===> Running airflow db migrate..."
airflow db migrate

echo "===> Creating admin user (if not exists)..."
airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
  --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME}" \
  --lastname "${_AIRFLOW_WWW_USER_LASTNAME}" \
  --role Admin \
  --email "${_AIRFLOW_WWW_USER_EMAIL}" || true

echo "===> Starting webserver..."
exec airflow webserver