#!/usr/bin/env bash
airflow db migrate

airflow users create \
    -r Admin \
    -u "${AIRFLOW_USERNAME:-admin}" \
    -e "${AIRFLOW_EMAIL:-admin@admin.com}" \
    -f "${AIRFLOW_FIRSTNAME:-admin}" \
    -l "${AIRFLOW_LASTNAME:-admin}" \
    -p "${AIRFLOW_PASSWORD:-admin}"

airflow webserver