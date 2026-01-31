#!/bin/bash

# Install Airflow
pip install apache-airflow

# Initialize Airflow
airflow db init

# Copy DAG file
cp dags/themeparks_dag.py ~/airflow/dags/

# Add your project to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_JWT_ISSUER="airflow"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS='admin:admin:Admin,andy:andy:Admin'

# Start Airflow
# airflow webserver -p 8080 &
# airflow standalone # ?
airflow api-server -p 8080 &
airflow scheduler &

Simple auth manager | Password for user 'admin': gm4Fs9kKsqmYqUNc

gm4Fs9kKsqmYqUNc