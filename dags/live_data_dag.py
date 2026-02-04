"""
Live Data Frequent DAG

Extracts, validates, transforms, and loads real-time park data frequently.
Includes wait times, operating hours, and ride status.
Runs every 5 minutes from 8 AM to 11 PM.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator

from config import (
    default_args,
    get_common_pod_kwargs,
    get_minio_path,
    get_kafka_topic,
    PARK_FILTER,
    SMALL_RESOURCES,
    MEDIUM_RESOURCES,
)

# Custom args for frequent DAG (fewer retries, shorter delays)
live_data_args = {
    **default_args,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# DAG definition
with DAG(
    dag_id="live_data_frequent_k8s",
    description="Frequent live data extraction using KubernetesPodOperator",
    default_args=live_data_args,
    schedule="*/5 8-23 * * *",  # Every 5 min, 8 AM - 11 PM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "live", "kubernetes", "frequent"],
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # Extract live data from API (use timestamp instead of date)
    extract = KubernetesPodOperator(
        task_id="extract_live",
        name="extract-live",
        arguments=[
            "extract",
            "live",
            "--output", get_minio_path("live", "raw", "{{ ts_nodash }}"),
            "--park-filter", PARK_FILTER,
            "--log-level", "INFO",
        ],
        resources=MEDIUM_RESOURCES,  # More resources for parallel API calls
        **get_common_pod_kwargs()
    )
    
    # Validate extracted data
    validate_extract = KubernetesPodOperator(
        task_id="validate_extract",
        name="validate-extract-live",
        arguments=[
            "validate",
            "live",
            "--input", get_minio_path("live", "raw", "{{ ts_nodash }}"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    # Transform data (add metadata)
    transform = KubernetesPodOperator(
        task_id="transform_live",
        name="transform-live",
        arguments=[
            "transform",
            "live",
            "--input", get_minio_path("live", "raw", "{{ ts_nodash }}"),
            "--output", get_minio_path("live", "transformed", "{{ ts_nodash }}"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    # Load to Kafka (skip second validation for speed)
    load = KubernetesPodOperator(
        task_id="load_live",
        name="load-live",
        arguments=[
            "load",
            "kafka",
            "--input", get_minio_path("live", "transformed", "{{ ts_nodash }}"),
            "--topic", get_kafka_topic("live"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    end = EmptyOperator(task_id="end")
    
    # Define pipeline flow (skip second validation for speed)
    start >> extract >> validate_extract >> transform >> load >> end
