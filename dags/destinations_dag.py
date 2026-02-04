"""
Destinations Daily DAG

Extracts, validates, transforms, and loads theme park destination data daily.
Runs at 6 AM daily.
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator

from config import (
    default_args,
    get_common_pod_kwargs,
    get_minio_path,
    get_kafka_topic,
    SMALL_RESOURCES,
)

# DAG definition
with DAG(
    dag_id="destinations_daily_k8s",
    description="Daily destinations extraction using KubernetesPodOperator",
    default_args=default_args,
    schedule="0 6 * * *",  # 6 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "destinations", "kubernetes", "daily"],
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # Extract destinations from API
    extract = KubernetesPodOperator(
        task_id="extract_destinations",
        name="extract-destinations",
        arguments=[
            "extract",
            "destinations",
            "--output", get_minio_path("destinations", "raw", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    # Validate extracted data
    validate_extract = KubernetesPodOperator(
        task_id="validate_extract",
        name="validate-extract-destinations",
        arguments=[
            "validate",
            "destinations",
            "--input", get_minio_path("destinations", "raw", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    # Transform data (add metadata)
    transform = KubernetesPodOperator(
        task_id="transform_destinations",
        name="transform-destinations",
        arguments=[
            "transform",
            "destinations",
            "--input", get_minio_path("destinations", "raw", "{{ ds }}"),
            "--output", get_minio_path("destinations", "transformed", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    # Validate transformed data
    validate_transform = KubernetesPodOperator(
        task_id="validate_transform",
        name="validate-transform-destinations",
        arguments=[
            "validate",
            "destinations",
            "--input", get_minio_path("destinations", "transformed", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    # Load to Kafka
    load = KubernetesPodOperator(
        task_id="load_destinations",
        name="load-destinations",
        arguments=[
            "load",
            "kafka",
            "--input", get_minio_path("destinations", "transformed", "{{ ds }}"),
            "--topic", get_kafka_topic("destinations"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    end = EmptyOperator(task_id="end")
    
    # Define pipeline flow
    start >> extract >> validate_extract >> transform >> validate_transform >> load >> end
