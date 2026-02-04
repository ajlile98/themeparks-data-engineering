"""
Entities Daily DAG

Extracts, validates, transforms, and loads theme park entity data daily.
Entities include rides, shows, restaurants, and other attractions.
Runs at 7 AM daily.
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
    PARK_FILTER,
    SMALL_RESOURCES,
    MEDIUM_RESOURCES,
)

# DAG definition
with DAG(
    dag_id="entities_daily_k8s",
    description="Daily entities extraction using KubernetesPodOperator",
    default_args=default_args,
    schedule="0 7 * * *",  # 7 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "entities", "kubernetes", "daily"],
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # Extract entities from API (parallel API calls, needs more resources)
    extract = KubernetesPodOperator(
        task_id="extract_entities",
        name="extract-entities",
        arguments=[
            "extract",
            "entities",
            "--output", get_minio_path("entities", "raw", "{{ ds }}"),
            "--park-filter", PARK_FILTER,
            "--log-level", "INFO",
        ],
        resources=MEDIUM_RESOURCES,  # More resources for parallel API calls
        **get_common_pod_kwargs()
    )
    
    # Validate extracted data
    validate_extract = KubernetesPodOperator(
        task_id="validate_extract",
        name="validate-extract-entities",
        arguments=[
            "validate",
            "entities",
            "--input", get_minio_path("entities", "raw", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    # Transform data (add metadata)
    transform = KubernetesPodOperator(
        task_id="transform_entities",
        name="transform-entities",
        arguments=[
            "transform",
            "entities",
            "--input", get_minio_path("entities", "raw", "{{ ds }}"),
            "--output", get_minio_path("entities", "transformed", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    # Validate transformed data
    validate_transform = KubernetesPodOperator(
        task_id="validate_transform",
        name="validate-transform-entities",
        arguments=[
            "validate",
            "entities",
            "--input", get_minio_path("entities", "transformed", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    # Load to Kafka
    load = KubernetesPodOperator(
        task_id="load_entities",
        name="load-entities",
        arguments=[
            "load",
            "kafka",
            "--input", get_minio_path("entities", "transformed", "{{ ds }}"),
            "--topic", get_kafka_topic("entities"),
            "--log-level", "INFO",
        ],
        resources=SMALL_RESOURCES,
        **get_common_pod_kwargs()
    )
    
    end = EmptyOperator(task_id="end")
    
    # Define pipeline flow
    start >> extract >> validate_extract >> transform >> validate_transform >> load >> end
