"""
Kubernetes-Ready Airflow DAGs for Theme Parks Data Pipeline

Uses KubernetesPodOperator to run pipeline tasks as Kubernetes pods.
Each task runs in isolation with its own resources and dependencies.

Architecture:
- Single Docker image with CLI entrypoints
- Separate E-T-L tasks for observability
- Data passed via MinIO (not XCom)
- Resource limits per task
- Failure isolation

To deploy:
1. Build Docker image: docker build -t themeparks:latest -f docker/Dockerfile .
2. Push to registry: docker tag themeparks:latest registry.example.com/themeparks:latest
3. Update IMAGE_NAME below with your registry
4. Deploy DAG to Airflow
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s

# =============================================================================
# Configuration
# =============================================================================

# Docker image (update with your registry)
IMAGE_NAME = "themeparks:latest"  # or "registry.example.com/themeparks:latest"

# Kubernetes namespace
NAMESPACE = os.environ.get("AIRFLOW_K8S_NAMESPACE", "airflow")

# MinIO configuration (passed as env vars to pods)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

# Kafka configuration
KAFKA_HOST = os.environ.get("KAFKA_HOST", "kafka")
KAFKA_PORT = os.environ.get("KAFKA_PORT", "9092")

# Park filter
PARK_FILTER = "disney"

# Common environment variables for all pods
COMMON_ENV_VARS = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "KAFKA_HOST": KAFKA_HOST,
    "KAFKA_PORT": KAFKA_PORT,
    "PYTHONUNBUFFERED": "1",
}

# Default DAG args
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Resource configurations
SMALL_RESOURCES = k8s.V1ResourceRequirements(
    requests={"memory": "256Mi", "cpu": "250m"},
    limits={"memory": "512Mi", "cpu": "500m"}
)

MEDIUM_RESOURCES = k8s.V1ResourceRequirements(
    requests={"memory": "512Mi", "cpu": "500m"},
    limits={"memory": "1Gi", "cpu": "1000m"}
)

LARGE_RESOURCES = k8s.V1ResourceRequirements(
    requests={"memory": "1Gi", "cpu": "1000m"},
    limits={"memory": "2Gi", "cpu": "2000m"}
)


# =============================================================================
# Helper Functions
# =============================================================================

def get_minio_path(pipeline: str, stage: str, ds: str) -> str:
    """Generate MinIO path for data."""
    return f"minio://themeparks-pipeline/{pipeline}/{stage}/{ds}.parquet"


# =============================================================================
# DAG 1: Destinations Daily
# =============================================================================

with DAG(
    dag_id="destinations_daily_k8s",
    description="Daily destinations extraction using KubernetesPodOperator",
    default_args=default_args,
    schedule="0 6 * * *",  # 6 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "destinations", "kubernetes"],
    max_active_runs=1,
) as dag_destinations:
    
    start = EmptyOperator(task_id="start")
    
    # Extract
    extract = KubernetesPodOperator(
        task_id="extract_destinations",
        name="extract-destinations",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "extract",
            "destinations",
            "--output", "{{ ti.xcom_pull(task_ids='get_extract_path') }}",
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Validate extract
    validate_extract = KubernetesPodOperator(
        task_id="validate_extract",
        name="validate-extract-destinations",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "validate",
            "destinations",
            "--input", get_minio_path("destinations", "raw", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Transform
    transform = KubernetesPodOperator(
        task_id="transform_destinations",
        name="transform-destinations",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "transform",
            "destinations",
            "--input", get_minio_path("destinations", "raw", "{{ ds }}"),
            "--output", get_minio_path("destinations", "transformed", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Validate transform
    validate_transform = KubernetesPodOperator(
        task_id="validate_transform",
        name="validate-transform-destinations",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "validate",
            "destinations",
            "--input", get_minio_path("destinations", "transformed", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Load to Kafka
    load = KubernetesPodOperator(
        task_id="load_destinations",
        name="load-destinations",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "load",
            "kafka",
            "--input", get_minio_path("destinations", "transformed", "{{ ds }}"),
            "--topic", "themeparks.destinations",
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Pipeline flow
    start >> extract >> validate_extract >> transform >> validate_transform >> load >> end


# =============================================================================
# DAG 2: Entities Daily
# =============================================================================

with DAG(
    dag_id="entities_daily_k8s",
    description="Daily entities extraction using KubernetesPodOperator",
    default_args=default_args,
    schedule="0 7 * * *",  # 7 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "entities", "kubernetes"],
    max_active_runs=1,
) as dag_entities:
    
    start = EmptyOperator(task_id="start")
    
    # Extract
    extract = KubernetesPodOperator(
        task_id="extract_entities",
        name="extract-entities",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "extract",
            "entities",
            "--output", get_minio_path("entities", "raw", "{{ ds }}"),
            "--park-filter", PARK_FILTER,
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=MEDIUM_RESOURCES,  # More resources for parallel API calls
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Validate extract
    validate_extract = KubernetesPodOperator(
        task_id="validate_extract",
        name="validate-extract-entities",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "validate",
            "entities",
            "--input", get_minio_path("entities", "raw", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Transform
    transform = KubernetesPodOperator(
        task_id="transform_entities",
        name="transform-entities",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "transform",
            "entities",
            "--input", get_minio_path("entities", "raw", "{{ ds }}"),
            "--output", get_minio_path("entities", "transformed", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Validate transform
    validate_transform = KubernetesPodOperator(
        task_id="validate_transform",
        name="validate-transform-entities",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "validate",
            "entities",
            "--input", get_minio_path("entities", "transformed", "{{ ds }}"),
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Load to Kafka
    load = KubernetesPodOperator(
        task_id="load_entities",
        name="load-entities",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "load",
            "kafka",
            "--input", get_minio_path("entities", "transformed", "{{ ds }}"),
            "--topic", "themeparks.entities",
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Pipeline flow
    start >> extract >> validate_extract >> transform >> validate_transform >> load >> end


# =============================================================================
# DAG 3: Live Data Frequent
# =============================================================================

with DAG(
    dag_id="live_data_frequent_k8s",
    description="Frequent live data extraction using KubernetesPodOperator",
    default_args={
        **default_args,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    schedule="*/5 8-23 * * *",  # Every 5 min, 8 AM - 11 PM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "live", "kubernetes"],
    max_active_runs=1,
) as dag_live:
    
    start = EmptyOperator(task_id="start")
    
    # Extract
    extract = KubernetesPodOperator(
        task_id="extract_live",
        name="extract-live",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "extract",
            "live",
            "--output", get_minio_path("live", "raw", "{{ ts_nodash }}"),
            "--park-filter", PARK_FILTER,
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=MEDIUM_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Validate extract
    validate_extract = KubernetesPodOperator(
        task_id="validate_extract",
        name="validate-extract-live",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "validate",
            "live",
            "--input", get_minio_path("live", "raw", "{{ ts_nodash }}"),
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Transform
    transform = KubernetesPodOperator(
        task_id="transform_live",
        name="transform-live",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "transform",
            "live",
            "--input", get_minio_path("live", "raw", "{{ ts_nodash }}"),
            "--output", get_minio_path("live", "transformed", "{{ ts_nodash }}"),
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    # Load to Kafka
    load = KubernetesPodOperator(
        task_id="load_live",
        name="load-live",
        namespace=NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python", "-m", "themeparks"],
        arguments=[
            "load",
            "kafka",
            "--input", get_minio_path("live", "transformed", "{{ ts_nodash }}"),
            "--topic", "themeparks.live",
            "--log-level", "INFO",
        ],
        env_vars=COMMON_ENV_VARS,
        resources=SMALL_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Pipeline flow (skip some validations for speed)
    start >> extract >> validate_extract >> transform >> load >> end
