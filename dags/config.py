"""
Shared configuration for Theme Parks Kubernetes DAGs

This module provides common settings, environment variables, and helper functions
used across all theme parks data pipeline DAGs.
"""

import os
from datetime import timedelta
from kubernetes.client import models as k8s

# =============================================================================
# Docker Image Configuration
# =============================================================================

# Docker image (update with your registry before deploying to production)
IMAGE_NAME = os.environ.get("THEMEPARKS_IMAGE", "themeparks:latest")
# Example: "registry.example.com/themeparks:latest"

# =============================================================================
# Kubernetes Configuration
# =============================================================================

# Kubernetes namespace where pods will run
NAMESPACE = os.environ.get("AIRFLOW_K8S_NAMESPACE", "airflow")

# =============================================================================
# Storage Configuration (MinIO)
# =============================================================================

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "themeparks-pipeline")

# =============================================================================
# Kafka Configuration
# =============================================================================

KAFKA_HOST = os.environ.get("KAFKA_HOST", "kafka")
KAFKA_PORT = os.environ.get("KAFKA_PORT", "9092")

# =============================================================================
# Pipeline Configuration
# =============================================================================

# Filter for parks (e.g., "disney", "universal", "all")
PARK_FILTER = os.environ.get("PARK_FILTER", "disney")

# Email notifications
NOTIFICATION_EMAIL = os.environ.get("NOTIFICATION_EMAIL", "data-engineering@example.com")

# =============================================================================
# Common Environment Variables
# =============================================================================

COMMON_ENV_VARS = {
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "KAFKA_HOST": KAFKA_HOST,
    "KAFKA_PORT": KAFKA_PORT,
    "PYTHONUNBUFFERED": "1",
}

# =============================================================================
# Default DAG Arguments
# =============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [NOTIFICATION_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# =============================================================================
# Kubernetes Resource Profiles
# =============================================================================

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
    """
    Generate MinIO URI for data storage.
    
    Args:
        pipeline: Pipeline name (destinations, entities, live)
        stage: Processing stage (raw, transformed)
        ds: Date string (YYYY-MM-DD) or timestamp
        
    Returns:
        MinIO URI string
    """
    return f"minio://{MINIO_BUCKET}/{pipeline}/{stage}/{ds}.parquet"


def get_kafka_topic(resource_type: str) -> str:
    """
    Generate Kafka topic name for a resource type.
    
    Args:
        resource_type: Type of resource (destinations, entities, live)
        
    Returns:
        Kafka topic name
    """
    return f"themeparks.{resource_type}"


def get_common_pod_kwargs() -> dict:
    """
    Get common KubernetesPodOperator keyword arguments.
    
    Returns:
        Dictionary of common kwargs for KubernetesPodOperator
    """
    return {
        "namespace": NAMESPACE,
        "image": IMAGE_NAME,
        "cmds": ["python", "-m", "themeparks"],
        "env_vars": COMMON_ENV_VARS,
        "get_logs": True,
        "is_delete_operator_pod": True,
    }
