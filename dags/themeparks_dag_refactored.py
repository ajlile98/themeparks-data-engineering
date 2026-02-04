"""
Refactored Airflow DAGs for Theme Parks Data Pipeline

This file defines production-ready DAGs with:
- Separate Extract, Transform, Load tasks for observability
- Data quality validation at each stage
- MinIO intermediate storage for large datasets
- Proper error handling and retry logic
- SLA monitoring

Architecture:
1. destinations_daily: Extract park/destination metadata daily
2. entities_daily: Extract attractions/entities metadata daily  
3. live_data_frequent: Extract live wait times every 5 minutes
4. full_refresh_manual: Manual full refresh of all pipelines

To use:
1. Set up MinIO: docker run -p 9000:9000 minio/minio server /data
2. Set environment variables: MINIO_ENDPOINT, KAFKA_HOST, KAFKA_PORT
3. Copy to Airflow DAGs folder
"""

import os
import asyncio
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

PARK_FILTER = "disney"  # Filter for specific parks (None = all)


# =============================================================================
# Error Handling Callbacks
# =============================================================================

def on_extract_failure(context):
    """Handle extract task failures."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    logger.error(
        f"Extract failed: {dag_id}.{task_id} "
        f"(Attempt {task_instance.try_number}/{task_instance.max_tries})"
    )
    # TODO: Send alert to monitoring system (e.g., Slack, PagerDuty)


def on_transform_failure(context):
    """Handle transform task failures."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    logger.error(
        f"Transform failed: {dag_id}.{task_id}. "
        f"Raw data preserved in MinIO for investigation."
    )
    # TODO: Send alert with link to MinIO object for investigation


def on_validation_failure(context):
    """Handle validation failures."""
    task_instance = context['task_instance']
    exception = context.get('exception')
    
    logger.error(
        f"Validation failed: {task_instance.task_id}. "
        f"Error: {exception}"
    )
    # TODO: Send detailed validation report


# =============================================================================
# Task Functions - Destinations Pipeline
# =============================================================================

def extract_destinations():
    """
    Extract destinations/parks metadata.
    Saves raw data to MinIO, returns metadata via XCom.
    """
    from pipelines import DestinationsPipeline
    from loaders.MinioStorage import save_to_minio
    
    logger.info("Starting destinations extraction")
    
    # Run extraction only (no transform/load)
    pipeline = DestinationsPipeline(targets=[])  # No targets needed
    df = asyncio.run(pipeline.extract())
    
    logger.info(f"Extracted {len(df)} destinations")
    
    # Save to MinIO and return metadata
    metadata = save_to_minio(df, pipeline_name="destinations", stage="raw")
    return metadata


def transform_destinations(**context):
    """
    Transform destinations data.
    Loads from MinIO, transforms, saves back to MinIO.
    """
    from pipelines import DestinationsPipeline
    from loaders.MinioStorage import load_from_minio, save_to_minio
    
    # Get extract metadata from XCom
    ti = context['ti']
    extract_metadata = ti.xcom_pull(task_ids='extract_destinations')
    
    logger.info(f"Loading destinations from MinIO: {extract_metadata['path']}")
    df = load_from_minio(extract_metadata)
    
    # Run transformation
    pipeline = DestinationsPipeline(targets=[])
    df_transformed = pipeline.transform(df)
    
    logger.info(f"Transformed {len(df_transformed)} destinations")
    
    # Save transformed data to MinIO
    metadata = save_to_minio(df_transformed, pipeline_name="destinations", stage="transformed")
    return metadata


def load_destinations(**context):
    """
    Load destinations to Kafka.
    Loads transformed data from MinIO and sends to target.
    """
    from loaders import KafkaLoader
    from loaders.MinioStorage import load_from_minio
    
    # Get transform metadata from XCom
    ti = context['ti']
    transform_metadata = ti.xcom_pull(task_ids='transform_destinations')
    
    logger.info(f"Loading destinations from MinIO: {transform_metadata['path']}")
    df = load_from_minio(transform_metadata)
    
    # Load to Kafka
    loader = KafkaLoader(
        hostname=os.environ.get("KAFKA_HOST", "localhost"),
        port=int(os.environ.get("KAFKA_PORT", "9094")),
        topic="themeparks.destinations"
    )
    loader.load(df)
    
    logger.info(f"Loaded {len(df)} destinations to Kafka")


# =============================================================================
# Task Functions - Entities Pipeline
# =============================================================================

def extract_entities(park_filter: str | None = None):
    """Extract entities/attractions metadata."""
    from pipelines import EntityPipeline
    from loaders.MinioStorage import save_to_minio
    
    logger.info(f"Starting entities extraction (filter: {park_filter})")
    
    pipeline = EntityPipeline(targets=[], park_filter=park_filter)
    df = asyncio.run(pipeline.extract())
    
    logger.info(f"Extracted {len(df)} entities")
    
    metadata = save_to_minio(df, pipeline_name="entities", stage="raw")
    return metadata


def transform_entities(**context):
    """Transform entities data."""
    from pipelines import EntityPipeline
    from loaders.MinioStorage import load_from_minio, save_to_minio
    
    ti = context['ti']
    extract_metadata = ti.xcom_pull(task_ids='extract_entities')
    
    logger.info(f"Loading entities from MinIO: {extract_metadata['path']}")
    df = load_from_minio(extract_metadata)
    
    pipeline = EntityPipeline(targets=[])
    df_transformed = pipeline.transform(df)
    
    logger.info(f"Transformed {len(df_transformed)} entities")
    
    metadata = save_to_minio(df_transformed, pipeline_name="entities", stage="transformed")
    return metadata


def load_entities(**context):
    """Load entities to Kafka."""
    from loaders import KafkaLoader
    from loaders.MinioStorage import load_from_minio
    
    ti = context['ti']
    transform_metadata = ti.xcom_pull(task_ids='transform_entities')
    
    logger.info(f"Loading entities from MinIO: {transform_metadata['path']}")
    df = load_from_minio(transform_metadata)
    
    loader = KafkaLoader(
        hostname=os.environ.get("KAFKA_HOST", "localhost"),
        port=int(os.environ.get("KAFKA_PORT", "9092")),
        topic="themeparks.entities"
    )
    loader.load(df)
    
    logger.info(f"Loaded {len(df)} entities to Kafka")


# =============================================================================
# Task Functions - Live Data Pipeline
# =============================================================================

def extract_live_data(park_filter: str | None = None):
    """Extract live wait times."""
    from pipelines import LiveDataPipeline
    from loaders.MinioStorage import save_to_minio
    
    logger.info(f"Starting live data extraction (filter: {park_filter})")
    
    pipeline = LiveDataPipeline(targets=[], park_filter=park_filter)
    df = asyncio.run(pipeline.extract())
    
    logger.info(f"Extracted {len(df)} live records")
    
    metadata = save_to_minio(df, pipeline_name="live_data", stage="raw")
    return metadata


def transform_live_data(**context):
    """Transform live data."""
    from pipelines import LiveDataPipeline
    from loaders.MinioStorage import load_from_minio, save_to_minio
    
    ti = context['ti']
    extract_metadata = ti.xcom_pull(task_ids='extract_live_data')
    
    logger.info(f"Loading live data from MinIO: {extract_metadata['path']}")
    df = load_from_minio(extract_metadata)
    
    pipeline = LiveDataPipeline(targets=[])
    df_transformed = pipeline.transform(df)
    
    logger.info(f"Transformed {len(df_transformed)} live records")
    
    metadata = save_to_minio(df_transformed, pipeline_name="live_data", stage="transformed")
    return metadata


def load_live_data(**context):
    """Load live data to Kafka."""
    from loaders import KafkaLoader
    from loaders.MinioStorage import load_from_minio
    
    ti = context['ti']
    transform_metadata = ti.xcom_pull(task_ids='transform_live_data')
    
    logger.info(f"Loading live data from MinIO: {transform_metadata['path']}")
    df = load_from_minio(transform_metadata)
    
    loader = KafkaLoader(
        hostname=os.environ.get("KAFKA_HOST", "localhost"),
        port=int(os.environ.get("KAFKA_PORT", "9092")),
        topic="themeparks.live_data"
    )
    loader.load(df)
    
    logger.info(f"Loaded {len(df)} live records to Kafka")


# =============================================================================
# DAG 1: Destinations Daily
# =============================================================================

with DAG(
    dag_id="destinations_daily",
    description="Daily extraction of park/destination metadata with full E-T-L separation",
    default_args=default_args,
    schedule="0 6 * * *",  # 6 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "destinations", "daily"],
    max_active_runs=1,
) as dag_destinations:
    
    start = EmptyOperator(task_id="start")
    
    # Extract
    extract = PythonOperator(
        task_id="extract_destinations",
        python_callable=extract_destinations,
        on_failure_callback=on_extract_failure,
        sla=timedelta(minutes=10),
    )
    
    # Validate extraction
    validate_extract = PythonOperator(
        task_id="validate_extract",
        python_callable=lambda **context: __import__('dags.validators', fromlist=['validate_destinations_extract_task']).validate_destinations_extract_task(**context),
        on_failure_callback=on_validation_failure,
    )
    
    # Transform
    transform = PythonOperator(
        task_id="transform_destinations",
        python_callable=transform_destinations,
        on_failure_callback=on_transform_failure,
    )
    
    # Validate transformation
    validate_transform = PythonOperator(
        task_id="validate_transform",
        python_callable=lambda **context: __import__('dags.validators', fromlist=['validate_transform_task']).validate_transform_task(['id', 'name'])(**context),
        on_failure_callback=on_validation_failure,
    )
    
    # Load
    load = PythonOperator(
        task_id="load_destinations",
        python_callable=load_destinations,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Pipeline flow
    start >> extract >> validate_extract >> transform >> validate_transform >> load >> end


# =============================================================================
# DAG 2: Entities Daily
# =============================================================================

with DAG(
    dag_id="entities_daily",
    description="Daily extraction of attractions/entities metadata with full E-T-L separation",
    default_args=default_args,
    schedule="0 7 * * *",  # 7 AM daily (after destinations)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "entities", "daily"],
    max_active_runs=1,
) as dag_entities:
    
    start = EmptyOperator(task_id="start")
    
    # Extract
    extract = PythonOperator(
        task_id="extract_entities",
        python_callable=extract_entities,
        op_kwargs={"park_filter": PARK_FILTER},
        on_failure_callback=on_extract_failure,
        sla=timedelta(minutes=15),
    )
    
    # Validate extraction
    validate_extract = PythonOperator(
        task_id="validate_extract",
        python_callable=lambda **context: __import__('dags.validators', fromlist=['validate_entities_extract_task']).validate_entities_extract_task(**context),
        on_failure_callback=on_validation_failure,
    )
    
    # Transform
    transform = PythonOperator(
        task_id="transform_entities",
        python_callable=transform_entities,
        on_failure_callback=on_transform_failure,
    )
    
    # Validate transformation
    validate_transform = PythonOperator(
        task_id="validate_transform",
        python_callable=lambda **context: __import__('dags.validators', fromlist=['validate_transform_task']).validate_transform_task(['id', 'name', 'entityType'])(**context),
        on_failure_callback=on_validation_failure,
    )
    
    # Load
    load = PythonOperator(
        task_id="load_entities",
        python_callable=load_entities,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Pipeline flow
    start >> extract >> validate_extract >> transform >> validate_transform >> load >> end


# =============================================================================
# DAG 3: Live Data Frequent
# =============================================================================

with DAG(
    dag_id="live_data_frequent",
    description="Frequent extraction of live wait times with full E-T-L separation",
    default_args={
        **default_args,
        "retries": 2,  # Fewer retries for frequent runs
        "retry_delay": timedelta(minutes=2),
    },
    schedule="*/5 8-23 * * *",  # Every 5 min, 8 AM to 11 PM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "live", "frequent"],
    max_active_runs=1,
) as dag_live:
    
    start = EmptyOperator(task_id="start")
    
    # Extract
    extract = PythonOperator(
        task_id="extract_live_data",
        python_callable=extract_live_data,
        op_kwargs={"park_filter": PARK_FILTER},
        on_failure_callback=on_extract_failure,
        sla=timedelta(minutes=3),
    )
    
    # Validate extraction
    validate_extract = PythonOperator(
        task_id="validate_extract",
        python_callable=lambda **context: __import__('dags.validators', fromlist=['validate_live_extract_task']).validate_live_extract_task(**context),
        on_failure_callback=on_validation_failure,
    )
    
    # Transform
    transform = PythonOperator(
        task_id="transform_live_data",
        python_callable=transform_live_data,
        on_failure_callback=on_transform_failure,
    )
    
    # Load
    load = PythonOperator(
        task_id="load_live_data",
        python_callable=load_live_data,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Pipeline flow (skip some validations for speed)
    start >> extract >> validate_extract >> transform >> load >> end


# =============================================================================
# DAG 4: Full Refresh (Manual)
# =============================================================================

with DAG(
    dag_id="full_refresh_manual",
    description="Manual full refresh of all pipelines",
    default_args=default_args,
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "manual", "full-refresh"],
) as dag_refresh:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Destinations group
    with TaskGroup("destinations_pipeline") as dest_group:
        dest_extract = PythonOperator(
            task_id="extract",
            python_callable=extract_destinations,
        )
        dest_transform = PythonOperator(
            task_id="transform",
            python_callable=transform_destinations,
        )
        dest_load = PythonOperator(
            task_id="load",
            python_callable=load_destinations,
        )
        dest_extract >> dest_transform >> dest_load
    
    # Entities group
    with TaskGroup("entities_pipeline") as entities_group:
        ent_extract = PythonOperator(
            task_id="extract",
            python_callable=extract_entities,
            op_kwargs={"park_filter": PARK_FILTER},
        )
        ent_transform = PythonOperator(
            task_id="transform",
            python_callable=transform_entities,
        )
        ent_load = PythonOperator(
            task_id="load",
            python_callable=load_entities,
        )
        ent_extract >> ent_transform >> ent_load
    
    # Live data group
    with TaskGroup("live_pipeline") as live_group:
        live_extract = PythonOperator(
            task_id="extract",
            python_callable=extract_live_data,
            op_kwargs={"park_filter": PARK_FILTER},
        )
        live_transform = PythonOperator(
            task_id="transform",
            python_callable=transform_live_data,
        )
        live_load = PythonOperator(
            task_id="load",
            python_callable=load_live_data,
        )
        live_extract >> live_transform >> live_load
    
    # Flow: destinations first, then entities and live in parallel
    start >> dest_group >> [entities_group, live_group] >> end
