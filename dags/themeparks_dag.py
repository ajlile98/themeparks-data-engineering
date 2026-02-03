"""
Airflow DAGs for Theme Parks Data Pipeline

This file defines DAGs for orchestrating the theme parks ETL pipelines:

1. themeparks_static_daily: Runs destinations + entities once per day
2. themeparks_live_frequent: Runs live data every 5 minutes during park hours

To use:
1. Install Airflow: pip install apache-airflow
2. Copy this file to your Airflow DAGs folder (usually ~/airflow/dags/)
3. Ensure your project is in PYTHONPATH or installed as a package
4. Configure Airflow connections/variables as needed

Note: This is a template - adjust paths and schedules for your environment.
"""

# git sync bump

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Default args applied to all tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Filter for which parks to collect (None = all parks)
PARK_FILTER = "disney"


# -----------------------------------------------------------------------------
# Task Functions
# -----------------------------------------------------------------------------

def run_destinations_pipeline():
    """Task: Extract destination/park metadata."""
    from pipelines import DestinationsPipeline
    from loaders import KafkaLoader
    
    targets = [
        KafkaLoader(
            hostname=os.environ.get("KAFKA_HOST", "localhost"),
            port=int(os.environ.get("KAFKA_PORT", "9092")),
            topic="themeparks.destinations"
        )
    ]
    pipeline = DestinationsPipeline(targets)
    pipeline.run()


def run_entities_pipeline(park_filter: str | None = None):
    """Task: Extract entity metadata."""
    from pipelines import EntityPipeline
    from loaders import KafkaLoader
    
    targets = [
        KafkaLoader(
            hostname=os.environ.get("KAFKA_HOST", "localhost"),
            port=int(os.environ.get("KAFKA_PORT", "9092")),
            topic="themeparks.entities"
        )
    ]
    pipeline = EntityPipeline(targets, park_filter=park_filter)
    pipeline.run()


def run_live_data_pipeline(park_filter: str | None = None):
    """Task: Extract live wait times."""
    from pipelines import LiveDataPipeline
    from loaders import KafkaLoader
    
    targets = [
        KafkaLoader(
            hostname=os.environ.get("KAFKA_HOST", "localhost"),
            port=int(os.environ.get("KAFKA_PORT", "9092")),
            topic="themeparks.live_data"
        )
    ]
    pipeline = LiveDataPipeline(targets, park_filter=park_filter)
    pipeline.run()


# -----------------------------------------------------------------------------
# DAG 1: Static Data (Daily)
# -----------------------------------------------------------------------------

with DAG(
    dag_id="themeparks_static_daily",
    description="Daily extraction of destinations and entity metadata",
    default_args=default_args,
    schedule="0 6 * * *",  # 6 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "etl", "daily"],
) as dag_static:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Extract destinations (parks don't change often)
    extract_destinations = PythonOperator(
        task_id="extract_destinations",
        python_callable=run_destinations_pipeline,
    )
    
    # Extract entities (attractions, shows, etc.)
    extract_entities = PythonOperator(
        task_id="extract_entities",
        python_callable=run_entities_pipeline,
        op_kwargs={"park_filter": PARK_FILTER},
    )
    
    # Dependencies: destinations -> entities (entities need park IDs)
    start >> extract_destinations >> extract_entities >> end


# -----------------------------------------------------------------------------
# DAG 2: Live Data (Frequent)
# -----------------------------------------------------------------------------

with DAG(
    dag_id="themeparks_live_frequent",
    description="Frequent extraction of live wait times and status",
    default_args={
        **default_args,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    schedule="*/5 8-23 * * *",  # Every 5 min from 8 AM to 11 PM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlap
    tags=["themeparks", "etl", "live", "frequent"],
) as dag_live:
    
    extract_live = PythonOperator(
        task_id="extract_live_data",
        python_callable=run_live_data_pipeline,
        op_kwargs={"park_filter": PARK_FILTER},
    )


# -----------------------------------------------------------------------------
# DAG 3: Full Backfill (Manual)
# -----------------------------------------------------------------------------

with DAG(
    dag_id="themeparks_full_backfill",
    description="Manual trigger to run all pipelines",
    default_args=default_args,
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["themeparks", "etl", "manual", "backfill"],
) as dag_backfill:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    destinations = PythonOperator(
        task_id="extract_destinations",
        python_callable=run_destinations_pipeline,
    )
    
    entities = PythonOperator(
        task_id="extract_entities", 
        python_callable=run_entities_pipeline,
        op_kwargs={"park_filter": PARK_FILTER},
    )
    
    live = PythonOperator(
        task_id="extract_live_data",
        python_callable=run_live_data_pipeline,
        op_kwargs={"park_filter": PARK_FILTER},
    )
    
    # Run destinations first, then entities and live in parallel
    start >> destinations >> [entities, live] >> end
