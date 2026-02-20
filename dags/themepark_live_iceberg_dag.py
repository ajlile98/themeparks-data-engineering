"""
## Theme Park ETL — Live Data → Iceberg (silver)

Triggered by the raw_theme_park_live_data asset (every 5 minutes).
Reads the bronze JSONL file (path from XCom), converts to Arrow, and
appends to the silver Iceberg table via the Nessie catalog.

Note: The 'queue' column arrives as a nested struct from JSONL.
Flattening (queue_wait_time, queue_type, etc.) is a gold-layer concern.
"""

from airflow.sdk import Asset, asset


@asset(schedule=[Asset("raw_theme_park_live_data")])
def iceberg_live_data(context: dict) -> dict:
    """Append live data bronze JSONL to the Iceberg silver table."""
    from include.writers.bronze_writer import read_bronze
    from include.writers.iceberg_writer import get_catalog, append_to_iceberg

    path = context["ti"].xcom_pull(
        dag_id="raw_theme_park_live_data",
        task_ids="raw_theme_park_live_data",
        key="return_value",
        include_prior_dates=True,
    )
    if isinstance(path, list):
        path = path[-1]  # take the most-recent path

    records = read_bronze(path)
    print(f"[live_data] read {len(records)} records from {path}")

    catalog = get_catalog()
    result = append_to_iceberg(catalog, namespace="silver", table_name="live_data", records=records)
    return result
