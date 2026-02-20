"""
## Theme Park ETL — Destinations → Iceberg (silver)

Triggered by the raw_theme_park_destinations asset.
Reads the bronze JSONL file (path from XCom), converts to Arrow, and
appends to the silver Iceberg table via the Nessie catalog.
"""

from airflow.sdk import Asset, asset


@asset(schedule=[Asset("raw_theme_park_destinations")])
def iceberg_destinations(context: dict) -> dict:
    """Append destinations bronze JSONL to the Iceberg silver table."""
    from include.writers.bronze_writer import read_bronze
    from include.writers.iceberg_writer import get_catalog, append_to_iceberg

    path = context["ti"].xcom_pull(
        dag_id="raw_theme_park_destinations",
        task_ids="raw_theme_park_destinations",
        key="return_value",
        include_prior_dates=True,
    )
    if isinstance(path, list):
        path = path[-1]  # take the most-recent path

    records = read_bronze(path)
    print(f"[destinations] read {len(records)} records from {path}")

    catalog = get_catalog()
    result = append_to_iceberg(catalog, namespace="silver", table_name="destinations", records=records)
    return result
