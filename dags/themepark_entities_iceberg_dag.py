"""
## Theme Park ETL — Entities → Iceberg (silver)

Triggered by the raw_theme_park_entities asset.
Reads the bronze JSONL file (path from XCom), converts to Arrow, and
appends to the silver Iceberg table via the Nessie catalog.
"""

from airflow.sdk import Asset, asset


@asset(schedule=[Asset("raw_theme_park_entities")])
def iceberg_entities(context: dict) -> dict:
    """Append entities bronze JSONL to the Iceberg silver table."""
    from include.writers.bronze_writer import read_bronze
    from include.writers.iceberg_writer import get_catalog, append_to_iceberg

    path = context["ti"].xcom_pull(
        dag_id="raw_theme_park_entities",
        task_ids="raw_theme_park_entities",
        key="return_value",
        include_prior_dates=True,
    )
    if isinstance(path, list):
        path = path[-1]  # take the most-recent path

    records = read_bronze(path)
    print(f"[entities] read {len(records)} records from {path}")

    catalog = get_catalog()
    result = append_to_iceberg(catalog, namespace="silver", table_name="entities", records=records)
    return result
