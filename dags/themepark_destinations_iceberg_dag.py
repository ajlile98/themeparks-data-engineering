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

    # Explode parks out of each destination into a flat list for silver.parks.
    # This gives entities and live_data a clean table to join park_id against.
    park_records = []
    for dest in records:
        for park in dest.get("parks", []):
            if park.get("id"):
                park_records.append({
                    "destination_id": dest.get("id"),
                    "destination_name": dest.get("name"),
                    "id": park.get("id"),
                    "name": park.get("name"),
                    "slug": park.get("slug"),
                })
    print(f"[parks] exploded {len(park_records)} park records from {len(records)} destinations")

    catalog = get_catalog()
    result = append_to_iceberg(catalog, namespace="silver", table_name="destinations", records=records, overwrite=True)
    parks_result = append_to_iceberg(catalog, namespace="silver", table_name="parks", records=park_records, overwrite=True)
    print(f"[parks] {parks_result}")
    return result
