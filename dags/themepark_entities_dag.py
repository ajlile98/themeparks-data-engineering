
"""
## Theme Park ETL — Entities (bronze)

Triggered by the raw_theme_park_destinations asset.

Uses **dynamic task mapping** — one `fetch_park_entities` task instance per
park_id — so a single park's transient API failure is retried in isolation and
does not block entity extraction for every other park.

Task graph:
  get_park_ids  →  fetch_park_entities[0..N]  →  merge_and_write
                    (one mapped task per park)      (emits the asset)
"""

from datetime import datetime, timezone, timedelta
from airflow.sdk import dag, task, Asset

RAW_ENTITIES_ASSET = Asset("raw_theme_park_entities")


@dag(
    schedule=[Asset("raw_theme_park_destinations")],
    catchup=False,
    tags=["themeparks", "bronze"],
)
def raw_theme_park_entities():
    """Fetch entities for every park — one task per park via dynamic task mapping."""

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=2),
    )
    def get_park_ids(**context) -> dict:
        """Read destinations bronze path from XCom, return park IDs and a shared ingest timestamp."""
        from include.writers.bronze_writer import read_bronze

        destinations_path = context["ti"].xcom_pull(
            dag_id="raw_theme_park_destinations",
            task_ids="fetch_and_write",
            key="return_value",
            include_prior_dates=True,
        )
        if isinstance(destinations_path, list):
            destinations_path = destinations_path[-1]

        destinations = read_bronze(destinations_path)
        park_ids = [
            park["id"]
            for dest in destinations
            for park in dest.get("parks", [])
            if park.get("id")
        ]
        ingest_timestamp = datetime.now(timezone.utc).isoformat()
        print(f"Resolved {len(park_ids)} park IDs from {destinations_path}, ingest_timestamp={ingest_timestamp}")
        return {"park_ids": park_ids, "ingest_timestamp": ingest_timestamp}

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=5),
    )
    def fetch_park_entities(park_id: str, ingest_timestamp: str) -> list[dict]:
        """Fetch entity children for a single park. Retried independently on failure."""
        from include.extractors.themeparks import ThemeParksClient

        client = ThemeParksClient()
        result = client.get_entity_children_sync(park_id)
        records = [
            {
                "park_id": park_id,
                "id": entity.get("id"),
                "name": entity.get("name"),
                "entityType": entity.get("entityType"),
                "ingest_timestamp": ingest_timestamp,
            }
            for entity in result.get("children", [])
        ]
        print(f"Park {park_id}: fetched {len(records)} entities")
        return records

    @task(outlets=[RAW_ENTITIES_ASSET])
    def merge_and_write(all_records: list[list[dict]]) -> str:
        """Combine per-park entity lists, write a single bronze JSONL, emit the asset."""
        from include.writers.bronze_writer import write_bronze

        merged = [record for park_records in all_records for record in park_records]
        print(f"Merging {len(merged)} entity records from {len(all_records)} parks")
        return write_bronze(merged, prefix="entities")

    meta = get_park_ids()
    per_park = fetch_park_entities.partial(ingest_timestamp=meta["ingest_timestamp"]).expand(park_id=meta["park_ids"])
    merge_and_write(per_park)


raw_theme_park_entities()

