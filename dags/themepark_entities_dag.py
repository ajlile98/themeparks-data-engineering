
"""
## Theme Park ETL — Entities (bronze)

Triggered by the raw_theme_park_destinations asset.

Uses **dynamic task mapping** — one `fetch_park_entities` task instance per
park_id — so a single park's transient API failure is retried in isolation and
does not block entity extraction for every other park.

Source: GET https://queue-times.com/parks/{park_id}/queue_times.json
Entities (rides) are extracted from the queue_times response — there is no
dedicated entity-children endpoint in the queue-times.com API. Each ride
record includes land grouping (land_id, land_name) as extra enrichment.

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
    def get_park_ids(**context) -> list[int]:
        """Read destinations bronze path from XCom, return the list of park IDs."""
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
        print(f"Resolved {len(park_ids)} park IDs from {destinations_path}")
        return park_ids

    @task
    def get_ingest_timestamp() -> str:
        """Capture a single shared ingest timestamp for all mapped park tasks."""
        return datetime.now(timezone.utc).isoformat()

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=5),
    )
    def fetch_park_entities(park_id: int, ingest_timestamp: str) -> list[dict]:
        """Fetch ride catalog for a single park via queue_times. Retried independently on failure."""
        from include.extractors.queue_times import QueueTimesClient

        client = QueueTimesClient()
        result = client.get_park_queue_times_sync(park_id)
        rides = QueueTimesClient.extract_rides(park_id, result)
        records = [
            {
                "park_id":   ride["park_id"],
                "land_id":   ride["land_id"],
                "land_name": ride["land_name"],
                "id":        ride["id"],
                "name":      ride["name"],
                "entityType": "ATTRACTION",
                "ingest_timestamp": ingest_timestamp,
            }
            for ride in rides
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

    park_ids = get_park_ids()
    ingest_timestamp = get_ingest_timestamp()
    per_park = fetch_park_entities.partial(ingest_timestamp=ingest_timestamp).expand(park_id=park_ids)
    merge_and_write(per_park)


raw_theme_park_entities()

