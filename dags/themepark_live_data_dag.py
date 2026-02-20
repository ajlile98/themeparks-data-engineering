
"""
## Theme Park ETL — Live Data (bronze)

Runs every 5 minutes. Reads the entities JSONL path from XCom (tiny string),
fetches current wait times for every ride in every park, and writes results
to the bronze layer as JSONL. Returns the path string via XCom.

Queue data is stored as a raw dict in JSONL (not stringified) so the silver
Iceberg writer can flatten it properly.
"""

from datetime import datetime, timezone
from airflow.sdk import asset, get_current_context


@asset(schedule="*/5 * * * *")
def raw_theme_park_live_data() -> str:
    """Fetch live wait times for all parks and write to bronze layer. Returns JSONL path."""
    from include.extractors.themeparks import ThemeParksClient
    from include.writers.bronze_writer import read_bronze, write_bronze
    import asyncio

    ctx = get_current_context()
    ingest_timestamp = datetime.now(timezone.utc).isoformat()

    # XCom carries a tiny path string — no async conflict, no S3 backend pressure
    entities_path = ctx["ti"].xcom_pull(
        dag_id="raw_theme_park_entities",
        task_ids="raw_theme_park_entities",
        key="return_value",
        include_prior_dates=True,
    )
    if isinstance(entities_path, list):
        entities_path = entities_path[-1]  # take the most-recent path
    entities = read_bronze(entities_path) if entities_path else []
    park_ids = sorted({e["park_id"] for e in entities if isinstance(e, dict) and e.get("park_id")})
    print(f"Fetching live data for {len(park_ids)} parks")

    async def fetch(park_ids: list[str]):
        async with ThemeParksClient() as client:
            tasks = [client.get_entity_live(pid) for pid in park_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            records = []
            for park_id, result in zip(park_ids, results):
                if isinstance(result, Exception):
                    print(f"Failed to get live data for park {park_id}: {result}")
                    continue
                for item in result.get("liveData", []):
                    records.append({
                        "park_id": park_id,
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "entityType": item.get("entityType"),
                        "status": item.get("status"),
                        # Store raw dict in bronze — silver layer flattens it
                        "queue": item.get("queue", {}),
                        "lastUpdated": item.get("lastUpdated"),
                        "ingest_timestamp": ingest_timestamp,
                    })
            print(f"Extracted {len(records)} live records from {len(park_ids)} parks")
            return records

    records = asyncio.run(fetch(park_ids))
    return write_bronze(records, prefix="live")

