
"""
## Theme Park ETL — Entities (bronze)

Triggered by the raw_theme_park_destinations asset.
Reads destinations from the bronze JSONL path passed via XCom (tiny string),
fetches child entities for every park, and writes results to the bronze layer.
Returns the JSONL path string via XCom.
"""

from datetime import datetime, timezone
from airflow.sdk import Asset, asset


@asset(schedule=[Asset("raw_theme_park_destinations")])
def raw_theme_park_entities(context: dict) -> str:
    """Fetch entities for all parks and write to bronze layer. Returns JSONL path."""
    from include.extractors.themeparks import ThemeParksClient
    from include.writers.bronze_writer import read_bronze, write_bronze
    import asyncio

    ingest_timestamp = datetime.now(timezone.utc).isoformat()

    # XCom carries a tiny path string — no S3 backend pressure.
    # Guard against Airflow returning a list when multiple prior runs exist
    # (can happen with include_prior_dates=True across dag boundary pulls).
    destinations_path = context["ti"].xcom_pull(
        dag_id="raw_theme_park_destinations",
        task_ids="raw_theme_park_destinations",
        key="return_value",
        include_prior_dates=True,
    )
    if isinstance(destinations_path, list):
        destinations_path = destinations_path[-1]  # take the most-recent path
    destinations = read_bronze(destinations_path)
    print(f"Read {len(destinations)} destinations from {destinations_path}")

    park_ids = []
    for dest in destinations:
        for park in dest.get("parks", []):
            if park.get("id"):
                park_ids.append(park["id"])
    print(f"Fetching entities for {len(park_ids)} parks")

    async def fetch(park_ids: list[str]):
        async with ThemeParksClient() as client:
            tasks = [client.get_entity_children(pid) for pid in park_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            records = []
            for park_id, result in zip(park_ids, results):
                if isinstance(result, Exception):
                    print(f"Failed to get entities for park {park_id}: {result}")
                    continue
                for entity in result.get("children", []):
                    records.append({
                        "park_id": park_id,
                        "id": entity.get("id"),
                        "name": entity.get("name"),
                        "entityType": entity.get("entityType"),
                        "ingest_timestamp": ingest_timestamp,
                    })
            print(f"Extracted {len(records)} entities from {len(park_ids)} parks")
            return records

    records = asyncio.run(fetch(park_ids))
    return write_bronze(records, prefix="entities")

