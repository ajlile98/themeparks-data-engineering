
"""
## Theme Park ETL — Destinations (bronze)

Hourly extraction of all theme park destinations from the ThemeParks.wiki API.
Raw records are written as JSONL to the MinIO bronze layer.
Returns the object storage path string via XCom (~80 bytes — no S3 XCom backend needed).
"""

from datetime import datetime, timezone, timedelta
from airflow.sdk import dag, task, Asset

RAW_DESTINATIONS_ASSET = Asset("raw_theme_park_destinations")


@dag(
    schedule="0 */1 * * *",
    catchup=False,
    tags=["themeparks", "bronze"],
)
def raw_theme_park_destinations():
    """Hourly extraction of all theme park destinations from the ThemeParks.wiki API."""

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=5),
        outlets=[RAW_DESTINATIONS_ASSET],
    )
    def fetch_and_write() -> str:
        """Fetch all destinations and write to bronze layer. Returns JSONL path."""
        from include.extractors.themeparks import ThemeParksClient
        from include.writers.bronze_writer import write_bronze
        import asyncio

        ingest_timestamp = datetime.now(timezone.utc).isoformat()

        async def fetch():
            async with ThemeParksClient() as client:
                print("Fetching all destinations...")
                response = await client.get_destinations()
                records = response.get("destinations", [])
                print(f"Found {len(records)} destinations")
                for r in records:
                    r["ingest_timestamp"] = ingest_timestamp
                return records

        records = asyncio.run(fetch())
        return write_bronze(records, prefix="destinations")

    fetch_and_write()


raw_theme_park_destinations()

