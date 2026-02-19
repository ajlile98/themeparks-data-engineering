
"""
## Theme Park ETL 

This DAG queries the live data of theme parks available on themepark api

"""

from datetime import datetime
from airflow.sdk import asset, get_current_context

@asset(schedule="0 */1 * * *")
def raw_theme_park_destinations() -> list[dict]:
    """extracts theme park destinations"""
    from include.extractors.themeparks import ThemeParksClient
    import asyncio

    ingest_timestamp = datetime.now().isoformat()

    async def fetch_destinations():
        """Async function to fetch theme park destinations"""
        async with ThemeParksClient() as client:
            print("\nFetching all destinations...")
            
            destinations = await client.get_destinations()
            print(f"Found {len(destinations.get('destinations', []))} destinations\n")

            destination_records = destinations.get('destinations', [])
            for destination in destination_records:
                destination["ingest_timestamp"] = ingest_timestamp

            return destination_records
    
    return asyncio.run(fetch_destinations())

