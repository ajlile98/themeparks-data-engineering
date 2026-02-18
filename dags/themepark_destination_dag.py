
"""
## Theme Park ETL 

This DAG queries the live data of theme parks available on themepark api

"""

from airflow.sdk import Asset, dag, task, asset

@asset(schedule="@daily")
def raw_theme_park_destinations() -> list[dict]:
    """extracts theme park destinations"""
    from include.extractors.themeparks import ThemeParksClient
    import asyncio

    async def fetch_destinations():
        """Async function to fetch theme park destinations"""
        async with ThemeParksClient() as client:
            # Get all destinations
            print("\nFetching all destinations...")
            
            destinations = await client.get_destinations()
            print(f"Found {len(destinations.get('destinations', []))} destinations\n")
            
            return destinations
    
    # Run the async function synchronously
    return asyncio.run(fetch_destinations())

