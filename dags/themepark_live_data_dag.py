
"""
## Theme Park ETL 

This DAG queries the list of theme parks available on themepark api
then gets existing wait times for each ride in each park.

"""

from airflow.sdk import Asset, dag, task, asset, get_current_context

@asset(
    schedule="*/5 * * * *", 
    params={
        "park_filter": 'disney'
    }
    )
def raw_theme_park_live_data() -> list[dict]:
    """extracts theme park live data"""
    from include.extractors.themeparks import ThemeParksClient
    import asyncio
    
    ctx = get_current_context()

    park_filter = ctx['params']['park_filter'].lower().strip()

    async def fetch_live_data():
        """Async function to fetch theme park live data"""
        async with ThemeParksClient() as client:
            # Get destinations
            data = await client.get_destinations()
            destinations = data.get("destinations", [])
            
            # Apply filter if specified
            if park_filter:
                destinations = [
                    d for d in destinations 
                    if park_filter.lower() in d.get("name", "").lower()
                ]
                print(f"Filtered to {len(destinations)} destinations")
            
            # Collect all park IDs
            park_ids = []
            for dest in destinations:
                parks = dest.get("parks", [])
                park_ids.extend([p.get("id") for p in parks if p.get("id")])
            
            print(f"Fetching live data for {len(park_ids)} parks")
            
            # Fetch live data for all parks in parallel
            tasks = [client.get_entity_live(park_id) for park_id in park_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Flatten results
            records = []
            for park_id, result in zip(park_ids, results):
                if isinstance(result, Exception):
                    print(f"Failed to get live data for park {park_id}: {result}")
                    continue
                
                live_data = result.get("liveData", [])
                for item in live_data:
                    record = {
                        "park_id": park_id,
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "entityType": item.get("entityType"),
                        "status": item.get("status"),
                        "queue": str(item.get("queue", {})),  # Serialize to string
                        "lastUpdated": item.get("lastUpdated"),
                    }
                    records.append(record)
            
            print(f"Extracted {len(records)} live records from {len(park_ids)} parks")
            return records
    
    # Run the async function synchronously
    return asyncio.run(fetch_live_data())

