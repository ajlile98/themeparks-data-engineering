
"""
## Theme Park ETL 

This DAG queries the list of theme parks entities available on themepark api

"""

from airflow.sdk import Asset, dag, task, asset, get_current_context


@asset(
    schedule="@daily", 
    params={
        "park_filter": 'disney'
    }
    )
def raw_theme_park_entities() -> list[dict]:
    """extracts theme park entities"""
    from include.extractors.themeparks import ThemeParksClient
    import asyncio
    
    ctx = get_current_context()

    park_filter = ctx['params']['park_filter'].lower().strip()

    async def fetch_entities():
        """Async function to fetch theme park entities"""
        async with ThemeParksClient() as client:
            # Get all destinations first
            data = await client.get_destinations()
            destinations = data.get("destinations", [])
            
            # Apply filter if specified
            if park_filter:
                destinations = [
                    d for d in destinations 
                    if park_filter in d.get("name", "").lower()
                ]
                print(f"Filtered to {len(destinations)} destinations")
            
            # Collect all park IDs
            park_ids = []
            for dest in destinations:
                parks = dest.get("parks", [])
                park_ids.extend([p.get("id") for p in parks if p.get("id")])
            
            print(f"Fetching entities for {len(park_ids)} parks")
            
            # Fetch entities for all parks in parallel
            tasks = [client.get_entity_children(park_id) for park_id in park_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Flatten results
            records = []
            for park_id, result in zip(park_ids, results):
                if isinstance(result, Exception):
                    print(f"Failed to get entities for park {park_id}: {result}")
                    continue
                
                entities = result.get("children", [])
                for entity in entities:
                    record = {
                        "park_id": park_id,
                        "id": entity.get("id"),
                        "name": entity.get("name"),
                        "entityType": entity.get("entityType"),
                    }
                    records.append(record)
            
            print(f"Extracted {len(records)} entities from {len(park_ids)} parks")
            return records
    
    # Run the async function synchronously
    return asyncio.run(fetch_entities())

