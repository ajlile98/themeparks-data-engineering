
"""
## Theme Park ETL 

This DAG queries the list of theme parks entities available on themepark api

"""

from airflow.sdk import Asset, dag, task, asset, get_current_context


@asset(
    schedule=[Asset("raw_theme_park_destinations")] 
    )
def raw_theme_park_entities(context: dict) -> list[dict]:
    """extracts theme park entities"""
    from include.extractors.themeparks import ThemeParksClient
    import asyncio
    
    async def fetch_entities():
        """Async function to fetch theme park entities"""
        async with ThemeParksClient() as client:
            destinations = context["ti"].xcom_pull(
                dag_id="raw_theme_park_destinations",
                task_ids="raw_theme_park_destinations",
                key="return_value",
                include_prior_dates=True,
                )
            
            print(f"Count of destinations from asset: {len(destinations)}")
            print(destinations)
                
            park_ids = []
            for dest in destinations:
                parks = dest.get("parks", [])
                park_ids.extend([p.get("id") for p in parks if p.get("id")])
            
            print(f"Fetching entities for {len(park_ids)} parks")
            
            tasks = [client.get_entity_children(park_id) for park_id in park_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
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
    
    return asyncio.run(fetch_entities())

