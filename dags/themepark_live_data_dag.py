
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
@task(inlets=Asset("raw_theme_park_entities"))
def raw_theme_park_live_data() -> list[dict]:
    """extracts theme park live data"""
    from include.extractors.themeparks import ThemeParksClient
    import asyncio
    
    ctx = get_current_context()

    async def fetch_live_data():
        """Async function to fetch theme park live data"""
        async with ThemeParksClient() as client:
            entities = ctx["ti"].xcom_pull(
                dag_id="raw_theme_park_entities",
                task_ids="raw_theme_park_entities",
                key="return_value",
                include_prior_dates=True,
            )

            if entities is None:
                entities = []

            park_ids = sorted({e.get("park_id") for e in entities if isinstance(e, dict) and e.get("park_id")})
            
            print(f"Fetching live data for {len(park_ids)} parks")
            
            tasks = [client.get_entity_live(park_id) for park_id in park_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
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
                        "queue": str(item.get("queue", {})),  
                        "lastUpdated": item.get("lastUpdated"),
                    }
                    records.append(record)
            
            print(f"Extracted {len(records)} live records from {len(park_ids)} parks")
            return records
    
    return asyncio.run(fetch_live_data())

