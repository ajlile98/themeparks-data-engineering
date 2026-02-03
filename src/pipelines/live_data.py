"""
Live Data Pipeline

Extracts real-time wait times, show times, and status for all attractions.
Run frequency: Every 5 minutes (data changes constantly)
"""

import pandas as pd
from pipelines.base import BasePipeline
from extractors.themeparks_client import ThemeparksClient


class LiveDataPipeline(BasePipeline):
    """
    Pipeline for collecting real-time park data.
    
    This includes wait times, attraction status, show times, and
    operating hours. Data changes frequently, so this should run
    every 5-15 minutes during park operating hours.
    """
    
    name = "live_data"
    
    def __init__(self, targets, park_filter: str | None = None):
        """
        Initialize live data pipeline.
        
        Args:
            targets: List of load targets (CSV, Parquet, etc.)
            park_filter: Optional filter string (e.g., 'disney' to only get Disney parks)
        """
        super().__init__(targets)
        self.park_filter = park_filter
    
    async def extract(self) -> pd.DataFrame:
        """Extract live data for all parks."""
        async with ThemeparksClient() as client:
            # Get destinations
            data = await client.get_destinations()
            destinations = data.get("destinations", [])
            
            # Apply filter if specified
            if self.park_filter:
                destinations = [
                    d for d in destinations 
                    if self.park_filter.lower() in d.get("name", "").lower()
                ]
            
            # Collect park info
            parks = []
            for dest in destinations:
                for park in dest.get("parks", []):
                    parks.append({
                        "park_id": park.get("id"),
                        "park_name": park.get("name"),
                        "destination_name": dest.get("name"),
                    })
            
            park_ids = [p["park_id"] for p in parks if p["park_id"]]
            park_info = {p["park_id"]: p for p in parks}
            
            print(f"[{self.name}] Fetching live data for {len(park_ids)} parks...")
            
            # Fetch live data for all parks in parallel
            results = await client.get_multiple_parks_live(park_ids)
            
            # Flatten results
            all_live_data = []
            for park_id, result in results.items():
                info = park_info.get(park_id, {})
                
                if not result.get("success"):
                    print(f"[{self.name}]   [ERROR] {info.get('park_name', park_id)}: {result.get('error')}")
                    continue
                
                live_items = result.get("liveData", [])
                for item in live_items:
                    item["park_id"] = park_id
                    item["park_name"] = info.get("park_name")
                    item["destination_name"] = info.get("destination_name")
                    all_live_data.append(item)
                
                print(f"[{self.name}]   [OK] {info.get('park_name', park_id)}: {len(live_items)} items")
            
            print(f"[{self.name}] Total: {len(all_live_data)} items")
            return pd.DataFrame.from_records(all_live_data)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and flatten key fields from live data."""
        df = super().transform(df)
        
        # Parse lastUpdated to date
        if "lastUpdated" in df.columns:
            df["lastUpdatedDate"] = pd.to_datetime(df["lastUpdated"]).dt.strftime("%Y-%m-%d")
        
        # Rename id to entity_id for clarity
        if "id" in df.columns:
            df = df.rename(columns={"id": "entity_id"})
        
        # Extract standby wait time from nested queue structure
        if "queue" in df.columns:
            df["wait_time_standby"] = df["queue"].apply(
                lambda q: q.get("STANDBY", {}).get("waitTime") if isinstance(q, dict) else None
            )
            df["wait_time_lightning_lane"] = df["queue"].apply(
                lambda q: q.get("LIGHTNING_LANE", {}).get("waitTime") if isinstance(q, dict) else None
            )
        
        # Standardize status to lowercase
        if "status" in df.columns:
            df["status"] = df["status"].str.lower()
        
        return df
