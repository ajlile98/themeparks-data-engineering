


import asyncio
import httpx
from loaders.load_target import Loader
from extractors.themeparks_client import ThemeparksClient
import pandas as pd


class ThemeParkPipeline:

    def __init__(self, target: Loader):
        self.target: Loader = target

    async def extract(self) -> pd.DataFrame:
        async with ThemeparksClient() as client:
            # Get Disney destinations
            destinations = (await client.get_destinations()).get("destinations", [])
            destinations = [d for d in destinations if 'disney' in d.get('name', '').lower()]
            parks = [park for dest in destinations for park in dest.get("parks", [])]
            
            park_ids = [park.get("id") for park in parks if park.get("id")]
            park_names = {park.get("id"): park.get("name") for park in parks}
            
            print(f"\nFetching {len(park_ids)} Disney parks in parallel...")
            
            # Fetch all parks in parallel - the async magic!
            results = await client.get_multiple_parks_live(park_ids)
            
            # Convert to DataFrame
            all_live_data = []
            for park_id, data in results.items():
                if data.get("success"):
                    park_name = park_names.get(park_id, "Unknown")
                    live_items = data.get("liveData", [])
                    for item in live_items:
                        item["park_id"] = park_id
                        item["park_name"] = park_name
                    all_live_data.extend(live_items)
                    print(f"  [OK] {park_name}: {len(live_items)} items")
                else:
                    print(f"  [ERROR] {park_names.get(park_id, park_id)}: {data.get('error')}")
            
            print(f"\nTotal: {len(all_live_data)} items from {len(results)} parks")
            return pd.DataFrame.from_records(all_live_data)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df["lastUpdatedDate"] = pd.to_datetime(df["lastUpdated"]).dt.strftime("%Y-%m-%d")
        return df
    
    def load(self, data: pd.DataFrame):
        self.target.load(data)

    def run(self):
        """Sync entry point - runs the async pipeline."""
        df = asyncio.run(self.extract())
        df = self.transform(df)
        self.load(df)