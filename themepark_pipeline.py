


import httpx
from loaders.load_target import LoadTarget
from extractors.themeparks_client import ThemeparksClient
import pandas as pd
import logging

class ThemeParkPipeline:

    def __init__(self, target):
        self.target = target

    def extract(self) -> pd.DataFrame:
        with ThemeparksClient() as client:
            # Magic Kingdom ID from themeparks API
            magic_kingdom_id = "75ea578a-adc8-4116-a54d-dccb60765ef9"
        
            print("\n" + "=" * 60)
            print("Fetching live wait times for Magic Kingdom...")
            print("=" * 60)
        
            try:
                live_data = client.get_entity_live(magic_kingdom_id)
                live_items = live_data.get("liveData", [])
                df = pd.DataFrame.from_dict(live_items)
            except httpx.HTTPStatusError as e:
                print(f"Error fetching live data: {e}")
                return []
        
            print("\n" + "=" * 60)
            print("Export complete!")
            print("=" * 60)
            return live_items

    def transform(self, live_items) -> pd.DataFrame:
        return live_items
    
    def load(self, data: pd.DataFrame):

        self.target.load(data)
        
        return
    
    def run(self):

        df: pd.DataFrame = self.extract()
        data = self.transform(data)
        self.load(data)

        return