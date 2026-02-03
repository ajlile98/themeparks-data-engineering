"""
Destinations Pipeline

Extracts theme park destinations (resorts) and their associated parks.
Run frequency: Weekly (data rarely changes)
"""

import logging
import pandas as pd
from pipelines.base import BasePipeline
from extractors.themeparks_client import ThemeparksClient

logger = logging.getLogger(__name__)


class DestinationsPipeline(BasePipeline):
    """
    Pipeline for collecting destination (resort) data.
    
    This is relatively static data - destinations and their parks
    don't change often. Run weekly or on-demand.
    """
    
    name = "destinations"
    
    async def extract(self) -> pd.DataFrame:
        """Extract all destinations and their parks."""
        async with ThemeparksClient() as client:
            data = await client.get_destinations()
            destinations = data.get("destinations", [])
            
            logger.info(f"Found {len(destinations)} destinations")
            
            # Flatten: one row per park (with destination info)
            records = []
            for dest in destinations:
                dest_info = {
                    "destination_id": dest.get("id"),
                    "destination_name": dest.get("name"),
                    "destination_slug": dest.get("slug"),
                }
                
                parks = dest.get("parks", [])
                if parks:
                    for park in parks:
                        record = {
                            **dest_info,
                            "park_id": park.get("id"),
                            "park_name": park.get("name"),
                        }
                        records.append(record)
                else:
                    # Destination with no parks
                    records.append({**dest_info, "park_id": None, "park_name": None})
            
            return pd.DataFrame.from_records(records)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata columns."""
        df = super().transform(df)
        return df
