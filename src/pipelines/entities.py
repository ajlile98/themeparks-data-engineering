"""
Entities Pipeline

Extracts entity metadata (attractions, shows, restaurants) for all parks.
Run frequency: Daily (entity details change occasionally)
"""

import asyncio
import pandas as pd
from pipelines.base import BasePipeline
from extractors.themeparks_client import ThemeparksClient


class EntityPipeline(BasePipeline):
    """
    Pipeline for collecting entity metadata.
    
    Entities include attractions, shows, restaurants, and other
    points of interest within parks. This is reference data used
    to enrich live data.
    """
    
    name = "entities"
    
    def __init__(self, target, park_filter: str | None = None):
        """
        Initialize entity pipeline.
        
        Args:
            target: Load target (CSV, Parquet, etc.)
            park_filter: Optional filter string (e.g., 'disney' to only get Disney parks)
        """
        super().__init__(target)
        self.park_filter = park_filter
    
    async def extract(self) -> pd.DataFrame:
        """Extract entity metadata for all parks."""
        async with ThemeparksClient() as client:
            # Get all destinations first
            data = await client.get_destinations()
            destinations = data.get("destinations", [])
            
            # Apply filter if specified
            if self.park_filter:
                destinations = [
                    d for d in destinations 
                    if self.park_filter.lower() in d.get("name", "").lower()
                ]
            
            # Collect all park IDs
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
            
            print(f"[{self.name}] Fetching entities for {len(park_ids)} parks...")
            
            # Fetch children (entities) for all parks in parallel
            tasks = [client.get_entity_children(pid) for pid in park_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Flatten results
            all_entities = []
            for park_id, result in zip(park_ids, results):
                if isinstance(result, Exception):
                    print(f"[{self.name}] Error fetching {park_id}: {result}")
                    continue
                
                info = park_info.get(park_id, {})
                for entity in result.get("children", []):
                    entity["park_id"] = park_id
                    entity["park_name"] = info.get("park_name")
                    entity["destination_name"] = info.get("destination_name")
                    all_entities.append(entity)
                
                print(f"[{self.name}]   {info.get('park_name', park_id)}: {len(result.get('children', []))} entities")
            
            return pd.DataFrame.from_records(all_entities)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize entity data."""
        df = super().transform(df)
        
        # Rename id to entity_id for clarity
        if "id" in df.columns:
            df = df.rename(columns={"id": "entity_id"})
        
        # Standardize entity types to lowercase
        if "entityType" in df.columns:
            df["entityType"] = df["entityType"].str.lower()
        
        return df
