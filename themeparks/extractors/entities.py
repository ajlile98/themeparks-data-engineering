"""
Entities Extractor

Extracts entity metadata (attractions, shows, restaurants) from the API.
Pure extraction logic with no transformation.
"""

import asyncio
import logging
import pandas as pd
from themeparks.extractors.base import Extractor
from extractors.themeparks_client import ThemeparksClient

logger = logging.getLogger(__name__)


class EntitiesExtractor(Extractor):
    """Extracts entity metadata from themeparks.wiki API."""
    
    @property
    def name(self) -> str:
        return "entities"
    
    async def extract(self, park_filter: str | None = None, **kwargs) -> pd.DataFrame:
        """
        Extract entity metadata for all parks.
        
        Args:
            park_filter: Optional filter string (e.g., 'disney' to only get Disney parks)
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with entity metadata
        """
        logger.info(f"Starting entities extraction (filter: {park_filter})")
        
        async with ThemeparksClient() as client:
            # Get all destinations first
            data = await client.get_destinations()
            destinations = data.get("destinations", [])
            
            # Apply filter if specified
            if park_filter:
                destinations = [
                    d for d in destinations 
                    if park_filter.lower() in d.get("name", "").lower()
                ]
                logger.info(f"Filtered to {len(destinations)} destinations")
            
            # Collect all park IDs
            park_ids = []
            for dest in destinations:
                parks = dest.get("parks", [])
                park_ids.extend([p.get("id") for p in parks if p.get("id")])
            
            logger.info(f"Fetching entities for {len(park_ids)} parks")
            
            # Fetch entities for all parks in parallel
            tasks = [client.get_entity_children(park_id) for park_id in park_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Flatten results
            records = []
            for park_id, result in zip(park_ids, results):
                if isinstance(result, Exception):
                    logger.warning(f"Failed to get entities for park {park_id}: {result}")
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
            
            df = pd.DataFrame(records)
            logger.info(f"Extracted {len(df)} entities from {len(park_ids)} parks")
            return df


# Convenience function for CLI usage
async def extract_entities(park_filter: str | None = None, **kwargs) -> pd.DataFrame:
    """
    Extract entities data.
    
    Args:
        park_filter: Optional filter for park names
        **kwargs: Additional parameters
        
    Returns:
        DataFrame with entities data
    """
    extractor = EntitiesExtractor()
    return await extractor.extract(park_filter=park_filter, **kwargs)
