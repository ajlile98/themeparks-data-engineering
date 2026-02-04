"""
Live Data Extractor

Extracts real-time wait times and status for all attractions.
Pure extraction logic with no transformation.
"""

import asyncio
import logging
import pandas as pd
from themeparks.extractors.base import Extractor
from extractors.themeparks_client import ThemeparksClient

logger = logging.getLogger(__name__)


class LiveDataExtractor(Extractor):
    """Extracts live wait time data from themeparks.wiki API."""
    
    @property
    def name(self) -> str:
        return "live_data"
    
    async def extract(self, park_filter: str | None = None, **kwargs) -> pd.DataFrame:
        """
        Extract live data for all parks.
        
        Args:
            park_filter: Optional filter string (e.g., 'disney')
            **kwargs: Additional parameters
            
        Returns:
            DataFrame with live data (wait times, status, etc.)
        """
        logger.info(f"Starting live data extraction (filter: {park_filter})")
        
        async with ThemeparksClient() as client:
            # Get destinations
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
            
            logger.info(f"Fetching live data for {len(park_ids)} parks")
            
            # Fetch live data for all parks in parallel
            tasks = [client.get_park_live(park_id) for park_id in park_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Flatten results
            records = []
            for park_id, result in zip(park_ids, results):
                if isinstance(result, Exception):
                    logger.warning(f"Failed to get live data for park {park_id}: {result}")
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
            
            df = pd.DataFrame(records)
            logger.info(f"Extracted {len(df)} live records from {len(park_ids)} parks")
            return df


# Convenience function for CLI usage
async def extract_live_data(park_filter: str | None = None, **kwargs) -> pd.DataFrame:
    """
    Extract live data.
    
    Args:
        park_filter: Optional filter for park names
        **kwargs: Additional parameters
        
    Returns:
        DataFrame with live data
    """
    extractor = LiveDataExtractor()
    return await extractor.extract(park_filter=park_filter, **kwargs)
