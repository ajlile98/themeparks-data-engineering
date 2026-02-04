"""
Destinations Extractor

Extracts theme park destinations and their parks from the API.
Pure extraction logic with no transformation.
"""

import logging
import pandas as pd
from themeparks.extractors.base import Extractor
from extractors.themeparks_client import ThemeparksClient

logger = logging.getLogger(__name__)


class DestinationsExtractor(Extractor):
    """Extracts destination and park data from themeparks.wiki API."""
    
    @property
    def name(self) -> str:
        return "destinations"
    
    async def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract all destinations and their parks.
        
        Returns:
            DataFrame with columns:
            - destination_id
            - destination_name
            - destination_slug
            - park_id
            - park_name
        """
        logger.info("Starting destinations extraction")
        
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
            
            df = pd.DataFrame(records)
            logger.info(f"Extracted {len(df)} destination/park records")
            return df


# Convenience function for CLI usage
async def extract_destinations(**kwargs) -> pd.DataFrame:
    """
    Extract destinations data.
    
    Args:
        **kwargs: Optional parameters (none currently used)
        
    Returns:
        DataFrame with destinations data
    """
    extractor = DestinationsExtractor()
    return await extractor.extract(**kwargs)
