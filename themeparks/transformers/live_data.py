"""
Live Data Transformer

Transforms raw live data by adding metadata and cleaning.
"""

import logging
import pandas as pd
from themeparks.transformers.base import Transformer

logger = logging.getLogger(__name__)


class LiveDataTransformer(Transformer):
    """Transforms live data."""
    
    @property
    def name(self) -> str:
        return "live_data"
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform live data.
        
        - Adds pipeline metadata
        - Cleans status values
        - Handles timestamps
        
        Args:
            df: Raw live data DataFrame
            **kwargs: Additional parameters
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transforming {len(df)} live data records")
        
        # Add metadata
        df = self.add_metadata(df, self.name)
        
        # Standardize status values
        if "status" in df.columns:
            df["status"] = df["status"].str.upper()
        
        # Remove null IDs
        if "id" in df.columns:
            df = df[df["id"].notna()]
            logger.info(f"Filtered to {len(df)} records with IDs")
        
        # Ensure consistent column order
        column_order = [
            "park_id",
            "id",
            "name",
            "entityType",
            "status",
            "queue",
            "lastUpdated",
            "_pipeline",
            "_extracted_at",
        ]
        existing_cols = [col for col in column_order if col in df.columns]
        df = df[existing_cols]
        
        logger.info(f"Transformation complete: {len(df)} records")
        return df


# Convenience function for CLI usage
def transform_live_data(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Transform live data.
    
    Args:
        df: Raw live data DataFrame
        **kwargs: Additional parameters
        
    Returns:
        Transformed DataFrame
    """
    transformer = LiveDataTransformer()
    return transformer.transform(df, **kwargs)
