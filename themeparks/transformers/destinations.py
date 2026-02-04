"""
Destinations Transformer

Transforms raw destinations data by adding metadata and cleaning.
"""

import logging
import pandas as pd
from themeparks.transformers.base import Transformer

logger = logging.getLogger(__name__)


class DestinationsTransformer(Transformer):
    """Transforms destinations data."""
    
    @property
    def name(self) -> str:
        return "destinations"
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform destinations data.
        
        - Adds pipeline metadata
        - Cleans column names
        - Handles null values
        
        Args:
            df: Raw destinations DataFrame
            **kwargs: Additional parameters
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transforming {len(df)} destination records")
        
        # Add metadata
        df = self.add_metadata(df, self.name)
        
        # Clean data: remove rows with no park_id (destinations without parks)
        if "park_id" in df.columns:
            df = df[df["park_id"].notna()]
            logger.info(f"Filtered to {len(df)} records with park IDs")
        
        # Ensure consistent column order
        column_order = [
            "destination_id",
            "destination_name",
            "destination_slug",
            "park_id",
            "park_name",
            "_pipeline",
            "_extracted_at",
        ]
        existing_cols = [col for col in column_order if col in df.columns]
        df = df[existing_cols]
        
        logger.info(f"Transformation complete: {len(df)} records")
        return df


# Convenience function for CLI usage
def transform_destinations(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Transform destinations data.
    
    Args:
        df: Raw destinations DataFrame
        **kwargs: Additional parameters
        
    Returns:
        Transformed DataFrame
    """
    transformer = DestinationsTransformer()
    return transformer.transform(df, **kwargs)
