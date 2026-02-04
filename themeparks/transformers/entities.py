"""
Entities Transformer

Transforms raw entities data by adding metadata and cleaning.
"""

import logging
import pandas as pd
from themeparks.transformers.base import Transformer

logger = logging.getLogger(__name__)


class EntitiesTransformer(Transformer):
    """Transforms entities data."""
    
    @property
    def name(self) -> str:
        return "entities"
    
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform entities data.
        
        - Adds pipeline metadata
        - Cleans column names
        - Standardizes entity types
        
        Args:
            df: Raw entities DataFrame
            **kwargs: Additional parameters
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Transforming {len(df)} entity records")
        
        # Add metadata
        df = self.add_metadata(df, self.name)
        
        # Clean entity types (standardize to uppercase)
        if "entityType" in df.columns:
            df["entityType"] = df["entityType"].str.upper()
        
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
            "_pipeline",
            "_extracted_at",
        ]
        existing_cols = [col for col in column_order if col in df.columns]
        df = df[existing_cols]
        
        logger.info(f"Transformation complete: {len(df)} records")
        return df


# Convenience function for CLI usage
def transform_entities(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Transform entities data.
    
    Args:
        df: Raw entities DataFrame
        **kwargs: Additional parameters
        
    Returns:
        Transformed DataFrame
    """
    transformer = EntitiesTransformer()
    return transformer.transform(df, **kwargs)
