"""
Transformer Base Module

Defines the interface for all transformers.
Transformers are pure functions that add metadata and clean data.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import pandas as pd


class Transformer(ABC):
    """
    Base interface for transformers.
    
    Transformers should be stateless and return DataFrames.
    They handle only transformation logic - no extraction or loading.
    """
    
    @abstractmethod
    def transform(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Transform raw data.
        
        Args:
            df: Raw DataFrame from extractor
            **kwargs: Configuration parameters
            
        Returns:
            Transformed DataFrame with metadata
        """
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the transformer for logging."""
        pass
    
    def add_metadata(self, df: pd.DataFrame, pipeline_name: str) -> pd.DataFrame:
        """
        Add standard metadata columns to DataFrame.
        
        Args:
            df: DataFrame to add metadata to
            pipeline_name: Name of the pipeline
            
        Returns:
            DataFrame with metadata columns
        """
        df = df.copy()
        df["_pipeline"] = pipeline_name
        df["_extracted_at"] = datetime.now(timezone.utc).isoformat()
        return df
