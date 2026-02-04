"""
Extractor Base Module

Defines the interface for all extractors.
Extractors are pure functions that fetch data from external sources.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List
import pandas as pd


class Extractor(ABC):
    """
    Base interface for extractors.
    
    Extractors should be stateless and return DataFrames.
    They handle only extraction logic - no transformation or loading.
    """
    
    @abstractmethod
    async def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract data from source.
        
        Args:
            **kwargs: Configuration parameters (filters, dates, etc.)
            
        Returns:
            DataFrame with raw extracted data
        """
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the extractor for logging."""
        pass
