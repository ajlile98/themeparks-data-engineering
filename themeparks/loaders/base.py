"""
Loader Base Module

Defines the interface for all loaders.
Loaders handle writing data to target systems.
"""

from abc import ABC, abstractmethod
import pandas as pd


class Loader(ABC):
    """
    Base interface for loaders.
    
    Loaders should be stateless and handle writing DataFrames to targets.
    """
    
    @abstractmethod
    def load(self, df: pd.DataFrame, **kwargs) -> None:
        """
        Load DataFrame to target.
        
        Args:
            df: DataFrame to load
            **kwargs: Configuration parameters
        """
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the loader for logging."""
        pass
