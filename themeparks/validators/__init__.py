"""
Validators Module

Data quality validation functions for each data type.
"""

import logging
from typing import List
import pandas as pd

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


def validate_destinations(df: pd.DataFrame) -> None:
    """
    Validate destinations data.
    
    Checks:
    - Row count > 0
    - No duplicate park IDs
    - All parks have names
    - Required columns present
    
    Args:
        df: Destinations DataFrame
        
    Raises:
        ValidationError: If validation fails
    """
    logger.info(f"Validating destinations data: {len(df)} rows")
    
    # Check row count
    if len(df) == 0:
        raise ValidationError("No destinations extracted - API may be down")
    
    # Check required columns
    required_cols = ['destination_id', 'park_id', 'park_name']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing required columns: {missing_cols}")
    
    # Check for duplicates
    duplicates = df[df.duplicated(subset=['park_id'], keep=False)]
    if not duplicates.empty:
        raise ValidationError(f"Found {len(duplicates)} duplicate park IDs")
    
    # Check for missing names
    missing_names = df[df['park_name'].isna() | (df['park_name'] == '')]
    if not missing_names.empty:
        raise ValidationError(f"Found {len(missing_names)} parks without names")
    
    logger.info("Destinations validation passed")


def validate_entities(df: pd.DataFrame) -> None:
    """
    Validate entities data.
    
    Checks:
    - Row count > 0
    - Entity IDs are unique
    - Valid entity types
    - Required columns present
    
    Args:
        df: Entities DataFrame
        
    Raises:
        ValidationError: If validation fails
    """
    logger.info(f"Validating entities data: {len(df)} rows")
    
    # Check row count
    if len(df) == 0:
        raise ValidationError("No entities extracted")
    
    # Check required columns
    required_cols = ['id', 'name', 'entityType']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing required columns: {missing_cols}")
    
    # Check for duplicate entity IDs
    duplicates = df[df.duplicated(subset=['id'], keep=False)]
    if not duplicates.empty:
        raise ValidationError(f"Found {len(duplicates)} duplicate entity IDs")
    
    # Check entity types are valid
    valid_types = ['ATTRACTION', 'SHOW', 'RESTAURANT', 'MERCHANDISE']
    invalid_types = df[~df['entityType'].isin(valid_types)]
    if not invalid_types.empty:
        logger.warning(f"Found {len(invalid_types)} entities with non-standard types")
    
    logger.info("Entities validation passed")


def validate_live_data(df: pd.DataFrame) -> None:
    """
    Validate live data.
    
    Checks:
    - Required fields present
    - No missing IDs
    - Valid status values
    
    Args:
        df: Live data DataFrame
        
    Raises:
        ValidationError: If validation fails
    """
    logger.info(f"Validating live data: {len(df)} rows")
    
    # Allow empty (parks may be closed)
    if len(df) == 0:
        logger.warning("No live data extracted - parks may be closed")
        return
    
    # Check required columns
    required_cols = ['id', 'status']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing required columns: {missing_cols}")
    
    # Check for missing IDs
    missing_ids = df[df['id'].isna()]
    if not missing_ids.empty:
        raise ValidationError(f"Found {len(missing_ids)} records without IDs")
    
    # Check valid status values
    valid_statuses = ['OPERATING', 'DOWN', 'CLOSED', 'REFURBISHMENT']
    if 'status' in df.columns:
        invalid_statuses = df[~df['status'].isin(valid_statuses)]
        if not invalid_statuses.empty:
            logger.warning(f"Found {len(invalid_statuses)} records with non-standard status values")
    
    logger.info("Live data validation passed")


def validate_schema(df: pd.DataFrame, expected_columns: List[str]) -> None:
    """
    Validate DataFrame has expected schema.
    
    Args:
        df: DataFrame to validate
        expected_columns: List of required column names
        
    Raises:
        ValidationError: If schema validation fails
    """
    logger.info(f"Validating schema for {len(df)} rows")
    
    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing expected columns: {missing_cols}")
    
    logger.info("Schema validation passed")


def validate_no_duplicates(df: pd.DataFrame, key_columns: List[str]) -> None:
    """
    Check for duplicate records.
    
    Args:
        df: DataFrame to check
        key_columns: Columns that should be unique
        
    Raises:
        ValidationError: If duplicates found
    """
    logger.info(f"Checking for duplicates on {key_columns}")
    
    duplicates = df[df.duplicated(subset=key_columns, keep=False)]
    if not duplicates.empty:
        raise ValidationError(f"Found {len(duplicates)} duplicate records")
    
    logger.info("No duplicates found")
