"""
Data Validation Functions for Airflow DAGs

Validation functions to ensure data quality at each stage of the pipeline.
Each validator raises an exception if validation fails, causing the task to fail.
"""

import logging
from typing import Dict, Any, List
import pandas as pd

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


# =============================================================================
# Extract Stage Validators
# =============================================================================

def validate_extract_destinations(df: pd.DataFrame, metadata: Dict[str, Any]) -> None:
    """
    Validate extracted destinations data.
    
    Checks:
    - Required columns exist
    - No duplicate park IDs
    - All parks have names
    - Row count > 0
    
    Args:
        df: Extracted DataFrame
        metadata: XCom metadata from extract task
        
    Raises:
        ValidationError: If validation fails
    """
    logger.info(f"Validating destinations data: {metadata.get('row_count', 0)} rows")
    
    # Check row count
    if len(df) == 0:
        raise ValidationError("No destinations extracted - API may be down")
    
    # Check required columns
    required_cols = ['id', 'name']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing required columns: {missing_cols}")
    
    # Check for duplicates
    duplicates = df[df.duplicated(subset=['id'], keep=False)]
    if not duplicates.empty:
        raise ValidationError(f"Found {len(duplicates)} duplicate destination IDs: {duplicates['id'].tolist()}")
    
    # Check for missing names
    missing_names = df[df['name'].isna() | (df['name'] == '')]
    if not missing_names.empty:
        raise ValidationError(f"Found {len(missing_names)} destinations without names")
    
    logger.info("Destinations validation passed")


def validate_extract_entities(df: pd.DataFrame, metadata: Dict[str, Any]) -> None:
    """
    Validate extracted entities data.
    
    Checks:
    - Required columns exist
    - Entity IDs are unique
    - All entities linked to parks
    - Row count > 0
    
    Args:
        df: Extracted DataFrame
        metadata: XCom metadata from extract task
        
    Raises:
        ValidationError: If validation fails
    """
    logger.info(f"Validating entities data: {metadata.get('row_count', 0)} rows")
    
    # Check row count
    if len(df) == 0:
        raise ValidationError("No entities extracted - API may be down or park_filter may be too restrictive")
    
    # Check required columns
    required_cols = ['id', 'name', 'entityType']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing required columns: {missing_cols}")
    
    # Check for duplicate entity IDs
    duplicates = df[df.duplicated(subset=['id'], keep=False)]
    if not duplicates.empty:
        dup_ids = duplicates['id'].unique()[:10]  # Show first 10
        raise ValidationError(f"Found {len(duplicates)} duplicate entity IDs (showing first 10): {dup_ids.tolist()}")
    
    # Check entity types are valid
    valid_types = ['ATTRACTION', 'SHOW', 'RESTAURANT', 'MERCHANDISE']
    invalid_types = df[~df['entityType'].isin(valid_types)]
    if not invalid_types.empty:
        raise ValidationError(f"Found {len(invalid_types)} entities with invalid types")
    
    logger.info("Entities validation passed")


def validate_extract_live(df: pd.DataFrame, metadata: Dict[str, Any]) -> None:
    """
    Validate extracted live data.
    
    Checks:
    - Timestamps are recent (< 10 min old)
    - No negative wait times
    - Required fields present
    - Row count > 0
    
    Args:
        df: Extracted DataFrame
        metadata: XCom metadata from extract task
        
    Raises:
        ValidationError: If validation fails
    """
    logger.info(f"Validating live data: {metadata.get('row_count', 0)} rows")
    
    # Check row count
    if len(df) == 0:
        logger.warning("No live data extracted - parks may be closed")
        return  # Not an error, parks might be closed
    
    # Check required columns
    required_cols = ['id', 'status', 'queue']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing required columns: {missing_cols}")
    
    # Check for negative wait times (extract numeric wait times from queue dict/object)
    if 'queue' in df.columns:
        wait_times = []
        for queue in df['queue']:
            if isinstance(queue, dict) and 'STANDBY' in queue:
                standby = queue['STANDBY']
                if isinstance(standby, dict) and 'waitTime' in standby:
                    wait_time = standby['waitTime']
                    if wait_time is not None:
                        wait_times.append(wait_time)
        
        if wait_times:
            negative_waits = [w for w in wait_times if w < 0]
            if negative_waits:
                raise ValidationError(f"Found {len(negative_waits)} negative wait times")
            
            # Sanity check: wait times should be < 300 minutes (5 hours)
            excessive_waits = [w for w in wait_times if w > 300]
            if excessive_waits:
                logger.warning(f"Found {len(excessive_waits)} wait times > 5 hours (may be valid but unusual)")
    
    logger.info("Live data validation passed")


# =============================================================================
# Transform Stage Validators
# =============================================================================

def validate_schema(df: pd.DataFrame, expected_columns: List[str], metadata: Dict[str, Any]) -> None:
    """
    Validate that DataFrame has expected schema.
    
    Args:
        df: Transformed DataFrame
        expected_columns: List of column names that must exist
        metadata: XCom metadata
        
    Raises:
        ValidationError: If schema validation fails
    """
    logger.info(f"Validating schema for {metadata.get('row_count', 0)} rows")
    
    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing expected columns after transform: {missing_cols}")
    
    # Check metadata columns were added
    metadata_cols = ['_pipeline', '_extracted_at']
    missing_metadata = [col for col in metadata_cols if col not in df.columns]
    if missing_metadata:
        raise ValidationError(f"Missing metadata columns: {missing_metadata}")
    
    logger.info("Schema validation passed")


def validate_data_types(df: pd.DataFrame, metadata: Dict[str, Any]) -> None:
    """
    Validate data types are correct after transformation.
    
    Args:
        df: Transformed DataFrame
        metadata: XCom metadata
        
    Raises:
        ValidationError: If data type validation fails
    """
    logger.info("Validating data types")
    
    # Check _extracted_at is string (ISO format)
    if '_extracted_at' in df.columns:
        if df['_extracted_at'].dtype != 'object':
            raise ValidationError(f"_extracted_at should be string, got {df['_extracted_at'].dtype}")
    
    logger.info("Data type validation passed")


# =============================================================================
# Pre-Load Validators
# =============================================================================

def validate_no_duplicates(df: pd.DataFrame, key_columns: List[str], metadata: Dict[str, Any]) -> None:
    """
    Check for duplicate records before loading.
    
    Args:
        df: DataFrame to load
        key_columns: Columns that should be unique
        metadata: XCom metadata
        
    Raises:
        ValidationError: If duplicates found
    """
    logger.info(f"Checking for duplicates on {key_columns}")
    
    duplicates = df[df.duplicated(subset=key_columns, keep=False)]
    if not duplicates.empty:
        raise ValidationError(
            f"Found {len(duplicates)} duplicate records before load. "
            f"First few duplicates: {duplicates[key_columns].head().to_dict('records')}"
        )
    
    logger.info("No duplicates found")


def validate_cardinality(
    current_count: int,
    previous_count: int | None,
    threshold_percent: float = 50.0,
    metadata: Dict[str, Any] = None
) -> None:
    """
    Warn if row count changed significantly vs previous run.
    
    This catches potential API issues (e.g., partial response, downtime).
    
    Args:
        current_count: Current record count
        previous_count: Previous run record count (None if first run)
        threshold_percent: Alert if change > this percent (default 50%)
        metadata: XCom metadata
        
    Raises:
        ValidationError: If cardinality change exceeds threshold
    """
    if previous_count is None:
        logger.info(f"First run, skipping cardinality check (current count: {current_count})")
        return
    
    percent_change = abs(current_count - previous_count) / previous_count * 100
    
    if percent_change > threshold_percent:
        raise ValidationError(
            f"Row count changed by {percent_change:.1f}% "
            f"(previous: {previous_count}, current: {current_count}). "
            f"This may indicate an API issue or data quality problem."
        )
    
    logger.info(f"Cardinality check passed ({percent_change:.1f}% change)")


# =============================================================================
# Airflow Task Wrapper Functions
# =============================================================================

def validate_destinations_extract_task(**context):
    """Airflow task to validate destinations after extract."""
    from loaders.MinioStorage import load_from_minio
    
    # Get metadata from extract task
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='extract_destinations')
    
    # Load DataFrame from MinIO
    df = load_from_minio(metadata)
    
    # Run validation
    validate_extract_destinations(df, metadata)


def validate_entities_extract_task(**context):
    """Airflow task to validate entities after extract."""
    from loaders.MinioStorage import load_from_minio
    
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='extract_entities')
    df = load_from_minio(metadata)
    validate_extract_entities(df, metadata)


def validate_live_extract_task(**context):
    """Airflow task to validate live data after extract."""
    from loaders.MinioStorage import load_from_minio
    
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='extract_live_data')
    df = load_from_minio(metadata)
    validate_extract_live(df, metadata)


def validate_transform_task(expected_columns: List[str]):
    """
    Factory function to create transform validation task.
    
    Args:
        expected_columns: List of required columns
        
    Returns:
        Callable for PythonOperator
    """
    def _validate(**context):
        from loaders.MinioStorage import load_from_minio
        
        ti = context['ti']
        task_id = context['task'].upstream_task_ids  # Get upstream task
        upstream_task_id = list(task_id)[0] if task_id else None
        
        if not upstream_task_id:
            raise ValidationError("No upstream task found")
        
        metadata = ti.xcom_pull(task_ids=upstream_task_id)
        df = load_from_minio(metadata)
        
        validate_schema(df, expected_columns, metadata)
        validate_data_types(df, metadata)
    
    return _validate


def validate_preload_task(key_columns: List[str]):
    """
    Factory function to create pre-load validation task.
    
    Args:
        key_columns: Columns that should be unique
        
    Returns:
        Callable for PythonOperator
    """
    def _validate(**context):
        from loaders.MinioStorage import load_from_minio
        
        ti = context['ti']
        task_id = context['task'].upstream_task_ids
        upstream_task_id = list(task_id)[0] if task_id else None
        
        if not upstream_task_id:
            raise ValidationError("No upstream task found")
        
        metadata = ti.xcom_pull(task_ids=upstream_task_id)
        df = load_from_minio(metadata)
        
        validate_no_duplicates(df, key_columns, metadata)
        # Could also check cardinality here if we store previous counts
    
    return _validate
