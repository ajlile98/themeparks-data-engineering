"""
Validate Command

Validates data quality at various stages.
"""

import logging
import sys
import click
from themeparks.loaders import load_dataframe

logger = logging.getLogger(__name__)


@click.command()
@click.argument('resource', type=click.Choice(['destinations', 'entities', 'live'], case_sensitive=False))
@click.option('--input', '-i', required=True, help='Input URI (minio://bucket/path or /local/path)')
@click.option('--checks', help='Comma-separated list of checks to run (default: all)')
@click.option('--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']))
def validate_command(resource, input, checks, log_level):
    """
    Validate data quality.
    
    RESOURCE: Type of data to validate (destinations, entities, live)
    
    Examples:
    
        # Validate destinations data
        themeparks validate destinations --input minio://bucket/raw/destinations.parquet
        
        # Validate with specific checks
        themeparks validate entities \\
            --input /tmp/entities.parquet \\
            --checks row_count,duplicates
    """
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Import validators
        from themeparks.validators import (
            validate_destinations,
            validate_entities,
            validate_live_data,
            ValidationError
        )
        
        logger.info(f"Starting validation: {resource}")
        logger.info(f"Input: {input}")
        
        # Load data
        df = load_dataframe(input)
        logger.info(f"Loaded {len(df)} records")
        
        # Run validation
        if resource == 'destinations':
            validate_destinations(df)
        elif resource == 'entities':
            validate_entities(df)
        elif resource == 'live':
            validate_live_data(df)
        
        logger.info("Validation passed")
        sys.exit(0)
        
    except ValidationError as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Validation error: {e}", exc_info=True)
        sys.exit(2)
