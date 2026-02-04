"""
Transform Command

Transforms raw data by adding metadata and cleaning.
"""

import logging
import sys
import click
from themeparks.loaders import load_dataframe, save_dataframe

logger = logging.getLogger(__name__)


@click.command()
@click.argument('resource', type=click.Choice(['destinations', 'entities', 'live'], case_sensitive=False))
@click.option('--input', '-i', required=True, help='Input URI (minio://bucket/path or /local/path)')
@click.option('--output', '-o', required=True, help='Output URI (minio://bucket/path or /local/path)')
@click.option('--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']))
def transform_command(resource, input, output, log_level):
    """
    Transform raw data.
    
    RESOURCE: Type of data to transform (destinations, entities, live)
    
    Examples:
    
        # Transform destinations
        themeparks transform destinations \\
            --input minio://bucket/raw/destinations.parquet \\
            --output minio://bucket/transformed/destinations.parquet
        
        # Transform entities from local file
        themeparks transform entities \\
            --input /tmp/entities_raw.parquet \\
            --output /tmp/entities_transformed.parquet
    """
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Import transformers
        from themeparks.transformers import (
            transform_destinations,
            transform_entities,
            transform_live_data
        )
        
        logger.info(f"Starting transformation: {resource}")
        logger.info(f"Input: {input}")
        logger.info(f"Output: {output}")
        
        # Load data
        df = load_dataframe(input)
        logger.info(f"Loaded {len(df)} records")
        
        # Run transformation
        if resource == 'destinations':
            df_transformed = transform_destinations(df)
        elif resource == 'entities':
            df_transformed = transform_entities(df)
        elif resource == 'live':
            df_transformed = transform_live_data(df)
        
        logger.info(f"Transformed to {len(df_transformed)} records with {len(df_transformed.columns)} columns")
        
        # Save to storage
        metadata = save_dataframe(df_transformed, output)
        logger.info(f"Saved to: {metadata['uri']}")
        logger.info(f"Size: {metadata['size_bytes']:,} bytes")
        
        # Print metadata as JSON
        import json
        print(json.dumps(metadata))
        
        logger.info("Transformation complete")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Transformation failed: {e}", exc_info=True)
        sys.exit(1)
