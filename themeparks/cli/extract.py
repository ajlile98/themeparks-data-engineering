"""
Extract Command

Extracts data from the themeparks.wiki API and saves to storage.
"""

import asyncio
import logging
import sys
import click
from themeparks.loaders import save_dataframe

logger = logging.getLogger(__name__)


@click.command()
@click.argument('resource', type=click.Choice(['destinations', 'entities', 'live'], case_sensitive=False))
@click.option('--output', '-o', required=True, help='Output URI (minio://bucket/path or /local/path)')
@click.option('--park-filter', help='Filter parks by name (e.g., "disney")')
@click.option('--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']))
def extract_command(resource, output, park_filter, log_level):
    """
    Extract data from themeparks.wiki API.
    
    RESOURCE: Type of data to extract (destinations, entities, live)
    
    Examples:
    
        # Extract destinations to MinIO
        themeparks extract destinations --output minio://bucket/raw/destinations.parquet
        
        # Extract Disney entities to local file
        themeparks extract entities --output /tmp/entities.parquet --park-filter disney
        
        # Extract live data
        themeparks extract live --output minio://bucket/raw/live.parquet
    """
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Import extractors
        from themeparks.extractors import (
            extract_destinations,
            extract_entities,
            extract_live_data
        )
        
        logger.info(f"Starting extraction: {resource}")
        logger.info(f"Output: {output}")
        if park_filter:
            logger.info(f"Park filter: {park_filter}")
        
        # Run extraction
        # TODO - maybe bad design. need to update if new resource types are available. factory pattern?
        if resource == 'destinations':
            df = asyncio.run(extract_destinations())
        elif resource == 'entities':
            df = asyncio.run(extract_entities(park_filter=park_filter))
        elif resource == 'live':
            df = asyncio.run(extract_live_data(park_filter=park_filter))
        
        logger.info(f"Extracted {len(df)} records with {len(df.columns)} columns")
        
        # Save to storage
        metadata = save_dataframe(df, output)
        logger.info(f"Saved to: {metadata['uri']}")
        logger.info(f"Size: {metadata['size_bytes']:,} bytes")
        
        # Print metadata as JSON for consumption by Airflow
        import json
        print(json.dumps(metadata))
        
        logger.info("Extraction complete")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        sys.exit(1)
