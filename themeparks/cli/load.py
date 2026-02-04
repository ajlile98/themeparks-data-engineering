"""
Load Command

Loads transformed data to target systems (Kafka, etc.).
"""

import logging
import sys
import click
from themeparks.loaders import load_dataframe, load_to_kafka

logger = logging.getLogger(__name__)


@click.command()
@click.argument('target', type=click.Choice(['kafka'], case_sensitive=False))
@click.option('--input', '-i', required=True, help='Input URI (minio://bucket/path or /local/path)')
@click.option('--topic', help='Kafka topic name')
@click.option('--kafka-host', help='Kafka broker hostname (default: KAFKA_HOST env var)')
@click.option('--kafka-port', type=int, help='Kafka broker port (default: KAFKA_PORT env var)')
@click.option('--log-level', default='INFO', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']))
def load_command(target, input, topic, kafka_host, kafka_port, log_level):
    """
    Load data to target system.
    
    TARGET: Destination system (kafka)
    
    Examples:
    
        # Load to Kafka
        themeparks load kafka \\
            --input minio://bucket/transformed/destinations.parquet \\
            --topic themeparks.destinations
        
        # Load with custom Kafka broker
        themeparks load kafka \\
            --input /tmp/data.parquet \\
            --topic themeparks.entities \\
            --kafka-host kafka.example.com \\
            --kafka-port 9092
    """
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        logger.info(f"Starting load to: {target}")
        logger.info(f"Input: {input}")
        
        # Load data
        df = load_dataframe(input)
        logger.info(f"Loaded {len(df)} records")
        
        # Load to target
        if target == 'kafka':
            if not topic:
                logger.error("--topic is required for Kafka loading")
                sys.exit(1)
            
            logger.info(f"Loading to Kafka topic: {topic}")
            load_to_kafka(
                df,
                hostname=kafka_host,
                port=kafka_port,
                topic=topic
            )
        
        logger.info("Load complete")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Load failed: {e}", exc_info=True)
        sys.exit(1)
