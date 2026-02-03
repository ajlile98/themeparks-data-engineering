"""
Theme Parks Data Engineering Pipeline

Orchestrates ETL pipelines for collecting theme park data:
- Destinations: Resort and park metadata (weekly)
- Entities: Attraction/show details (daily)  
- Live Data: Real-time wait times (every 5 min)

Usage:
    python main.py              # Run all pipelines
    python main.py --live       # Run live data only
    python main.py --static     # Run destinations + entities only
"""

import argparse
import logging
from pathlib import Path

from pipelines import DestinationsPipeline, EntityPipeline, LiveDataPipeline
from loaders import CsvLoader, KafkaLoader, ParquetLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


# Data directory structure
DATA_DIR = Path("./data")
BRONZE_DIR = DATA_DIR / "bronze"


def ensure_directories():
    """Create data directories if they don't exist."""
    (BRONZE_DIR / "destinations").mkdir(parents=True, exist_ok=True)
    (BRONZE_DIR / "entities").mkdir(parents=True, exist_ok=True)
    (BRONZE_DIR / "live").mkdir(parents=True, exist_ok=True)
    logger.debug(f"Ensured data directories exist at {BRONZE_DIR}")


def run_destinations():
    """Run destinations pipeline (weekly)."""
    targets = [CsvLoader(str(BRONZE_DIR / "destinations" / "destinations.csv"))]
    pipeline = DestinationsPipeline(targets)
    return pipeline.run()


def run_entities(park_filter: str | None = None):
    """Run entities pipeline (daily)."""
    targets = [CsvLoader(str(BRONZE_DIR / "entities" / "entities.csv"))]
    pipeline = EntityPipeline(targets, park_filter=park_filter)
    return pipeline.run()


def run_live_data(park_filter: str | None = None):
    """Run live data pipeline (every 5 min)."""
    targets = [
        ParquetLoader(
            str(BRONZE_DIR / "live" / "live_data.parquet"),
            partition_cols=["lastUpdatedDate", "park_name"]
        ),
        KafkaLoader(
            "10.0.1.196",
            9094,
            "theme-park"
        )
    ]
    pipeline = LiveDataPipeline(targets, park_filter=park_filter)
    return pipeline.run()


def run_all(park_filter: str | None = None):
    """Run all pipelines."""
    logger.info("="*60)
    logger.info("THEME PARKS DATA PIPELINE - Full Run")
    logger.info("="*60)
    
    run_destinations()
    run_entities(park_filter)
    run_live_data(park_filter)
    
    logger.info("="*60)
    logger.info("All pipelines complete!")
    logger.info("="*60)


def run_static(park_filter: str | None = None):
    """Run static data pipelines only (destinations + entities)."""
    logger.info("="*60)
    logger.info("THEME PARKS DATA PIPELINE - Static Data")
    logger.info("="*60)
    
    run_destinations()
    run_entities(park_filter)
    
    logger.info("="*60)
    logger.info("Static pipelines complete!")
    logger.info("="*60)


def main():
    parser = argparse.ArgumentParser(description="Theme Parks Data Pipeline")
    parser.add_argument("--live", action="store_true", help="Run live data pipeline only")
    parser.add_argument("--static", action="store_true", help="Run static data pipelines only")
    parser.add_argument("--filter", type=str, default="disney", help="Filter parks (default: disney)")
    
    args = parser.parse_args()
    
    ensure_directories()
    
    if args.live:
        run_live_data(args.filter)
    elif args.static:
        run_static(args.filter)
    else:
        run_all(args.filter)


if __name__ == "__main__":
    main()
