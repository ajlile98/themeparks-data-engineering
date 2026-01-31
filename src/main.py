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
from pathlib import Path

from pipelines import DestinationsPipeline, EntityPipeline, LiveDataPipeline
from loaders.load_target import CsvLoadTarget, ParquetLoadTarget


# Data directory structure
DATA_DIR = Path("./data")
BRONZE_DIR = DATA_DIR / "bronze"


def ensure_directories():
    """Create data directories if they don't exist."""
    (BRONZE_DIR / "destinations").mkdir(parents=True, exist_ok=True)
    (BRONZE_DIR / "entities").mkdir(parents=True, exist_ok=True)
    (BRONZE_DIR / "live").mkdir(parents=True, exist_ok=True)


def run_destinations():
    """Run destinations pipeline (weekly)."""
    target = CsvLoadTarget(str(BRONZE_DIR / "destinations" / "destinations.csv"))
    pipeline = DestinationsPipeline(target)
    return pipeline.run()


def run_entities(park_filter: str | None = None):
    """Run entities pipeline (daily)."""
    target = CsvLoadTarget(str(BRONZE_DIR / "entities" / "entities.csv"))
    pipeline = EntityPipeline(target, park_filter=park_filter)
    return pipeline.run()


def run_live_data(park_filter: str | None = None):
    """Run live data pipeline (every 5 min)."""
    target = ParquetLoadTarget(
        str(BRONZE_DIR / "live" / "live_data.parquet"),
        partition_cols=["lastUpdatedDate", "park_name"]
    )
    pipeline = LiveDataPipeline(target, park_filter=park_filter)
    return pipeline.run()


def run_all(park_filter: str | None = None):
    """Run all pipelines."""
    print("=" * 60)
    print("THEME PARKS DATA PIPELINE - Full Run")
    print("=" * 60)
    
    run_destinations()
    print()
    run_entities(park_filter)
    print()
    run_live_data(park_filter)
    
    print()
    print("=" * 60)
    print("All pipelines complete!")
    print("=" * 60)


def run_static(park_filter: str | None = None):
    """Run static data pipelines only (destinations + entities)."""
    print("=" * 60)
    print("THEME PARKS DATA PIPELINE - Static Data")
    print("=" * 60)
    
    run_destinations()
    print()
    run_entities(park_filter)
    
    print()
    print("=" * 60)
    print("Static pipelines complete!")
    print("=" * 60)


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
