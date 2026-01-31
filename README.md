# Theme Parks Data Engineering

A data engineering pipeline for collecting and analyzing theme park wait times from the [themeparks.wiki API](https://api.themeparks.wiki/docs). Built with Python, using async HTTP requests for parallel data fetching and Apache Airflow for orchestration.

## Overview

This project implements a medallion architecture (Bronze/Silver/Gold) for ingesting theme park data:

- **Bronze Layer**: Raw data from API (destinations, entities, live wait times)
- **Silver Layer**: Cleaned and normalized data (planned)
- **Gold Layer**: Aggregated analytics (planned)

## Features

- Async API client with parallel fetching across multiple parks
- Modular pipeline architecture with pluggable load targets (CSV, Parquet)
- Airflow DAGs for scheduled extraction
- Park filtering (e.g., Disney parks only)

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Apache Airflow 3.x (optional, for orchestration)

## Installation

```bash
# Clone the repository
git clone https://github.com/your-username/themeparks-data-engineering.git
cd themeparks-data-engineering

# Install dependencies with uv
uv sync
```

## Usage

### Command Line

```bash
# Run all pipelines (static + live data)
uv run python src/main.py

# Run only static data (destinations + entities)
uv run python src/main.py --static

# Run only live wait times
uv run python src/main.py --live

# Filter to specific parks
uv run python src/main.py --live --filter disney
```

### Python API

```python
import asyncio
from themeparks_client import ThemeparksClient

async def main():
    async with ThemeparksClient() as client:
        # Get all destinations
        destinations = await client.get_destinations()
        
        # Get live data for multiple parks in parallel
        park_ids = ["park-id-1", "park-id-2"]
        live_data = await client.get_multiple_parks_live(park_ids)

asyncio.run(main())
```

## Project Structure

```
themeparks-data-engineering/
├── src/
│   ├── themeparks_client.py    # Async API client
│   ├── main.py                 # CLI entry point
│   ├── pipelines/
│   │   ├── base.py             # Abstract base pipeline
│   │   ├── destinations.py     # Destinations pipeline
│   │   ├── entities.py         # Entities pipeline
│   │   └── live_data.py        # Live wait times pipeline
│   └── loaders/
│       └── load_target.py      # CSV and Parquet targets
├── dags/
│   └── themeparks_dag.py       # Airflow DAG definitions
├── data/
│   └── bronze/                 # Raw data output
├── pyproject.toml
└── README.md
```

## Airflow Integration

Three DAGs are provided:

| DAG | Schedule | Description |
|-----|----------|-------------|
| `themeparks_static_daily` | 6 AM daily | Destinations and entity metadata |
| `themeparks_live_frequent` | Every 5 min (8 AM - 11 PM) | Live wait times |
| `themeparks_full_backfill` | Manual | Run all pipelines on demand |

### Setup Airflow (WSL/Linux)

```bash
# Create virtual environment
python -m venv ~/.airflow-env
source ~/.airflow-env/bin/activate

# Install Airflow and dependencies
pip install apache-airflow pandas pyarrow httpx python-dotenv
pip install PyJWT==2.10.1  # Downgrade to fix auth issue

# Initialize database
airflow db init

# Copy DAG file
cp dags/themeparks_dag.py ~/airflow/dags/

# Set environment variables
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_JWT_ISSUER="airflow"
export PYTHONPATH="/path/to/themeparks-data-engineering/src:$PYTHONPATH"

# Start Airflow
airflow standalone
```

Access the UI at http://localhost:8080

## Configuration

Set the data output directory via environment variable:

```bash
export THEMEPARKS_DATA_DIR="/path/to/data"
```

Defaults to the project's `data/` directory.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/destinations` | List all theme park destinations |
| `/entity/{id}` | Get details for a specific entity |
| `/entity/{id}/children` | Get child entities (attractions, shows) |
| `/entity/{id}/live` | Get live wait times and status |
| `/entity/{id}/schedule` | Get operating schedules |

## License

MIT
