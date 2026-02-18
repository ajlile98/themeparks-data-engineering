# Theme Parks Data Engineering

A real-time ETL pipeline for collecting and processing theme park data from the [ThemeParks.wiki API](https://api.themeparks.wiki/). This project uses Apache Airflow to orchestrate data collection for destinations, park entities, and live wait times across major theme parks worldwide.

## Overview

This data engineering pipeline collects three types of data from theme parks:

1. **Destinations** - Theme park resort locations and their parks (daily)
2. **Entities** - Attractions, shows, and facilities within each park (daily)
3. **Live Data** - Real-time wait times and operational status (every 5 minutes)

The pipeline uses asynchronous Python for efficient parallel API calls and can be filtered to specific park chains (e.g., Disney, Universal).

## Architecture

- **Orchestration**: Apache Airflow (Astronomer Runtime)
- **Data Collection**: Async HTTP client (`httpx`) for parallel API requests
- **Scheduling**: Asset-based DAGs with configurable intervals
- **Data Formats**: JSON with extensible loader framework (CSV, Parquet, Iceberg, Kafka, MinIO)

## ETL Pipeline

### DAGs

#### 1. Theme Park Destinations (`themepark_destination_dag.py`)
- **Schedule**: Daily (`@daily`)
- **Purpose**: Fetches all theme park destinations and their associated parks
- **Output**: Destinations list with park hierarchies
- **Asset**: `raw_theme_park_destinations`

#### 2. Theme Park Entities (`themepark_entities_dag.py`)
- **Schedule**: Daily (`@daily`)
- **Purpose**: Fetches all attractions, shows, and facilities within parks
- **Parameters**: 
  - `park_filter` (default: `'disney'`) - Filter destinations by name
- **Output**: Flattened entity records with park associations
- **Asset**: `raw_theme_park_entities`
- **Features**:
  - Parallel fetching of entity data for multiple parks
  - Error handling for individual park failures
  - Extracts entity ID, name, type, and park ID

#### 3. Theme Park Live Data (`themepark_live_data_dag.py`)
- **Schedule**: Every 5 minutes (`*/5 * * * *`)
- **Purpose**: Fetches real-time wait times and operational status
- **Parameters**:
  - `park_filter` (default: `'disney'`) - Filter destinations by name
- **Output**: Live status records with queue information
- **Asset**: `raw_theme_park_live_data`
- **Features**:
  - High-frequency data collection for real-time analytics
  - Captures wait times, ride status, and last updated timestamps
  - Parallel processing across all filtered parks

### Data Extractors

#### ThemeParks API Client (`include/extractors/themeparks.py`)

Async HTTP client for efficient API interaction:

**Key Methods**:
- `get_destinations()` - Fetch all theme park resorts
- `get_entity_children(entity_id)` - Get attractions within a park
- `get_entity_live(entity_id)` - Get real-time wait times and status
- `get_multiple_parks_live(park_ids)` - Batch fetch live data in parallel

**Features**:
- Async context manager for connection pooling
- Parallel request processing with `asyncio.gather()`
- Comprehensive error handling and logging
- 30-second default timeout

### Data Loaders

The `include/loaders/` directory contains extensible loader implementations:

- **CsvLoader** - Export to CSV files
- **ParquetLoader** - Export to Parquet format
- **IcebergLoader** - Load to Apache Iceberg tables
- **KafkaLoader** - Stream to Kafka topics
- **MinioLoader** - Upload to MinIO object storage
- **Loader** - Base loader interface

## Project Structure

```
themeparks-data-engineering/
├── dags/                              # Airflow DAG definitions
│   ├── themepark_destination_dag.py   # Destinations ETL
│   ├── themepark_entities_dag.py      # Entities ETL
│   └── themepark_live_data_dag.py     # Live data ETL (5-min)
├── include/
│   ├── extractors/
│   │   └── themeparks.py              # Async API client
│   └── loaders/                       # Data output handlers
│       ├── Loader.py                  # Base loader interface
│       ├── CsvLoader.py
│       ├── ParquetLoader.py
│       ├── IcebergLoader.py
│       ├── KafkaLoader.py
│       └── MinioLoader.py
├── tests/                             # Unit tests
├── Dockerfile                         # Astronomer Runtime image
├── requirements.txt                   # Python dependencies
├── packages.txt                       # OS-level packages
└── airflow_settings.yaml             # Local Airflow config

```

## Getting Started

### Prerequisites

- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed
- Docker Desktop running

### Local Development

1. **Start Airflow**:
   ```bash
   astro dev start
   ```

   This spins up 5 Docker containers:
   - Postgres (Metadata Database)
   - Scheduler
   - DAG Processor
   - API Server
   - Triggerer

2. **Access Airflow UI**:
   - Navigate to http://localhost:8080
   - Default credentials: `admin` / `admin`

3. **Configure DAG Parameters** (Optional):
   - In the Airflow UI, go to each DAG and click "Trigger DAG w/ config"
   - Modify `park_filter` parameter to filter for specific theme parks:
     - `'disney'` - All Disney parks
     - `'universal'` - Universal parks
     - `'cedar'` - Cedar Fair parks
     - `''` - All parks (no filter)

4. **Monitor Pipeline**:
   - View DAG runs in the Airflow UI
   - Check logs for extracted record counts
   - Monitor asset dependencies

### Testing

Run DAG tests:
```bash
astro dev pytest
```

### Stopping Airflow

```bash
astro dev stop
```

## Data Flow

```
ThemeParks.wiki API
        ↓
  [HTTP Client]
        ↓
   [Async Tasks]
        ↓
  [Data Assets]
        ↓
    [Loaders]
        ↓
  [Target Storage]
```

## API Reference

This project uses the [ThemeParks.wiki API](https://api.themeparks.wiki/) which provides:
- Live wait times for attractions
- Show schedules
- Park operating hours
- Attraction details
- Coverage for 100+ parks worldwide

## Deployment

To deploy to Astronomer:

```bash
astro deploy
```

For detailed deployment instructions, see [Astronomer Documentation](https://www.astronomer.io/docs/astro/deploy-code/).

## Configuration

### Airflow Settings (`airflow_settings.yaml`)
Use this file to configure Connections, Variables, and Pools for local development.

### DAG Parameters
- **park_filter**: Filter destinations by keyword (case-insensitive)
- Configurable per-DAG execution via Airflow UI

## Contributing

When adding new DAGs or modifying existing ones:
1. Follow the asset-based pattern for dependencies
2. Use async methods for API calls
3. Implement proper error handling
4. Add unit tests in `tests/dags/`

## License

This project uses the Apache 2.0 license as provided by Astronomer.
