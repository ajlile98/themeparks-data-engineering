# Theme Parks Data Engineering

A real-time ETL pipeline for collecting and processing theme park data from the [ThemeParks.wiki API](https://api.themeparks.wiki/). Built with Apache Airflow (Astronomer Runtime) using **asset-based DAG dependencies** to create a self-managing, event-driven pipeline that extracts destinations, park entities, and live wait times, and loads them into MinIO object storage as Parquet files.

---

## Architecture Overview

The pipeline is structured as a four-stage asset dependency chain. Each DAG produces an Airflow **Asset** that triggers the next stage automatically, with the live data DAG running independently on its own schedule while still consuming entity data.

```
┌──────────────────────────────────────────────────────────┐
│                  ThemeParks.wiki API                     │
└─────────────────────┬────────────────────────────────────┘
                      │ HTTPS
        ┌─────────────▼─────────────┐
        │   raw_theme_park_         │  Schedule: 0 */1 * * *
        │   destinations            │  (every hour)
        │   [Asset produced]        │
        └─────────────┬─────────────┘
                      │ triggers (Asset dependency)
        ┌─────────────▼─────────────┐
        │   raw_theme_park_         │  Schedule: [Asset("raw_theme_park_destinations")]
        │   entities                │  (runs when destinations asset updates)
        │   [Asset produced]        │
        └──────┬────────────────────┘
               │ triggers (Asset dependency)   ┌───────────────────────────────┐
               │                               │  raw_theme_park_live_data     │
               │                               │  Schedule: */5 * * * *        │
               │                               │  inlets: raw_theme_park_      │
               │                               │  entities (reads XCom)        │
               │                               │  [Asset produced]             │
               │                               └────────────┬──────────────────┘
               │                                            │
        ┌──────▼────────────────────────────────────────────▼──────┐
        │          load_themeparks_to_minio                        │
        │          Schedule: [destinations ∨ entities ∨ live]      │
        │          Writes Parquet → MinIO (watermarked)            │
        └──────────────────────────────────────────────────────────┘
```

---

## DAGs

### 1. `raw_theme_park_destinations`
**File**: [dags/themepark_destination_dag.py](dags/themepark_destination_dag.py)

| Property | Value |
|---|---|
| Schedule | `0 */1 * * *` (every hour) |
| Asset produced | `raw_theme_park_destinations` |
| Returns | `list[dict]` — one record per destination |
| Triggers | `raw_theme_park_entities`, `load_themeparks_to_minio` |

Calls `GET /destinations` on the ThemeParks.wiki API and returns the full list of theme park resorts with their associated park hierarchies. Each record is tagged with an `ingest_timestamp` (`datetime.now().isoformat()`).

---

### 2. `raw_theme_park_entities`
**File**: [dags/themepark_entities_dag.py](dags/themepark_entities_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_destinations")]` |
| Asset produced | `raw_theme_park_entities` |
| Returns | `list[dict]` — one record per entity (attraction, show, facility) |
| Triggers | `load_themeparks_to_minio` |

Fires automatically when the destinations asset is refreshed. Pulls the destinations XCom to extract all `park_id` values, then fetches `GET /entity/{id}/children` for every park in parallel using `asyncio.gather()`. Records contain: `park_id`, `id`, `name`, `entityType`, `ingest_timestamp`.

**XCom pull pattern** (string `task_ids`, not list, to avoid shape-wrapping):
```python
destinations = context["ti"].xcom_pull(
    dag_id="raw_theme_park_destinations",
    task_ids="raw_theme_park_destinations",
    key="return_value",
    include_prior_dates=True,
)[0]
```

---

### 3. `raw_theme_park_live_data`
**File**: [dags/themepark_live_data_dag.py](dags/themepark_live_data_dag.py)

| Property | Value |
|---|---|
| Schedule | `*/5 * * * *` (every 5 minutes) |
| Asset produced | `raw_theme_park_live_data` |
| Inlets | `Asset("raw_theme_park_entities")` |
| Returns | `list[dict]` — one record per attraction's live status |
| Triggers | `load_themeparks_to_minio` |

Runs independently every 5 minutes on its own cron schedule. Uses `inlets=Asset("raw_theme_park_entities")` to read the most recent entities XCom snapshot (`include_prior_dates=True`) and derive the set of `park_id` values to query. Then calls `GET /entity/{id}/liveData` in parallel for all parks. Records contain: `park_id`, `id`, `name`, `entityType`, `status`, `queue`, `lastUpdated`, `ingest_timestamp`.

---

### 4. `load_themeparks_to_minio`
**File**: [dags/themepark_minio_load_dag.py](dags/themepark_minio_load_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_destinations") OR Asset("raw_theme_park_entities") OR Asset("raw_theme_park_live_data")]` |
| Storage backend | MinIO via `ObjectStoragePath` |
| Output format | Parquet (Snappy compressed) |
| Watermarked | Yes — skips writes when `ingest_timestamp` hasn't changed |

Triggered by any of the three upstream assets. For each dataset (`destinations`, `entities`, `live`), it:

1. Pulls the raw XCom snapshot via `include_prior_dates=True`
2. Compares the max `ingest_timestamp` against a stored Airflow Variable watermark
3. Skips the write if the data hasn't changed since the last load
4. Otherwise serializes to Parquet using `pandas` + `pyarrow` and writes to partitioned path
5. Updates the watermark Variable

**Output path pattern**:
```
s3://airflow-data/themeparks-pipeline/bronze/{pipeline}/{YYYY-MM-DD}/{HH-MM-SS}.parquet
```

**Watermark Variables** (auto-managed, pre-seeded as empty in `airflow_settings.yaml`):
- `themeparks_minio_last_ingest_ts__destinations`
- `themeparks_minio_last_ingest_ts__entities`
- `themeparks_minio_last_ingest_ts__live`

---

## Project Structure

```
themeparks-data-engineering/
├── dags/
│   ├── themepark_destination_dag.py   # Hourly destinations extraction
│   ├── themepark_entities_dag.py      # Asset-triggered entities extraction
│   ├── themepark_live_data_dag.py     # 5-min live wait time extraction
│   └── themepark_minio_load_dag.py    # Asset-triggered Parquet → MinIO loader
├── include/
│   ├── extractors/
│   │   └── themeparks.py              # Async httpx API client
│   └── loaders/
│       ├── __init__.py
│       ├── Loader.py                  # Abstract base loader
│       ├── CsvLoader.py
│       ├── ParquetLoader.py
│       ├── IcebergLoader.py
│       ├── KafkaLoader.py
│       ├── MinioLoader.py             # Direct MinIO client loader
│       └── MinioStorage.py
├── tests/
│   └── dags/
│       └── test_dag_example.py
├── .env                               # Environment variables (local only)
├── airflow_settings.yaml              # Connections, Variables, Pools (local only)
├── Dockerfile                         # Astronomer Runtime base image
├── requirements.txt                   # Python dependencies
└── packages.txt                       # OS-level packages
```

---

## Configuration

### Environment Variables (`.env`)

```dotenv
OBJECT_STORAGE_SYSTEM=s3              # Storage scheme: s3, gs, file, etc.
OBJECT_STORAGE_CONN_ID=minio_default  # Airflow connection ID
OBJECT_STORAGE_PATH=airflow-data/themeparks-pipeline  # Base bucket/prefix

MINIO_ENDPOINT=https://minio-api.andylile.com
MINIO_ACCESS_KEY=<your-access-key>
MINIO_SECRET_KEY=<your-secret-key>
```

### Airflow Connection (`airflow_settings.yaml`)

```yaml
connections:
  - conn_id: minio_default
    conn_type: aws
    conn_login: <MINIO_ACCESS_KEY>
    conn_password: <MINIO_SECRET_KEY>
    conn_extra:
      endpoint_url: https://minio-api.andylile.com
      region_name: us-east-1
      addressing_style: path
```

The `addressing_style: path` setting is required for MinIO (forces path-style URLs instead of virtual-hosted-style).

---

## Getting Started

### Prerequisites

- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- Docker Desktop

### Local Development

```bash
# Start Airflow (Postgres, Scheduler, DAG Processor, API Server, Triggerer)
astro dev start

# Airflow UI: http://localhost:8080  (admin / admin)

# Restart after config changes
astro dev restart

# Run tests
astro dev pytest

# Stop
astro dev stop
```

### First Run

Once Airflow is running:
1. Trigger `raw_theme_park_destinations` manually from the UI — this starts the asset chain
2. When it completes, `raw_theme_park_entities` fires automatically
3. When entities completes, `load_themeparks_to_minio` fires and writes all three datasets to MinIO
4. `raw_theme_park_live_data` runs independently every 5 minutes and also triggers a MinIO load on each run

---

## Data Model

### Destinations
| Field | Description |
|---|---|
| `id` | Destination ID |
| `name` | Resort name |
| `slug` | URL-safe identifier |
| `parks` | List of parks in the resort |
| `ingest_timestamp` | ISO8601 timestamp of extraction |

### Entities
| Field | Description |
|---|---|
| `park_id` | Parent park ID |
| `id` | Entity ID |
| `name` | Entity name |
| `entityType` | `ATTRACTION`, `SHOW`, `RESTAURANT`, etc. |
| `ingest_timestamp` | ISO8601 timestamp of extraction |

### Live Data
| Field | Description |
|---|---|
| `park_id` | Parent park ID |
| `id` | Entity ID |
| `name` | Entity name |
| `entityType` | Entity type |
| `status` | `OPERATING`, `DOWN`, `CLOSED`, etc. |
| `queue` | Serialized wait time queue data |
| `lastUpdated` | Last updated timestamp from API |
| `ingest_timestamp` | ISO8601 timestamp of extraction |

---

## Dependencies

| Package | Purpose |
|---|---|
| `apache-airflow-providers-amazon[s3fs]` | `ObjectStoragePath` S3/MinIO backend |
| `pyarrow==23.0.1` | Parquet serialization |

Optional (commented out in `requirements.txt`):
- `httpx` — async HTTP (bundled in extractor)
- `minio` — direct MinIO client (MinioLoader)
- `kafka-python` — KafkaLoader
- `pyspark` — IcebergLoader

---

## API Reference

This project uses the [ThemeParks.wiki API](https://api.themeparks.wiki/v1):

| Endpoint | Used by |
|---|---|
| `GET /destinations` | `raw_theme_park_destinations` |
| `GET /entity/{id}/children` | `raw_theme_park_entities` |
| `GET /entity/{id}/liveData` | `raw_theme_park_live_data` |

Coverage: 100+ parks worldwide including Disney, Universal, SeaWorld, Cedar Fair, and more.

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
