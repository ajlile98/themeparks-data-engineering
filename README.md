# Theme Parks Data Engineering

A real-time lakehouse pipeline for collecting and processing theme park data from the [ThemeParks.wiki API](https://api.themeparks.wiki/). Built with Apache Airflow (Astronomer Runtime) using **asset-based DAG dependencies** across two storage layers:

- **Bronze** — raw NDJSON (`.jsonl`) files in MinIO, written by extractor DAGs
- **Silver** — Apache Iceberg tables on MinIO, catalogued by Nessie REST catalog

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                      ThemeParks.wiki API                         │
└───────────┬─────────────────────┬───────────────────────┬────────┘
            │ HTTPS               │ HTTPS                 │ HTTPS
            ▼                     │                       ▼
  ┌─────────────────────┐         │         ┌──────────────────────┐
  │ raw_theme_park_     │         │         │ raw_theme_park_      │
  │ destinations        │         │         │ live_data            │
  │ Schedule: 0 */1 * * │         │         │ Schedule: */5 * * * *│
  │ → bronze/           │         │         │ → bronze/live/       │
  │   destinations/     │         │         │   *.jsonl            │
  └────────┬────────────┘         │         └──────────┬───────────┘
           │ triggers             │                    │ triggers
           ▼                      │                    ▼
  ┌─────────────────────┐         │         ┌──────────────────────┐
  │ raw_theme_park_     │◄────────┘         │ iceberg_live_data    │
  │ entities            │                   │ → silver.live_data   │
  │ → bronze/entities/  │                   │   (Iceberg/Nessie)   │
  │   *.jsonl           │                   └──────────────────────┘
  └────────┬────────────┘
           │ triggers (both)
           ▼
  ┌─────────────────────┐   ┌──────────────────────┐
  │ iceberg_destinations│   │ iceberg_entities      │
  │ → silver.destinations│  │ → silver.entities    │
  │   (Iceberg/Nessie)  │   │   (Iceberg/Nessie)   │
  └─────────────────────┘   └──────────────────────┘
```

**Data handoff pattern**: Each extractor writes records as NDJSON to MinIO and returns the `s3://` path string (~80 bytes) via XCom. Silver DAGs read that path, convert to Arrow, and append to Iceberg via the Nessie REST catalog.

---

## DAGs

### Bronze Layer (Extractors)

#### 1. `raw_theme_park_destinations`
**File**: [dags/themepark_destination_dag.py](dags/themepark_destination_dag.py)

| Property | Value |
|---|---|
| Schedule | `0 */1 * * *` (every hour) |
| Asset produced | `raw_theme_park_destinations` |
| Returns (XCom) | `s3://` path string to bronze JSONL |
| Triggers | `raw_theme_park_entities`, `iceberg_destinations` |

Calls `GET /destinations`, writes all resort records as NDJSON to `bronze/destinations/YYYY/MM/DD/HHMMSS.jsonl`.

---

#### 2. `raw_theme_park_entities`
**File**: [dags/themepark_entities_dag.py](dags/themepark_entities_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_destinations")]` |
| Asset produced | `raw_theme_park_entities` |
| Returns (XCom) | `s3://` path string to bronze JSONL |
| Triggers | `iceberg_entities` |

Reads the destinations JSONL path from XCom, extracts all `park_id` values, and calls `GET /entity/{id}/children` in parallel via `asyncio.gather()`. Records: `park_id`, `id`, `name`, `entityType`, `ingest_timestamp`.

---

#### 3. `raw_theme_park_live_data`
**File**: [dags/themepark_live_data_dag.py](dags/themepark_live_data_dag.py)

| Property | Value |
|---|---|
| Schedule | `*/5 * * * *` (every 5 minutes) |
| Asset produced | `raw_theme_park_live_data` |
| Returns (XCom) | `s3://` path string to bronze JSONL |
| Triggers | `iceberg_live_data` |

Reads the entities JSONL path from XCom (`include_prior_dates=True`), derives the park set, and calls `GET /entity/{id}/liveData` in parallel. Records: `park_id`, `id`, `name`, `entityType`, `status`, `queue` (nested dict), `lastUpdated`, `ingest_timestamp`.

---

### Silver Layer (Iceberg Writers)

#### 4. `iceberg_destinations`
**File**: [dags/themepark_destinations_iceberg_dag.py](dags/themepark_destinations_iceberg_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_destinations")]` |
| Output | `silver.destinations` Iceberg table |

Reads bronze JSONL → converts to Arrow → appends to Iceberg via Nessie. Creates namespace and table on first run.

---

#### 5. `iceberg_entities`
**File**: [dags/themepark_entities_iceberg_dag.py](dags/themepark_entities_iceberg_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_entities")]` |
| Output | `silver.entities` Iceberg table |

---

#### 6. `iceberg_live_data`
**File**: [dags/themepark_live_iceberg_dag.py](dags/themepark_live_iceberg_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_live_data")]` |
| Output | `silver.live_data` Iceberg table |

The `queue` field is stored as a nested struct in Iceberg. `pa.null()` fields (absent queues in a batch) are cast to `string` to satisfy Iceberg format-version 2.

---

## Infrastructure

### Nessie REST Catalog (`docker-compose.override.yml`)

Nessie acts as the Iceberg REST catalog, backed by Postgres for metadata persistence. Both run on a shared Docker network (`nessie-net`) with the Airflow scheduler.

| Service | Image | Port |
|---|---|---|
| `nessie` | `ghcr.io/projectnessie/nessie:latest` | `19120` |
| `nessie-db` | `postgres:16-alpine` | internal |

MinIO credentials are injected into Nessie via Quarkus URN-based secret resolution (values sourced from `.env`):
```yaml
- nessie.catalog.service.s3.default-options.access-key=urn:nessie-secret:quarkus:my-secrets.s3-default
- my-secrets.s3-default.name=${MINIO_ACCESS_KEY}
- my-secrets.s3-default.secret=${MINIO_SECRET_KEY}
```

---

## Project Structure

```
themeparks-data-engineering/
├── dags/
│   ├── themepark_destination_dag.py           # Hourly destinations extraction → bronze
│   ├── themepark_entities_dag.py              # Asset-triggered entities extraction → bronze
│   ├── themepark_live_data_dag.py             # 5-min live wait time extraction → bronze
│   ├── themepark_destinations_iceberg_dag.py  # bronze → silver.destinations (Iceberg)
│   ├── themepark_entities_iceberg_dag.py      # bronze → silver.entities (Iceberg)
│   └── themepark_live_iceberg_dag.py          # bronze → silver.live_data (Iceberg)
├── include/
│   ├── extractors/
│   │   └── themeparks.py                      # Async httpx API client
│   ├── writers/
│   │   ├── bronze_writer.py                   # write_bronze() / read_bronze() helpers
│   │   └── iceberg_writer.py                  # get_catalog() / append_to_iceberg() helpers
│   └── loaders/                               # Legacy loaders (unused, kept for reference)
│       ├── Loader.py
│       ├── CsvLoader.py
│       ├── ParquetLoader.py
│       ├── IcebergLoader.py
│       ├── KafkaLoader.py
│       ├── MinioLoader.py
│       └── MinioStorage.py
├── notebooks/
│   └── query_iceberg.ipynb                    # DuckDB + PyIceberg ad-hoc query notebook
├── tests/
│   └── dags/
│       └── test_dag_example.py
├── docker-compose.override.yml                # Nessie + nessie-db services
├── .env                                       # Environment variables (local only)
├── airflow_settings.yaml                      # Connections, Variables, Pools
├── Dockerfile                                 # Astronomer Runtime base image
├── requirements.txt                           # Python dependencies
└── packages.txt                               # OS-level packages
```

---

## Configuration

### Environment Variables (`.env`)

```dotenv
# MinIO object storage
MINIO_ENDPOINT=https://minio-api.example.com
MINIO_ACCESS_KEY=<your-access-key>
MINIO_SECRET_KEY=<your-secret-key>

# Airflow object storage connection
OBJECT_STORAGE_SYSTEM=s3
OBJECT_STORAGE_CONN_ID=minio_default
```

### Airflow Connection (`airflow_settings.yaml`)

```yaml
connections:
  - conn_id: minio_default
    conn_type: aws
    conn_login: <MINIO_ACCESS_KEY>
    conn_password: <MINIO_SECRET_KEY>
    conn_extra:
      endpoint_url: https://minio-api.example.com
      region_name: us-east-1
      addressing_style: path
```

`addressing_style: path` is required for MinIO (forces path-style S3 URLs).

---

## Getting Started

### Prerequisites

- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- Docker Desktop

### Local Development

```bash
# Start all services (Airflow + Nessie + nessie-db)
astro dev start

# Airflow UI: http://localhost:8080  (admin / admin)
# Nessie UI:  http://localhost:19120

# Restart after config / docker-compose changes
astro dev restart

# Run tests
astro dev pytest

# Stop
astro dev stop
```

### First Run

1. Trigger `raw_theme_park_destinations` manually from the Airflow UI
2. Completes → `raw_theme_park_entities` and `iceberg_destinations` fire automatically
3. Entities completes → `iceberg_entities` fires
4. `raw_theme_park_live_data` runs every 5 minutes → triggers `iceberg_live_data` each run

### Querying Iceberg Tables

Open [notebooks/query_iceberg.ipynb](notebooks/query_iceberg.ipynb) for an interactive DuckDB + PyIceberg notebook:
- Connects to Nessie at `localhost:19120` to resolve table metadata paths
- Uses DuckDB's native `iceberg_scan()` to run SQL directly against MinIO Parquet files
- Includes ready-to-run queries for all three silver tables

---

## Data Model

### Bronze layer — `s3://airflow-data/bronze/{table}/YYYY/MM/DD/HHMMSS.jsonl`

NDJSON, one JSON object per line. Schema mirrors the silver Iceberg tables below.

### Silver layer — Iceberg tables in `s3://airflow-data/iceberg/` (namespace: `silver`)

#### `silver.destinations`
| Field | Type | Description |
|---|---|---|
| `id` | string | Destination UUID |
| `name` | string | Resort name |
| `slug` | string | URL-safe identifier |
| `parks` | list\<struct\> | Parks in the resort |
| `ingest_timestamp` | string | UTC extraction timestamp |

#### `silver.entities`
| Field | Type | Description |
|---|---|---|
| `park_id` | string | Parent park UUID |
| `id` | string | Entity UUID |
| `name` | string | Entity name |
| `entityType` | string | `ATTRACTION`, `SHOW`, `RESTAURANT`, etc. |
| `ingest_timestamp` | string | UTC extraction timestamp |

#### `silver.live_data`
| Field | Type | Description |
|---|---|---|
| `park_id` | string | Parent park UUID |
| `id` | string | Entity UUID |
| `name` | string | Entity name |
| `entityType` | string | Entity type |
| `status` | string | `OPERATING`, `DOWN`, `CLOSED`, `REFURBISHMENT` |
| `queue` | struct | Nested queue data (`STANDBY.waitTime`, `SINGLE_RIDER.waitTime`, etc.) |
| `lastUpdated` | string | Last updated timestamp from API |
| `ingest_timestamp` | string | UTC extraction timestamp |

---

## Dependencies

| Package | Purpose |
|---|---|
| `apache-airflow-providers-amazon[s3fs]` | `ObjectStoragePath` S3/MinIO backend |
| `pyarrow==23.0.1` | Arrow serialization used by PyIceberg |
| `pyiceberg[nessie]>=0.7.0` | Iceberg Python client + Nessie REST catalog |

---

## API Reference

This project uses the [ThemeParks.wiki API](https://api.themeparks.wiki/v1):

| Endpoint | Used by |
|---|---|
| `GET /destinations` | `raw_theme_park_destinations` |
| `GET /entity/{id}/children` | `raw_theme_park_entities` |
| `GET /entity/{id}/liveData` | `raw_theme_park_live_data` |

Coverage: 100+ parks worldwide including Disney, Universal, SeaWorld, Cedar Fair, and more.

---

## License

This project uses the Apache 2.0 license as provided by Astronomer.

└───────────┬─────────────────────┬───────────────────────┬────────┘
            │ HTTPS               │ HTTPS                 │ HTTPS
┌───────────▼───────────┐         │            ┌──────────▼──────────────┐
│  raw_theme_park_      │         │            │  raw_theme_park_        │
│  destinations         │         │            │  live_data              │
│  Schedule: 0 */1 * *  │         │            │  Schedule: */5 * * * *  │
│  [Asset produced]     │         │            │  reads entity XCom      │
└───────────┬───────────┘         │            │  [Asset produced]       │
            │ triggers            │            └──────────┬──────────────┘
┌───────────▼───────────┐         │                       │ triggers
│  raw_theme_park_      │◄────────┘            ┌──────────▼──────────────┐
│  entities             │  triggers            │  load_live_to_parquet   │
│  Schedule: [Asset(    │                      │  Schedule: [Asset(      │
│  destinations)]       │                      │  live_data)]            │
│  [Asset produced]     │                      │  → MinIO bronze/live/   │
└───────────┬───────────┘                      └─────────────────────────┘
            │ triggers
┌───────────▼───────────┐   ┌────────────────────────────┐
│  load_destinations_   │   │  load_entities_to_parquet  │
│  to_parquet           │   │  Schedule: [Asset(         │
│  Schedule: [Asset(    │   │  entities)]                │
│  destinations)]       │   │  → MinIO bronze/entities/  │
│  → MinIO bronze/      │   └────────────────────────────┘
│    destinations/      │
└───────────────────────┘
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
| Triggers | `raw_theme_park_entities`, `load_destinations_to_parquet` |

Calls `GET /destinations` on the ThemeParks.wiki API and returns the full list of theme park resorts with their associated park hierarchies. Each record is tagged with an `ingest_timestamp` (`datetime.now().isoformat()`).

---

### 2. `raw_theme_park_entities`
**File**: [dags/themepark_entities_dag.py](dags/themepark_entities_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_destinations")]` |
| Asset produced | `raw_theme_park_entities` |
| Returns | `list[dict]` — one record per entity (attraction, show, facility) |
| Triggers | `load_entities_to_parquet` |

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
| Returns | `list[dict]` — one record per attraction's live status |
| Triggers | `load_live_to_parquet` |

Runs independently every 5 minutes on its own cron schedule. Reads the most recent entities XCom snapshot (`include_prior_dates=True`) to derive the set of `park_id` values to query, then calls `GET /entity/{id}/liveData` in parallel for all parks. Records contain: `park_id`, `id`, `name`, `entityType`, `status`, `queue`, `lastUpdated`, `ingest_timestamp`.

---

### 4. `load_destinations_to_parquet`
**File**: [dags/themepark_destinations_parquet_dag.py](dags/themepark_destinations_parquet_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_destinations")]` |
| Output | `bronze/destinations/YYYY-MM-DD/HH-MM-SS.parquet` |

Triggered immediately when destinations are extracted. Pulls the XCom snapshot and writes it to MinIO as a Snappy-compressed Parquet file via `ObjectStoragePath`.

---

### 5. `load_entities_to_parquet`
**File**: [dags/themepark_entities_parquet_dag.py](dags/themepark_entities_parquet_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_entities")]` |
| Output | `bronze/entities/YYYY-MM-DD/HH-MM-SS.parquet` |

Triggered immediately when entities are extracted. Pulls the XCom snapshot and writes it to MinIO as a Snappy-compressed Parquet file.

---

### 6. `load_live_to_parquet`
**File**: [dags/themepark_live_parquet_dag.py](dags/themepark_live_parquet_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_live_data")]` |
| Output | `bronze/live/YYYY-MM-DD/HH-MM-SS.parquet` |

Triggered on every 5-minute live data extraction. Each run appends a new time-partitioned Parquet file, building a historical log of wait time snapshots.

**Output path pattern (all loaders)**:
```
s3://airflow-data/themeparks-pipeline/bronze/{pipeline}/{YYYY-MM-DD}/{HH-MM-SS}.parquet
```

---

## Project Structure

```
themeparks-data-engineering/
├── dags/
│   ├── themepark_destination_dag.py          # Hourly destinations extraction
│   ├── themepark_entities_dag.py             # Asset-triggered entities extraction
│   ├── themepark_live_data_dag.py            # 5-min live wait time extraction
│   ├── themepark_destinations_parquet_dag.py # destinations → Parquet loader
│   ├── themepark_entities_parquet_dag.py     # entities → Parquet loader
│   └── themepark_live_parquet_dag.py         # live data → Parquet loader
├── include/
│   ├── extractors/
│   │   └── themeparks.py                     # Async httpx API client
│   └── loaders/
│       ├── __init__.py
│       ├── parquet_writer.py                 # Shared write_parquet() helper
│       ├── Loader.py                         # Abstract base loader
│       ├── CsvLoader.py
│       ├── ParquetLoader.py
│       ├── IcebergLoader.py
│       ├── KafkaLoader.py
│       ├── MinioLoader.py
│       └── MinioStorage.py
├── tests/
│   └── dags/
│       └── test_dag_example.py
├── .env                                      # Environment variables (local only)
├── airflow_settings.yaml                     # Connections, Variables, Pools (local only)
├── Dockerfile                                # Astronomer Runtime base image
├── requirements.txt                          # Python dependencies
└── packages.txt                              # OS-level packages
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

# Object Storage XCom backend (apache-airflow-providers-common-io, pre-installed)
# XComs larger than threshold (bytes) are stored in MinIO instead of Postgres
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=s3://minio_default@airflow-xcom/xcom
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=1048576
```

The S3 XCom backend is required because live data snapshots (thousands of records per run) exceed Postgres XCom size limits. With this configured, Airflow transparently stores XCom values larger than the threshold as JSON objects in `s3://airflow-xcom/xcom/` and only keeps the object storage key reference in Postgres. All DAG code (`xcom_pull`/`xcom_push`) works identically — no changes needed.

- Provider: `apache-airflow-providers-common-io` (pre-installed in Astronomer Runtime 3.x — no `requirements.txt` entry needed)
- Threshold: `1048576` bytes (1 MB) — XComs smaller than 1 MB stay in Postgres, larger go to MinIO
- Requires the `airflow-xcom` bucket to exist in MinIO before first run

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
1. Trigger `raw_theme_park_destinations` manually from the UI
2. When it completes → `raw_theme_park_entities` fires automatically, then `load_destinations_to_parquet` fires
3. When entities completes → `load_entities_to_parquet` fires
4. `raw_theme_park_live_data` runs every 5 minutes independently → triggers `load_live_to_parquet` on each run

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


## License

This project uses the Apache 2.0 license as provided by Astronomer.
