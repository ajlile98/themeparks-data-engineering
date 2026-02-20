# Theme Parks Data Engineering

A real-time lakehouse pipeline for collecting and processing theme park data from the [ThemeParks.wiki API](https://api.themeparks.wiki/). Built with Apache Airflow (Astronomer Runtime) using **asset-based DAG dependencies** across three storage layers:

- **Bronze** — raw NDJSON (`.jsonl`) files in MinIO, written by extractor DAGs
- **Silver** — Apache Iceberg tables on MinIO, catalogued by Nessie REST catalog (full history for event data; overwrite for reference data)
- **Gold** — pre-aggregated / deduplicated Iceberg tables for cheap downstream queries

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
  │   *.jsonl           │                   └──────────┬───────────┘
  └────────┬────────────┘                              │ triggers
           │ triggers (both)                           ▼
           ▼                              ┌──────────────────────┐
  ┌─────────────────────┐                 │ gold_live_data_      │
  │ iceberg_destinations│                 │ current              │
  │ → silver.destinations│                │ → gold.live_data_    │
  │ → silver.parks      │                 │   current            │
  │   (Iceberg/Nessie)  │                 │   (deduplicated)     │
  └─────────────────────┘                 └──────────────────────┘
  ┌─────────────────────┐
  │ iceberg_entities    │
  │ → silver.entities   │
  │   (Iceberg/Nessie)  │
  └─────────────────────┘
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
| Output | `silver.destinations`, `silver.parks` Iceberg tables |
| Write mode | Overwrite (reference data — complete dataset each run) |

Reads bronze JSONL → overwrites `silver.destinations`. Also explodes the nested `parks` array from each destination into a flat `silver.parks` table, giving `silver.entities` and `silver.live_data` a clean join target via `park_id`.

---

#### 5. `iceberg_entities`
**File**: [dags/themepark_entities_iceberg_dag.py](dags/themepark_entities_iceberg_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_entities")]` |
| Output | `silver.entities` Iceberg table |
| Write mode | Overwrite (reference data — complete dataset each run) |

---

#### 6. `iceberg_live_data`
**File**: [dags/themepark_live_iceberg_dag.py](dags/themepark_live_iceberg_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("raw_theme_park_live_data")]` |
| Output | `silver.live_data` Iceberg table |
| Write mode | Append (event data — full history retained) |

Flattens the `queue` dict from bronze into dedicated typed columns (`queue_standby_wait`, `queue_single_rider_wait`, etc.) — see Data Model below. Return-time fields are stored as strings to prevent PyArrow timestamp inference instability across batches.

---

### Gold Layer

#### 7. `gold_live_data_current`
**File**: [dags/themepark_live_gold_dag.py](dags/themepark_live_gold_dag.py)

| Property | Value |
|---|---|
| Schedule | `[Asset("iceberg_live_data")]` |
| Output | `gold.live_data_current` Iceberg table |
| Write mode | Overwrite |

Reads the full `silver.live_data` history via PyIceberg, deduplicates to one row per entity UUID (latest `ingest_timestamp` wins), and overwrites `gold.live_data_current`. Downstream queries against this table need no window function — one row per entity, always current state.

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
│   ├── themepark_destinations_iceberg_dag.py  # bronze → silver.destinations + silver.parks
│   ├── themepark_entities_iceberg_dag.py      # bronze → silver.entities (overwrite)
│   ├── themepark_live_iceberg_dag.py          # bronze → silver.live_data (append, flat queue)
│   ├── themepark_live_gold_dag.py             # silver.live_data → gold.live_data_current
│   └── themepark_maintenance_dag.py           # Daily Iceberg snapshot expiration
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
3. `iceberg_destinations` writes `silver.destinations` and `silver.parks`
4. Entities completes → `iceberg_entities` fires → writes `silver.entities`
5. `raw_theme_park_live_data` runs every 5 minutes → triggers `iceberg_live_data` → triggers `gold_live_data_current`

### Querying Iceberg Tables

Open [notebooks/query_iceberg.ipynb](notebooks/query_iceberg.ipynb) for an interactive DuckDB + PyIceberg notebook:
- Connects to Nessie at `localhost:19120` to resolve table metadata paths
- Uses DuckDB's native `iceberg_scan()` to run SQL directly against MinIO Parquet files
- Includes ready-to-run queries for all silver and gold tables
- Covers `DESCRIBE`, windowed deduplication strategies, and gold layer usage

---

## Data Model

### Bronze layer — `s3://airflow-data/bronze/{table}/YYYY/MM/DD/HHMMSS.jsonl`

NDJSON, one JSON object per line. Schema mirrors the silver Iceberg tables below.

### Silver layer — Iceberg tables in `s3://airflow-data/iceberg/` (namespace: `silver`)

Reference tables (`destinations`, `parks`, `entities`) use **overwrite** — each run is the complete current dataset, no history needed (Type 1 SCD). Live data uses **append** — each run is a point-in-time snapshot and history is preserved.

#### `silver.destinations` _(overwrite)_
| Field | Type | Description |
|---|---|---|
| `id` | string | Destination UUID |
| `name` | string | Resort name |
| `slug` | string | URL-safe identifier |
| `parks` | list\<struct\> | Parks in the resort |
| `ingest_timestamp` | string | UTC extraction timestamp |

#### `silver.parks` _(overwrite)_
Exploded from `destinations.parks`. Provides a flat join target for `park_id` in entities and live data.

| Field | Type | Description |
|---|---|---|
| `destination_id` | string | Parent destination UUID |
| `destination_name` | string | Parent destination name |
| `id` | string | Park UUID (= `park_id` in other tables) |
| `name` | string | Park name |
| `slug` | string | URL-safe identifier |

#### `silver.entities` _(overwrite)_
| Field | Type | Description |
|---|---|---|
| `park_id` | string | Parent park UUID |
| `id` | string | Entity UUID |
| `name` | string | Entity name |
| `entityType` | string | `ATTRACTION`, `SHOW`, `RESTAURANT`, etc. |
| `ingest_timestamp` | string | UTC extraction timestamp |

#### `silver.live_data` _(append)_
Queue data is flattened from the API's nested dict into dedicated columns. All return-time fields stored as strings to prevent PyArrow timestamp inference instability.

| Field | Type | Description |
|---|---|---|
| `park_id` | string | Parent park UUID |
| `id` | string | Entity UUID |
| `name` | string | Entity name |
| `entityType` | string | Entity type |
| `status` | string | `OPERATING`, `DOWN`, `CLOSED`, `REFURBISHMENT` |
| `queue_standby_wait` | int | `STANDBY.waitTime` (minutes) |
| `queue_single_rider_wait` | int | `SINGLE_RIDER.waitTime` |
| `queue_paid_standby_wait` | int | `PAID_STANDBY.waitTime` |
| `queue_return_time_state` | string | `RETURN_TIME.state` |
| `queue_return_time_start` | string | `RETURN_TIME.returnStart` |
| `queue_return_time_end` | string | `RETURN_TIME.returnEnd` |
| `queue_boarding_group_wait` | string | `BOARDING_GROUP.estimatedWait` |
| `queue_boarding_group_start` | string | `BOARDING_GROUP.currentGroupStart` |
| `queue_boarding_group_end` | string | `BOARDING_GROUP.currentGroupEnd` |
| `queue_boarding_group_status` | string | `BOARDING_GROUP.allocationStatus` |
| `queue_boarding_group_next_alloc` | string | `BOARDING_GROUP.nextAllocationTime` |
| `lastUpdated` | string | Last updated timestamp from API |
| `ingest_timestamp` | string | UTC extraction timestamp |

### Gold layer — Iceberg tables in `s3://airflow-data/iceberg/` (namespace: `gold`)

#### `gold.live_data_current` _(overwrite)_
Pre-deduplicated current state of every entity. Written after each `silver.live_data` append — one row per entity UUID, latest `ingest_timestamp` wins. Downstream queries need no window function.

Schema mirrors `silver.live_data` (all same columns, no `rn` or extra fields).

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

---

## Maintenance

### Iceberg Snapshot Expiration

**File**: [dags/themepark_maintenance_dag.py](dags/themepark_maintenance_dag.py)

Runs daily at 03:00 UTC. Expires old Iceberg snapshots and deletes the orphaned
Parquet data files they referenced.

> **Why not S3 lifecycle rules on `iceberg/`?** Lifecycle rules delete files
> based on age alone — they can remove a Parquet file still referenced by a
> valid snapshot, corrupting the table. `expire_snapshots()` marks files as
> orphaned in Iceberg metadata first, so deletion is always safe.

| Table | Retain snapshots newer than | Min kept |
|---|---|---|
| `silver.live_data` | 30 days | 5 |
| `gold.live_data_current` | 7 days | 2 |
| `silver.destinations` | 7 days | 1 |
| `silver.parks` | 7 days | 1 |
| `silver.entities` | 7 days | 1 |

Reference tables (`destinations`, `parks`, `entities`) overwrite on every run
so they rarely accumulate more than 2 snapshots — the expiry is a safety net.

### Bronze File Retention

Bronze JSONL files are standalone (no Iceberg references) — safe to expire via
MinIO Client (`mc`) lifecycle rules:

```bash
# 7-day expiry on reference bronze files (already in silver)
mc ilm rule add --expire-days 7 --prefix "bronze/destinations/" myminio/airflow-data
mc ilm rule add --expire-days 7 --prefix "bronze/entities/"     myminio/airflow-data

# 30-day expiry on live bronze (longer — historical audit trail)
mc ilm rule add --expire-days 30 --prefix "bronze/live/"         myminio/airflow-data

# Verify
mc ilm rule ls myminio/airflow-data
```

---

## License

This project uses the Apache 2.0 license as provided by Astronomer.
