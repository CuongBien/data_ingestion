# Data Ingestion ELT (Airflow + MinIO + Postgres + dbt)

## Overview

This repository implements a simple ELT pipeline:

1. **E (Extract)**: `ingestion_api_to_minio` pulls data from API and writes raw parquet to MinIO Bronze.
2. **L (Load)**: `elt_with_dbt_dag` loads Bronze parquet into Postgres raw table `raw.api_posts`.
3. **T (Transform)**: `elt_with_dbt_dag` runs `dbt run` to build transformed models in Postgres.

Key DAGs:
- `airflow/dags/data_ingestion_dag.py` -> `ingestion_api_to_minio`
- `airflow/dags/elt_pipeline_dag.py` -> `elt_with_dbt_dag`


## Repository Structure

- `docker-compose.yaml`: main local stack (Airflow, Postgres, MinIO, Redis)
- `.env`: environment variables for Docker Compose
- `airflow/dags/`: Airflow DAGs and helper scripts
- `transform_layer/`: dbt project


## Prerequisites

- Docker Desktop
- Docker Compose v2


## Local Setup

1. Configure environment:
   - Ensure `.env` at repository root has required values.
   - Important: `AIRFLOW_PROJ_DIR=./airflow`

2. Start stack:

```bash
docker compose up -d
```

3. Airflow UI:
   - URL: `http://localhost:8080`
   - Default user/password: from `.env` (`_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`)

4. MinIO UI:
   - URL: `http://localhost:9001`

5. Postgres (from host):
   - host: `127.0.0.1`
   - port: `5433`


## Required Airflow Variables

Create in **Airflow UI -> Admin -> Variables**:

- `minio_bucket` = `datalake`
- `telegram_bot_token` = `<your_token>`
- `telegram_chat_id` = `<your_chat_id>`


## Required Airflow Connections

Create in **Airflow UI -> Admin -> Connections**:

1. `minio_conn` (type: `Amazon Web Services`)
   - Login: `minioadmin`
   - Password: `minioadmin`
   - Extra:
     ```json
     {
       "endpoint_url": "http://minio:9000",
       "addressing_style": "path"
     }
     ```

2. `postgres_gold_conn` (type: `Postgres`)
   - Host: `postgres`
   - Port: `5432`
   - Database: `airflow`
   - Login: `airflow`
   - Password: `airflow`


## Run Order

### Step 1: Extract to Bronze

Trigger DAG `ingestion_api_to_minio`.

Expected Bronze path format:

`s3://datalake/bronze/api_posts/dt=<YYYY-MM-DD>/<run_id>/data.parquet`

### Step 2: ELT (Load + Transform)

Trigger DAG `elt_with_dbt_dag` with config:

```json
{"execution_date":"YYYY-MM-DD"}
```

Use the same date as Bronze data.


## dbt Notes

- dbt project path in container: `/opt/airflow/transform_layer`
- Profiles file: `transform_layer/profiles.yml`
  - target `docker`: for running dbt inside containers
  - target `local`: for running dbt from host machine

Example inside container:

```bash
cd /opt/airflow/transform_layer
dbt run --profiles-dir .
dbt test --profiles-dir .
```


## Troubleshooting

- **No DAGs visible**:
  - Confirm `.env` has `AIRFLOW_PROJ_DIR=./airflow`
  - Recreate Airflow services: `docker compose up -d --force-recreate`

- **`minio_conn must provide login/password`**:
  - Recheck `minio_conn` login/password in Airflow Connections.

- **`No files found ... bronze/api_posts/dt=...`**:
  - Bronze data for that `execution_date` does not exist yet.
  - Run `ingestion_api_to_minio` first.

- **dbt `No dbt_project.yml found`**:
  - Run dbt from `/opt/airflow/transform_layer` or pass `--project-dir`.
