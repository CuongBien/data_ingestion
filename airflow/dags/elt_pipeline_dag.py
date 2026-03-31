import logging
from datetime import datetime
from urllib.parse import urlparse

import duckdb
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow.sdk.bases.hook import BaseHook


def _parse_endpoint_host_port(endpoint_url: str) -> tuple[str, bool]:
    parsed = urlparse(endpoint_url.strip())
    host = parsed.hostname
    if host is None or host == "":
        raise ValueError("minio_conn extra endpoint_url is invalid.")
    port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 9000)
    use_ssl = parsed.scheme == "https"
    return f"{host}:{port}", use_ssl


def load_minio_to_postgres_raw(**kwargs) -> None:
    execution_date = kwargs["dag_run"].conf.get("execution_date") if kwargs.get("dag_run") else None
    if execution_date is None:
        raise ValueError(
            "elt_with_dbt_dag requires dag_run.conf.execution_date (YYYY-MM-DD). "
            "Example: {\"execution_date\": \"2026-03-30\"}"
        )

    minio_conn = BaseHook.get_connection("minio_conn")
    pg_conn = BaseHook.get_connection("postgres_gold_conn")
    bucket_name = Variable.get("minio_bucket")

    access_key = minio_conn.login
    secret_key = minio_conn.password
    endpoint_url = minio_conn.extra_dejson.get("endpoint_url", "http://minio:9000")
    endpoint_host_port, use_ssl = _parse_endpoint_host_port(endpoint_url)

    if not access_key or not secret_key:
        raise ValueError("minio_conn must provide login/password.")

    bronze_path = f"s3://{bucket_name}/bronze/api_posts/dt={execution_date}/*/data.parquet"
    logging.info("ELT LOAD raw.api_posts from %s", bronze_path)

    pg_conn_str = (
        f"host={pg_conn.host} "
        f"port={pg_conn.port or 5432} "
        f"dbname={pg_conn.schema} "
        f"user={pg_conn.login} "
        f"password={pg_conn.password}"
    )

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"SET s3_endpoint='{endpoint_host_port}';")
        con.execute(f"SET s3_access_key_id='{access_key}';")
        con.execute(f"SET s3_secret_access_key='{secret_key}';")
        con.execute(f"SET s3_use_ssl={str(use_ssl).lower()};")
        con.execute("SET s3_url_style='path';")

        con.execute(f"ATTACH '{pg_conn_str}' AS db_raw (TYPE POSTGRES);")
        con.execute("CREATE SCHEMA IF NOT EXISTS db_raw.raw;")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS db_raw.raw.api_posts (
                id INTEGER,
                userId INTEGER,
                title VARCHAR,
                body VARCHAR,
                ingested_at TIMESTAMP
            );
            """
        )

        con.execute(
            f"""
            DELETE FROM db_raw.raw.api_posts
            WHERE DATE(ingested_at) = DATE '{execution_date}';
            """
        )
        con.execute(
            f"""
            INSERT INTO db_raw.raw.api_posts (id, userId, title, body, ingested_at)
            SELECT
                id,
                userId,
                title,
                body,
                NOW() AS ingested_at
            FROM read_parquet('{bronze_path}');
            """
        )
        logging.info("ELT LOAD succeeded for date=%s", execution_date)
    finally:
        con.close()


with DAG(dag_id="elt_with_dbt_dag", start_date=datetime(2026, 3, 1), schedule=None, catchup=False) as dag:
    task_load_raw = PythonOperator(
        task_id="load_minio_to_raw_postgres",
        python_callable=load_minio_to_postgres_raw,
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run_transform",
        bash_command="cd /opt/airflow/transform_layer && dbt run --profiles-dir .",
    )

    task_load_raw >> task_dbt_run