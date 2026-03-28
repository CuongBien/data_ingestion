import logging
from urllib.parse import urlparse

import duckdb
from airflow.hooks.base import BaseHook

def _duckdb_s3_host_port(endpoint_url: str) -> tuple[str, bool]:
    """Parse Airflow AWS extra endpoint_url into host:port and ssl flag for DuckDB httpfs."""
    parsed = urlparse(endpoint_url.strip())
    host = parsed.hostname
    if host is None or host == "":
        raise ValueError("minio_conn extra endpoint_url must include a host, e.g. http://minio:9000")
    if parsed.port is not None:
        port = parsed.port
    else:
        port = 443 if parsed.scheme == "https" else 9000
    use_ssl = parsed.scheme == "https"
    return f"{host}:{port}", use_ssl


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def transform_bronze_to_silver(
    execution_date: str,
    bucket_name: str,
    output_run_id_fragment: str,
    minio_conn_id: str,
) -> None:
    """
    Read Bronze Parquet from MinIO via DuckDB httpfs, aggregate, write Silver Parquet.
    Credentials and endpoint come from Airflow Connection (same as aws_conn_id=minio_conn).
    """
    conn_af = BaseHook.get_connection(minio_conn_id)
    access_key = conn_af.login
    secret_key = conn_af.password
    if not access_key or not secret_key:
        raise ValueError(
            f"Connection {minio_conn_id!r} must have Login (access key) and Password (secret key) set."
        )

    extra = conn_af.extra_dejson
    endpoint_url = extra.get("endpoint_url") or "http://minio:9000"
    host_port, use_ssl = _duckdb_s3_host_port(endpoint_url)

    logging.info(
        "silver_transform: DuckDB S3 endpoint host:port=%s ssl=%s bucket=%s",
        host_port,
        use_ssl,
        bucket_name,
    )

    # One file per run: bronze/api_posts/dt=<ds>/<run_id>/data.parquet
    bronze_glob = (
        f"s3://{bucket_name}/bronze/api_posts/dt={execution_date}/*/data.parquet"
    )
    silver_path = (
        f"s3://{bucket_name}/silver/user_stats/dt={execution_date}/"
        f"{output_run_id_fragment}/stats.parquet"
    )

    logging.info("Đang đọc dữ liệu từ: %s", bronze_glob)

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_endpoint={_sql_literal(host_port)};")
        con.execute(f"SET s3_access_key_id={_sql_literal(access_key)};")
        con.execute(f"SET s3_secret_access_key={_sql_literal(secret_key)};")
        con.execute(f"SET s3_use_ssl={str(use_ssl).lower()};")
        con.execute("SET s3_url_style='path';")

        sql_query = f"""
            CREATE OR REPLACE TABLE user_stats AS
            SELECT
                userId,
                count(*) AS total_posts,
                max(id) AS latest_post_id,
                {_sql_literal(execution_date)} AS processed_at
            FROM read_parquet({_sql_literal(bronze_glob)})
            GROUP BY userId
        """

        con.execute(sql_query)
        logging.info("Transform dữ liệu thành công!")

        con.execute(f"COPY user_stats TO {_sql_literal(silver_path)} (FORMAT PARQUET)")
        logging.info("Đã lưu kết quả Silver tại: %s", silver_path)
    except Exception as e:
        logging.error("Lỗi trong quá trình Transform: %s", e)
        raise
    finally:
        con.close()
