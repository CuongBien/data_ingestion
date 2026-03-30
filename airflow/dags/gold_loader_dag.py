import logging
from datetime import datetime, timedelta

import duckdb
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # Fix 5
from airflow.hooks.base import BaseHook
from airflow.sdk import Variable
from dag_utils import with_telegram_alert


def load_silver_to_postgres(execution_date: str, silver_run_fragment: str) -> None:
    from airflow.sdk.bases.hook import BaseHook
    from airflow.sdk import Variable

    minio_conn   = BaseHook.get_connection("minio_conn")
    pg_conn      = BaseHook.get_connection("postgres_gold_conn")
    bucket_name  = Variable.get("minio_bucket")

    access_key   = minio_conn.login
    secret_key   = minio_conn.password
    extra        = minio_conn.extra_dejson
    endpoint     = extra.get("endpoint_url", "http://minio:9000").replace("http://", "")

    pg_conn_str  = (
        f"host={pg_conn.host} "
        f"port={pg_conn.port or 5432} "
        f"dbname={pg_conn.schema} "
        f"user={pg_conn.login} "
        f"password={pg_conn.password}"
    )

    silver_path  = (
        f"s3://{bucket_name}/silver/user_stats/dt={execution_date}/"
        f"{silver_run_fragment}/stats.parquet"
    )
    logging.info("Load Silver → Gold: %s", silver_path)

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs;  LOAD httpfs;")
        con.execute("INSTALL postgres; LOAD postgres;")

        con.execute(f"SET s3_endpoint='{endpoint}';")
        con.execute(f"SET s3_access_key_id='{access_key}';")
        con.execute(f"SET s3_secret_access_key='{secret_key}';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")

        con.execute(f"ATTACH '{pg_conn_str}' AS db_gold (TYPE POSTGRES);")

        # Tạo bảng nếu chưa có — nằm ngoài transaction vì DDL không rollback được
        con.execute("""
            CREATE TABLE IF NOT EXISTS db_gold.daily_user_summary (
                userId         INTEGER,
                total_posts    BIGINT,
                latest_post_id INTEGER,
                processed_at   DATE
            );
        """)

        # Wrap DELETE + INSERT trong transaction
        con.execute("BEGIN;")
        try:
            con.execute(f"""
                DELETE FROM db_gold.daily_user_summary
                WHERE processed_at = '{execution_date}';
            """)
            con.execute(f"""
                INSERT INTO db_gold.daily_user_summary
                (userId, total_posts, latest_post_id, processed_at)
                SELECT
                    userId,
                    total_posts,
                    latest_post_id,
                    processed_at
                FROM read_parquet('{silver_path}');
            """)
            con.execute("COMMIT;")
            logging.info("✅ Load Gold thành công, đã COMMIT")

        except Exception as e:
            con.execute("ROLLBACK;")
            logging.error("❌ Lỗi trong transaction, đã ROLLBACK: %s", e)
            raise

    except Exception as e:
        logging.error("❌ Lỗi load Gold: %s", e)
        raise
    finally:
        con.close()


default_args = {
    'owner': 'data_engineer_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


@with_telegram_alert
def run_load_to_postgres_gold(**kwargs) -> None:
    execution_date = kwargs.get("execution_date")
    silver_run_fragment = kwargs.get("silver_run_fragment")
    if execution_date is None or silver_run_fragment is None:
        raise ValueError(
            "gold_layer_to_postgres: missing execution_date or silver_run_fragment."
        )

    load_silver_to_postgres(
        execution_date=str(execution_date),
        silver_run_fragment=str(silver_run_fragment),
    )

with DAG(
    dag_id='gold_layer_to_postgres',
    default_args=default_args,
    schedule=None,               # Fix 3: Được trigger bởi DAG 2, không tự chạy
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['gold', 'postgres'],
) as dag:

    load_to_db = PythonOperator(
        task_id='load_to_postgres_gold',
        python_callable=run_load_to_postgres_gold,
        op_kwargs={
            'execution_date': '{{ dag_run.conf.get("execution_date") }}',
            'silver_run_fragment': '{{ dag_run.conf.get("silver_run_fragment") }}',
        },
    )