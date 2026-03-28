import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

from silver_transform import transform_bronze_to_silver
from dag_utils import safe_run_id_path_fragment, with_telegram_alert

MINIO_BUCKET = Variable.get("minio_bucket")

default_args = {
    'owner': 'analytics_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


@with_telegram_alert
def run_silver_layer(**kwargs):
    # Lấy execution_date từ conf (do DAG 1 truyền sang)
    # Fallback về logical_date nếu trigger thủ công
    conf           = kwargs["dag_run"].conf or {}
    execution_date = conf.get("execution_date") or kwargs["logical_date"].strftime("%Y-%m-%d")

    run_part = safe_run_id_path_fragment(kwargs["dag_run"].run_id)
    logging.info(
        "Bắt đầu transform Silver: date=%s silver_run_fragment=%s",
        execution_date,
        run_part,
    )

    transform_bronze_to_silver(
        execution_date=execution_date,
        bucket_name=MINIO_BUCKET,
        output_run_id_fragment=run_part,
        minio_conn_id="minio_conn",
    )


with DAG(
    dag_id='transform_bronze_to_silver',
    default_args=default_args,
    schedule=None,              # Chỉ chạy khi được trigger bởi DAG 1
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['silver', 'transform'],
) as dag:

    task_silver_transform = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=run_silver_layer,
    )