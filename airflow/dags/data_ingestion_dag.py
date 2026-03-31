import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.sdk import Variable

from ingestion_script import main as run_ingestion
from dag_utils import safe_run_id_path_fragment, with_telegram_alert

MINIO_BUCKET = Variable.get("minio_bucket")
LOCAL_DIR    = "/tmp/airflow_data"

default_args = {
    'owner': 'data_engineer_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


@with_telegram_alert
def execute_local_pipeline(**kwargs):
    logical_date   = kwargs["logical_date"]
    execution_date = logical_date.strftime("%Y-%m-%d")
    dag_run        = kwargs["dag_run"]
    run_part       = safe_run_id_path_fragment(dag_run.run_id)

    os.makedirs(LOCAL_DIR, exist_ok=True)
    local_filepath = f"{LOCAL_DIR}/posts_{execution_date}_{run_part}.parquet"
    logging.info("Local extract path: %s", local_filepath)

    run_ingestion(execution_date=execution_date, file_path=local_filepath)
    return local_filepath


@with_telegram_alert
def cleanup_local_file(**kwargs):
    filepath = kwargs['ti'].xcom_pull(task_ids='extract_transform_local')
    if filepath and os.path.exists(filepath):
        os.remove(filepath)
        logging.info("✅ Đã xóa file: %s", filepath)


with DAG(
    dag_id='ingestion_api_to_minio',
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['bronze', 'ingestion'],
) as dag:

    task_process_local = PythonOperator(
        task_id='extract_transform_local',
        python_callable=execute_local_pipeline,
    )

    task_upload_minio = LocalFilesystemToS3Operator(
        task_id='upload_to_minio',
        filename="{{ ti.xcom_pull(task_ids='extract_transform_local') }}",
        dest_key="bronze/api_posts/dt={{ ds }}/{{ dag_run.run_id }}/data.parquet",
        dest_bucket=MINIO_BUCKET,
        aws_conn_id='minio_conn',
        replace=True,
    )

    task_cleanup = PythonOperator(
        task_id='cleanup_local',
        python_callable=cleanup_local_file,
    )

    task_process_local >> task_upload_minio >> task_cleanup