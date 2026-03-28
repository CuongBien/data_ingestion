from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ingestion_script import main as run_ingestion

default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    'email': ['biencaocuongg@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def execute_pipeline_with_date(**kwargs):
    logical_date = kwargs['logical_date']
    execution_date = logical_date.strftime('%Y-%m-%d')
    run_ingestion(execution_date=execution_date)

with DAG(
    dag_id='daily_api_to_parquet_ingestion',
    default_args=default_args,
    description='Pipeline kéo dữ liệu API lưu thành Parquet hàng ngày',
    schedule='0 2 * * *',          # ← 'schedule', not 'schedule_interval'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingestion', 'api', 'bronze_layer'],
) as dag:

    ingestion_task = PythonOperator(
        task_id='extract_transform_load_api_data',
        python_callable=execute_pipeline_with_date,
        # No provide_context — removed in Airflow 3
    )