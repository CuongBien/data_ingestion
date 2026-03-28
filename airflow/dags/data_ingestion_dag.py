import requests
import logging
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

from ingestion_script import main as run_ingestion

TELEGRAM_BOT_TOKEN = Variable.get("telegram_bot_token")
TELEGRAM_CHAT_ID   = Variable.get("telegram_chat_id")


def send_telegram_message(dag_id, task_id, execution_date, exception):
    logging.info("🔔 send_telegram_message ĐƯỢC GỌI!")

    message = (
        "🚨 *CẢNH BÁO LỖI DATA PIPELINE* 🚨\n"
        f"- *DAG:* `{dag_id}`\n"
        f"- *Task:* `{task_id}`\n"
        f"- *Thời gian chạy:* {execution_date}\n"
        f"- *Chi tiết lỗi:* `{str(exception)}`"
    )

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                'chat_id': TELEGRAM_CHAT_ID,
                'text': message,
                'parse_mode': 'Markdown',
            },
            timeout=10
        )
        response.raise_for_status()
        logging.info("✅ Gửi Telegram thành công!")
    except Exception as e:
        logging.error(f"❌ Lỗi gửi Telegram: {e}")


default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


def execute_pipeline(**kwargs):
    try:
        logical_date   = kwargs['logical_date']
        execution_date = logical_date.strftime('%Y-%m-%d')
        run_ingestion(execution_date=execution_date)
    except Exception as e:
        send_telegram_message(
            dag_id=kwargs['dag'].dag_id,
            task_id=kwargs['task'].task_id,
            execution_date=kwargs['logical_date'].strftime('%Y-%m-%d %H:%M:%S'),
            exception=e,
        )
        raise


def intentional_fail(**kwargs):
    try:
        raise ValueError("Lỗi 500: Database sập nguồn rồi sếp ơi?!!!")
    except Exception as e:
        send_telegram_message(
            dag_id=kwargs['dag'].dag_id,
            task_id=kwargs['task'].task_id,
            execution_date=kwargs['logical_date'].strftime('%Y-%m-%d %H:%M:%S'),
            exception=e,
        )
        raise  # ← bắt buộc để Airflow vẫn mark Failed


with DAG(
    dag_id='daily_api_to_parquet_ingestion',
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingestion', 'alerting'],
) as dag:

    ingestion_task = PythonOperator(
        task_id='extract_transform_load_api_data',
        python_callable=execute_pipeline,
    )

    test_fail_task = PythonOperator(
        task_id='test_fail_task',
        python_callable=intentional_fail,
    )

    ingestion_task >> test_fail_task