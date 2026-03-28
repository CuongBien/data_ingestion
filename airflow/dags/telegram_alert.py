# dags/telegram_alert.py
import requests
import logging
from airflow.models import Variable

def send_telegram_message(dag_id, task_id, execution_date, exception):
    logging.info("🔔 send_telegram_message ĐƯỢC GỌI!")

    TELEGRAM_BOT_TOKEN = Variable.get("telegram_bot_token")
    TELEGRAM_CHAT_ID   = Variable.get("telegram_chat_id")

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