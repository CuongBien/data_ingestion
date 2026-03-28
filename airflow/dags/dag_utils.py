# dags/dag_utils.py
import functools
import re
from telegram_alert import send_telegram_message


def safe_run_id_path_fragment(run_id: str) -> str:
    """Filesystem / S3-safe fragment from Airflow dag_run.run_id."""
    return re.sub(r"[^\w\-.]", "_", run_id)


def with_telegram_alert(func):
    """Decorator tự động gửi Telegram khi task fail."""
    @functools.wraps(func)
    def wrapper(**kwargs):
        try:
            return func(**kwargs)
        except Exception as e:
            send_telegram_message(
                dag_id=kwargs['dag'].dag_id,
                task_id=kwargs['task'].task_id,
                execution_date=kwargs['logical_date'].strftime('%Y-%m-%d %H:%M:%S'),
                exception=e,
            )
            raise
    return wrapper
