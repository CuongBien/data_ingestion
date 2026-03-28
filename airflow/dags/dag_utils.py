# dags/dag_utils.py
import functools
from telegram_alert import send_telegram_message


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
