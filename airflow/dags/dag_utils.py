# dags/dag_utils.py
import functools
import re
from datetime import datetime

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
            dag_obj = kwargs.get("dag")
            dag_id = getattr(dag_obj, "dag_id", None) or kwargs.get("dag_id")

            task_obj = kwargs.get("task") or kwargs.get("task_instance") or kwargs.get("ti")
            task_id = getattr(task_obj, "task_id", None) or kwargs.get("task_id")

            logical_date = kwargs.get("logical_date")
            if logical_date is None:
                logical_date = kwargs.get("execution_date") or kwargs.get("ds")

            if logical_date is not None and hasattr(logical_date, "strftime"):
                execution_date_str = logical_date.strftime("%Y-%m-%d %H:%M:%S")
            elif logical_date is not None:
                execution_date_str = str(logical_date)
            else:
                execution_date_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # Make sure we always call Telegram with valid strings.
            dag_id_str = dag_id if dag_id is not None else "unknown_dag"
            task_id_str = task_id if task_id is not None else "unknown_task"

            try:
                send_telegram_message(
                    dag_id=dag_id_str,
                    task_id=task_id_str,
                    execution_date=execution_date_str,
                    exception=e,
                )
            except Exception as send_exc:
                # Do not mask the original task exception.
                # If Telegram fails, the task should still be marked as failed with the original error.
                import logging

                logging.error(
                    "with_telegram_alert: failed to send Telegram message: %s",
                    send_exc,
                )
            raise
    return wrapper
