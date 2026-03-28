import os
import logging
import requests
import pandas as pd
from requests.exceptions import HTTPError, ConnectionError, Timeout

# Cấu hình Logging chuẩn (Thay vì dùng print)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_URL = "https://jsonplaceholder.typicode.com/posts"
# Mounted in docker-compose as host ./data -> /opt/airflow/data
OUTPUT_DIR: str = os.environ.get("INGESTION_OUTPUT_DIR", "/opt/airflow/data")

# Bronze schema for posts API — dùng cho DataFrame/Parquet rỗng và tránh KeyError khi API trả rác
POST_SCHEMA_COLUMNS: list[str] = ["id", "userId", "title", "body"]


def extract_data(url: str) -> list:
    """Kéo dữ liệu từ API và xử lý lỗi mạng."""
    logging.info(f"Bắt đầu lấy dữ liệu từ {url}")
    try:
        # Luôn set timeout để tránh treo script mãi mãi
        response = requests.get(url, timeout=10)
        # Bắn lỗi nếu status code không phải 2xx (ví dụ: 404, 500)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            logging.error(
                "ingestion_script: API JSON không phải mảng (type=%s) — từ chối ingest.",
                type(data).__name__,
            )
            raise ValueError(
                "Expected JSON array from API, got " + type(data).__name__
            )
        if len(data) == 0:
            logging.warning(
                "ingestion_script: API trả [] — không có bản ghi (pipeline vẫn chạy an toàn)."
            )
        else:
            logging.info("Lấy thành công %d phần tử từ API.", len(data))
        return data
    
    except Timeout:
        logging.error("Lỗi: API phản hồi quá lâu (Timeout).")
        raise
    except ConnectionError:
        logging.error("Lỗi: Không thể kết nối tới API (Mất mạng/Sai URL).")
        raise
    except HTTPError as e:
        logging.error(f"Lỗi HTTP: {e}")
        raise
    except Exception as e:
        logging.error(f"Lỗi không xác định: {e}")
        raise

def transform_data(raw_data: list) -> pd.DataFrame:
    """Xử lý sơ bộ, làm sạch và ép kiểu dữ liệu."""
    logging.info("Bắt đầu xử lý dữ liệu (Transform)...")

    if len(raw_data) == 0:
        logging.warning("Transform: input [] — trả về DataFrame rỗng đúng schema.")
        return pd.DataFrame(columns=POST_SCHEMA_COLUMNS)

    dict_rows: list[dict] = [x for x in raw_data if isinstance(x, dict)]
    skipped: int = len(raw_data) - len(dict_rows)
    if skipped > 0:
        logging.warning(
            "Transform: bỏ qua %d phần tử không phải object JSON.",
            skipped,
        )
    if len(dict_rows) == 0:
        logging.warning("Transform: không còn object hợp lệ sau lọc — schema rỗng.")
        return pd.DataFrame(columns=POST_SCHEMA_COLUMNS)

    df = pd.DataFrame(dict_rows)
    for col in POST_SCHEMA_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
            logging.info("Transform: thêm cột thiếu %s (NA).", col)

    df = df[POST_SCHEMA_COLUMNS]

    df = df.dropna(subset=["title"])
    df["body"] = df["body"].fillna("No content available")

    if df.empty:
        logging.warning(
            "Transform: không còn dòng sau dropna(title) — trả về schema rỗng."
        )
        return pd.DataFrame(columns=POST_SCHEMA_COLUMNS)

    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df["userId"] = pd.to_numeric(df["userId"], errors="coerce")
    before_rows: int = len(df)
    df = df.dropna(subset=["id", "userId"])
    dropped_numeric: int = before_rows - len(df)
    if dropped_numeric > 0:
        logging.warning(
            "Transform: loại %d dòng id/userId không ép được số.",
            dropped_numeric,
        )

    if df.empty:
        logging.warning(
            "Transform: không còn dòng sau lọc id/userId — schema rỗng."
        )
        return pd.DataFrame(columns=POST_SCHEMA_COLUMNS)

    try:
        df["id"] = df["id"].astype(int)
        df["userId"] = df["userId"].astype(int)
        df["title"] = df["title"].astype(str)
    except (ValueError, TypeError) as e:
        logging.error("Lỗi định dạng khi ép kiểu cuối: %s", e)
        raise ValueError("Invalid data types after cleaning; see logs.") from e

    logging.info("Hoàn thành Transform. shape=%s", df.shape)
    return df

def load_data(df: pd.DataFrame, file_path: str) -> None:
    """Lưu Parquet. DataFrame rỗng vẫn ghi file chỉ có schema (tránh downstream thiếu file)."""
    parent = os.path.dirname(file_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    if df.empty:
        logging.warning(
            "load_data: 0 dòng — ghi Parquet schema-only tại %s (không crash pipeline).",
            file_path,
        )
        pd.DataFrame(columns=POST_SCHEMA_COLUMNS).to_parquet(
            file_path, engine="pyarrow", index=False
        )
        return

    logging.info("Bắt đầu lưu dữ liệu ra file %s...", file_path)
    try:
        df.to_parquet(file_path, engine="pyarrow", index=False)
        logging.info("Lưu dữ liệu thành công!")
    except Exception as e:
        logging.error("Lỗi khi lưu file: %s", e)
        raise

def main(execution_date: str, file_path: str | None) -> None:
    try:
        if file_path is not None:
            output_file = file_path
            parent = os.path.dirname(output_file)
            if parent:
                os.makedirs(parent, exist_ok=True)
            logging.info("ingestion_script: writing parquet to explicit path %s", output_file)
        else:
            logging.info(
                "ingestion_script: OUTPUT_DIR=%s (set INGESTION_OUTPUT_DIR to override)",
                OUTPUT_DIR,
            )
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            output_file = os.path.join(OUTPUT_DIR, f"posts_data_{execution_date}.parquet")
            logging.info("ingestion_script: writing parquet to %s", output_file)
        raw_data = extract_data(API_URL)
        clean_df = transform_data(raw_data)
        load_data(clean_df, output_file)
        
    except Exception as e:
        logging.error("Pipeline thất bại.")
        raise

if __name__ == "__main__":
    from datetime import date

    main(execution_date=date.today().strftime("%Y-%m-%d"), file_path=None)