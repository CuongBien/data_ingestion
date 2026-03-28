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

def extract_data(url: str) -> list:
    """Kéo dữ liệu từ API và xử lý lỗi mạng."""
    logging.info(f"Bắt đầu lấy dữ liệu từ {url}")
    try:
        # Luôn set timeout để tránh treo script mãi mãi
        response = requests.get(url, timeout=10)
        # Bắn lỗi nếu status code không phải 2xx (ví dụ: 404, 500)
        response.raise_for_status() 
        data = response.json()
        logging.info(f"Lấy thành công {len(data)} bản ghi.")
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
    
    # 1. Chuyển list các dictionary thành Pandas DataFrame
    df = pd.DataFrame(raw_data)
    
    if df.empty:
        logging.warning("Cảnh báo: DataFrame rỗng, không có dữ liệu để xử lý.")
        return df

    # 2. Xử lý lỗi thiếu dữ liệu (Missing Values)
    # Giả sử cột 'title' không được để trống, nếu trống thì xóa dòng đó
    df = df.dropna(subset=['title'])
    
    # Giả sử cột 'body' bị thiếu, ta điền giá trị mặc định
    df['body'] = df['body'].fillna("No content available")
    
    # 3. Xử lý sai định dạng (Ép kiểu dữ liệu - Data Casting)
    # Đảm bảo 'id' và 'userId' là số nguyên (integer)
    try:
        df['id'] = df['id'].astype(int)
        df['userId'] = df['userId'].astype(int)
        df['title'] = df['title'].astype(str)
    except ValueError as e:
        logging.error(f"Lỗi định dạng dữ liệu trong quá trình ép kiểu: {e}")
        raise

    logging.info(f"Hoàn thành Transform. Kích thước DataFrame hiện tại: {df.shape}")
    return df

def load_data(df: pd.DataFrame, file_path: str):
    """Lưu dữ liệu xuống Storage (Định dạng Parquet)."""
    if df.empty:
        logging.info("Không có dữ liệu hợp lệ để lưu.")
        return

    logging.info(f"Bắt đầu lưu dữ liệu ra file {file_path}...")
    try:
        # Lưu file Parquet (cần cài engine 'pyarrow' hoặc 'fastparquet')
        df.to_parquet(file_path, engine='pyarrow', index=False)
        logging.info("Lưu dữ liệu thành công!")
    except Exception as e:
        logging.error(f"Lỗi khi lưu file: {e}")
        raise

def main(execution_date: str):
    try:
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

    main(execution_date=date.today().strftime("%Y-%m-%d"))