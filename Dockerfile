# Sử dụng Python image nhẹ gọn
FROM python:3.10-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Copy requirements và cài đặt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY ingestion_script.py .

# Lệnh mặc định khi container chạy
CMD ["python", "ingestion_script.py"]