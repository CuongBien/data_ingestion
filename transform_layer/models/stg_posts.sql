{{ config(materialized='view') }}
SELECT 
    id,
    "userId" as user_id,
    title,
    ingested_at 
FROM raw.api_posts -- Bảng thô ta đẩy vào từ Airflow    