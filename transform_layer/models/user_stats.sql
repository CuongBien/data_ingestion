{{ config(materialized='table') }}
SELECT 
    user_id,
    count(*) as total_posts 
FROM {{ ref('stg_posts') }}
GROUP BY 1