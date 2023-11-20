{{
  config(
    materialized='table'
  )
}}

WITH stg_order_items AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__order_items') }}
    )
    
SELECT
{{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} AS order_item_key,
{{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key,
{{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_id,
quantity,
date_loaded
FROM stg_order_items