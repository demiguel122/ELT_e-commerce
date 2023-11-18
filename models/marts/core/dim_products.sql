{{
  config(
    materialized='table'
  )
}}

WITH stg_products AS 
(
    SELECT *
    FROM {{ ref("stg_sql_server_dbo__products") }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    name,
    price,
    inventory,
    date_loaded
FROM stg_products