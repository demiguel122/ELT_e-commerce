{{
  config(
    materialized='incremental'
  )
}}

WITH dim_products__snapshot AS (
    SELECT * 
    FROM {{ ref('dim_products__snapshot') }}
    )

SELECT
    product_key,
    name,
    price_usd,
    inventory
FROM dim_products__snapshot
WHERE dbt_valid_to IS NULL

