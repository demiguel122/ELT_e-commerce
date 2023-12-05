{{
  config(
    materialized='incremental'
  )
}}

WITH dim_promos__snapshot AS 
(
    SELECT *
    FROM {{ ref('dim_promos__snapshot') }}
)

SELECT
    promo_key,
    promo_name,
    discount_usd,
    status
FROM dim_promos__snapshot
WHERE dbt_valid_to IS NULL