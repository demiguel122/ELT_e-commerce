{{
  config(
    materialized='view'
  )
}}

WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

stg_products AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
        name,
        price AS price_usd,
        inventory,
        _fivetran_synced AS date_loaded
    FROM src_products
    )

SELECT * 
FROM stg_products