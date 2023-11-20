{{
  config(
    materialized='view'
  )
}}

WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

renamed_casted AS (
    SELECT
         product_id,
         name,
         price AS price_usd,
         inventory,
         _fivetran_synced AS date_loaded
    FROM src_products
    )

SELECT * FROM renamed_casted