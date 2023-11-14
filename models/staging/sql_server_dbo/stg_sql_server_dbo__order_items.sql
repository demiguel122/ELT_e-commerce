{{
  config(
    materialized='view'
  )
}}

WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

renamed_casted AS (
    SELECT
         order_id,
         product_id,
         quantity,
         _fivetran_synced AS date_load
    FROM src_order_items
    )

SELECT * FROM renamed_casted