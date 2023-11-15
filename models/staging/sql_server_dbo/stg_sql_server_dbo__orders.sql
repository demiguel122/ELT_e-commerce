{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT
         order_id,
         user_id,
         created_at,
         order_cost,
         status,
         shipping_service,
         shipping_cost,
         order_total,
         address_id,
         estimated_delivery_at,
         delivered_at,
         tracking_id,
         promo_id,
         _fivetran_synced AS date_loaded
    FROM src_orders
    )

SELECT * FROM renamed_casted