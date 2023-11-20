{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

stg_orders AS (
    SELECT
         order_id,
         user_id,
         to_date(created_at) AS created_date,
         to_time(created_at) AS created_time,
         order_cost::DECIMAL(18, 2) AS order_cost_usd,
         status,
         decode (
            shipping_service,
            'ups', 'ups',
            'usps', 'usps',
            'fedex', 'fedex',
            'dhl', 'dhl',
            '', 'pending'
         ) AS shipping_service,
         shipping_cost::DECIMAL(18, 2) AS shipping_cost_usd,
         order_total::DECIMAL(18, 2) AS order_total_usd,
         address_id,
         to_date(estimated_delivery_at) AS estimated_delivery_date,
         to_time(estimated_delivery_at) AS estimated_delivery_time,
         to_date(delivered_at) AS delivered_date,
         to_time(delivered_at) AS delivered_time,
         CASE 
            WHEN tracking_id = '' THEN 'pending'
            ELSE tracking_id
            END AS tracking_id,
         decode
            (promo_id,
            'task-force', 'task-force',
            'instruction set', 'instruction set',
            'leverage', 'leverage',
            'Optional', 'optional',
            'Mandatory', 'mandatory',
            'Digitized', 'digitized',
            '', 'no promo') AS promo_id,
         _fivetran_synced AS date_loaded
    FROM src_orders
    )

SELECT * FROM stg_orders