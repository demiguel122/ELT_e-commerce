{{
  config(
    materialized='view'
  )
}}

WITH src_order_items AS (
    SELECT
        order_id,
        product_id,
        quantity::INT AS quantity,
        _fivetran_synced AS date_loaded
    FROM {{ source('sql_server_dbo', 'order_items') }}
    )

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} AS order_item_key,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    quantity,
    date_loaded
FROM src_order_items