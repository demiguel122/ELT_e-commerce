{{
  config(
    materialized='table'
  )
}}

WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    ),

fact_orders AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key,
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
        {{ dbt_utils.generate_surrogate_key(['created_date']) }} AS created_date_key,
        {{ dbt_utils.generate_surrogate_key(['created_time']) }} AS created_time_key,
         order_cost,
        {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_key,
        {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} AS shipping_service_key,
         shipping_cost,
         order_total,
        {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_key,
        {{ dbt_utils.generate_surrogate_key(['estimated_delivery_date']) }} AS estimated_delivery_date_key,
        {{ dbt_utils.generate_surrogate_key(['estimated_delivery_time']) }} AS estimated_delivery_time_key,
        {{ dbt_utils.generate_surrogate_key(['delivered_date']) }} AS delivered_date_key,
        {{ dbt_utils.generate_surrogate_key(['delivered_time']) }} AS delivered_time_key,
         tracking_id,
        {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_key,
         date_loaded
    FROM stg_orders
    )

SELECT * FROM fact_orders