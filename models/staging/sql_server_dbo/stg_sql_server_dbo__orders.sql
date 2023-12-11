{{ config(
    materialized='incremental',
    unique_key = 'order_key'
    ) 
}}

WITH src_orders AS (
    SELECT
        order_id,
        user_id,
        to_date(created_at) AS created_date,
        to_time(created_at) AS created_time_utc,
        order_cost::DECIMAL(7, 2) AS order_cost_usd,
        status,
        decode (
            shipping_service,
            'ups', 'ups',
            'usps', 'usps',
            'fedex', 'fedex',
            'dhl', 'dhl',
            '', 'pending'
         ) AS shipping_service,
        shipping_cost::DECIMAL(7, 2) AS shipping_cost_usd,
        order_total::DECIMAL(7, 2) AS order_total_usd,
        address_id,
        to_date(estimated_delivery_at) AS estimated_delivery_date,
        to_time(estimated_delivery_at) AS estimated_delivery_time_utc,
        to_date(delivered_at) AS delivered_date,
        to_time(delivered_at) AS delivered_time_utc,
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
    FROM {{ source('sql_server_dbo', 'orders') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(date_loaded) from {{ this }})

{% endif %}
    )

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
    {{ dbt_utils.generate_surrogate_key(['created_date']) }} AS created_date_key,
    created_date,
    {{ dbt_utils.generate_surrogate_key(['created_time_utc']) }} AS created_time_utc_key,
    created_time_utc,
    order_cost_usd,
    {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_key,
    status,
    {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} AS shipping_service_key,
    shipping_service,
    shipping_cost_usd,
    order_total_usd,
    {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_key,
    {{ dbt_utils.generate_surrogate_key(['estimated_delivery_date']) }} AS estimated_delivery_date_key,
    estimated_delivery_date,
    {{ dbt_utils.generate_surrogate_key(['estimated_delivery_time_utc']) }} AS estimated_delivery_time_utc_key,
    estimated_delivery_time_utc,
    {{ dbt_utils.generate_surrogate_key(['delivered_date']) }} AS delivered_date_key,
    delivered_date,
    {{ dbt_utils.generate_surrogate_key(['delivered_time_utc']) }} AS delivered_time_utc_key,
    delivered_time_utc,
    tracking_id,
    {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_key,
    date_loaded
FROM src_orders