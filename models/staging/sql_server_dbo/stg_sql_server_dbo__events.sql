{{ config(
    materialized='incremental',
    unique_key = 'event_key'
    ) 
}}

WITH src_events AS (
    SELECT
        event_id,
        event_type,
        user_id,
        session_id,
        page_url,
        order_id,
        product_id,
        to_date(created_at) AS created_date,
        to_time(created_at) AS created_time_utc,
        _fivetran_synced AS date_loaded
    FROM {{ source('sql_server_dbo', 'events') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(date_loaded) from {{ this }})

{% endif %}
    )

SELECT
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS event_key,
    {{ dbt_utils.generate_surrogate_key(['event_type', 'page_url']) }} AS event_type_key,
    event_type,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
    {{ dbt_utils.generate_surrogate_key(['session_id']) }} AS session_key,
    page_url,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_key,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['created_date']) }} AS created_date_key,
    {{ dbt_utils.generate_surrogate_key(['created_time_utc']) }} AS created_time_utc_key,
    date_loaded
FROM src_events