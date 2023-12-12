{{ config(
    materialized='incremental',
    unique_key = 'event_key'
    ) 
}}

WITH stg_events AS 
(
    SELECT *
    FROM {{ ref("stg_sql_server_dbo__events") }}
{% if is_incremental() %}

	  where stg_sql_server_dbo__events.date_loaded > (select max(this.date_loaded) from {{ this }} as this)

{% endif %}
)

SELECT
    event_key,
    event_type_key,
    user_key,
    session_key,
    order_key,
    product_key,
    created_date_key,
    created_time_utc_key,
    date_loaded
FROM stg_events