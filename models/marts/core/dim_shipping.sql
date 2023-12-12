{{ config(
    materialized='incremental',
    unique_key = 'shipping_service_key'
    ) 
}}

WITH distinct_services AS (
    SELECT DISTINCT shipping_service_key, shipping_service, date_loaded
    FROM {{ ref('stg_sql_server_dbo__orders') }}
{% if is_incremental() %}

	  where stg_sql_server_dbo__orders.date_loaded > (select max(this.date_loaded) from {{ this }} as this)

{% endif %}
)

SELECT
    shipping_service_key,
    shipping_service,
    date_loaded
FROM distinct_services