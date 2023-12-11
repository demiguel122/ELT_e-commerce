{{ config(
    materialized='incremental',
    unique_key = 'status_key'
    ) 
}}

WITH distinct_statuses AS 
(
    SELECT DISTINCT status_key, status, date_loaded
    FROM {{ ref('stg_sql_server_dbo__orders') }}
{% if is_incremental() %}

	  where stg_sql_server_dbo__orders.date_loaded > (select max(this.date_loaded) from {{ this }} as this)

{% endif %}
)

SELECT
    status_key,
    status,
    date_loaded
FROM distinct_statuses