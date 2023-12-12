{{ config(
    materialized='incremental',
    unique_key = 'event_type_key'
    ) 
}}

WITH stg_event_types AS (
    SELECT DISTINCT event_type_key, event_type, date_loaded
    FROM {{ ref('stg_sql_server_dbo__events') }}
{% if is_incremental() %}

	  where stg_sql_server_dbo__events.date_loaded > (select max(this.date_loaded) from {{ this }} as this)

{% endif %}
)

SELECT *
FROM stg_event_types