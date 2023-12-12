{{ config(
    materialized='incremental',
    unique_key = 'user_key'
    ) 
}}

WITH dim_users__snapshot AS 
(
    SELECT * 
    FROM {{ ref('dim_users__snapshot') }}
{% if is_incremental() %}

	  where dim_users__snapshot.date_loaded > (select max(this.date_loaded) from {{ this }} as this)

{% endif %}
)

SELECT
    user_key,
    first_name,
    last_name,
    email,
    phone_number,
    address_key,
    created_date,
    created_time_utc,
    updated_date,
    updated_time_utc,
    date_loaded
FROM dim_users__snapshot
WHERE dbt_valid_to IS NULL