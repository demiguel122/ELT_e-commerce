{{ config(
    materialized='incremental',
    unique_key = 'promo_key'
    ) 
}}

WITH dim_promos__snapshot AS 
(
    SELECT *
    FROM {{ ref('dim_promos__snapshot') }}
{% if is_incremental() %}

	  where dim_promos__snapshot.date_loaded > (select max(this.date_loaded) from {{ this }} as this)

{% endif %}
)

SELECT
    promo_key,
    promo_name,
    discount_usd,
    status,
    date_loaded
FROM dim_promos__snapshot
WHERE dbt_valid_to IS NULL