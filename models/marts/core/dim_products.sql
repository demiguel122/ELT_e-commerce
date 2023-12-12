{{ config(
    materialized='incremental',
    unique_key = 'product_key'
    ) 
}}

WITH dim_products__snapshot AS (
    SELECT *
    FROM {{ ref('dim_products__snapshot') }}
{% if is_incremental() %}

	  where dim_products__snapshot.date_loaded > (select max(this.date_loaded) from {{ this }} as this)

{% endif %}
)

SELECT
    product_key,
    name,
    price_usd,
    inventory,
    date_loaded
FROM dim_products__snapshot
WHERE dbt_valid_to IS NULL