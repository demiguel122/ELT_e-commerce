{{ config(
    materialized='incremental',
    unique_key = 'budget_key'
    ) 
}}

WITH stg_budget AS 
(
    SELECT *
    FROM {{ ref('stg_google_sheets__budget') }}
{% if is_incremental() %}

	  where date_loaded > (select max(date_loaded) from {{ this }})

{% endif %}
)

SELECT
    budget_key,
    product_key,
    date_key,
    quantity,
    date_loaded
FROM stg_budget