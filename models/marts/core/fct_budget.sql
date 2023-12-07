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

	  where stg_google_sheets__budget.date_loaded > (select max(this.date_loaded) from {{ this }} as this)

{% endif %}
)

SELECT
    budget_key,
    product_key,
    date_key,
    quantity,
    date_loaded
FROM stg_budget