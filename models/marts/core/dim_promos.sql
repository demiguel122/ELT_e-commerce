{{
  config(
    materialized='table'
  )
}}

WITH stg_promos AS 
(
    SELECT
        promo_id,
        discount,
        status
    FROM {{ ref('stg_sql_server_dbo__promos') }}
),

new_promo AS 
(
    SELECT *
    FROM stg_promos
    UNION ALL
    SELECT 'no promo', 0, 'not applicable'
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['promo_id']) }} promo_key,
    promo_id as promo_name,  
    discount,
    status
FROM new_promo