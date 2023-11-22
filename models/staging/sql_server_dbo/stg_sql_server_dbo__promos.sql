{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT
        lower(promo_id) AS promo_id,
        discount AS discount_usd,
        status,
        _fivetran_synced AS date_loaded
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

new_promo AS (
    SELECT *
    FROM src_promos
    UNION ALL
    SELECT 'no promo', 0, 'not applicable'
    FROM src_promos
    )

SELECT
    {{ dbt_utils.generate_surrogate_key(['promo_id']) }} promo_key,
    promo_id as promo_name,  
    discount_usd,
    status,
    date_loaded
FROM new_promo