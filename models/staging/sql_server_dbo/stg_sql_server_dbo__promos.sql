{{
  config(
    materialized='view'
  )
}}

WITH base_promos AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__promos') }}
    )

SELECT
    decode
        (promo_id,
        'task-force', 'task_force',
        'instruction set', 'instruction set',
        'leverage', 'leverage',
        'Optional', 'optional',
        'Mandatory', 'mandatory',
        'Digitized', 'digitized',
        '', 'no promo') as promo_id,
    discount,
    status,
    date_loaded
FROM base_promos