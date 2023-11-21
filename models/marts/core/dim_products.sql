{{
  config(
    materialized='table'
  )
}}

WITH distinct_stg_order_items AS 
(
    SELECT DISTINCT product_id 
    FROM {{ ref('stg_sql_server_dbo__order_items') }}
),

distinct_stg_events AS 
(
    SELECT DISTINCT product_id 
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

distinct_stg_budget AS 
(
    SELECT DISTINCT product_id 
    FROM {{ ref('stg_google_sheets__budget') }}
),

union_all_with_duplicates AS 
(
    SELECT *
    FROM distinct_stg_order_items
    UNION ALL
    SELECT *
    FROM distinct_stg_events
    UNION ALL
    SELECT *
    FROM distinct_stg_budget
),

without_duplicates AS 
(
    SELECT DISTINCT product_id
    FROM union_all_with_duplicates
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    name,
    price_usd,
    inventory,
    date_loaded
FROM without_duplicates
FULL JOIN
{{ ref('stg_sql_server_dbo__products') }} AS products
USING (product_id)