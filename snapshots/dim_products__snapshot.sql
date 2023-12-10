{% snapshot dim_products__snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_key',
      strategy='timestamp',
      updated_at='date_loaded',
    )
}}

WITH distinct_stg_order_items AS 
(
    SELECT DISTINCT product_key
    FROM {{ ref('stg_sql_server_dbo__order_items') }}
),

distinct_stg_events AS 
(
    SELECT DISTINCT product_key
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

distinct_stg_budget AS 
(
    SELECT DISTINCT product_key
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
    SELECT DISTINCT *
    FROM union_all_with_duplicates
)

SELECT
    product_key,
    name,
    price_usd,
    inventory,
    date_loaded
FROM without_duplicates
FULL JOIN
{{ ref('stg_sql_server_dbo__products') }} AS stg_products
USING (product_key)

{% endsnapshot %}