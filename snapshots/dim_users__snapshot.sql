{% snapshot dim_users__snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_key',
      strategy='timestamp',
      updated_at='date_loaded',
    )
}}

WITH distinct_stg_orders AS 
(
    SELECT DISTINCT user_key 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

distinct_stg_events AS 
(
    SELECT DISTINCT user_key
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

distinct_stg_users AS 
(
    SELECT DISTINCT user_key
    FROM {{ ref('stg_sql_server_dbo__users') }}
),

union_all_with_duplicates AS 
(
    SELECT *
    FROM distinct_stg_orders
    UNION ALL
    SELECT *
    FROM distinct_stg_events
    UNION ALL
    SELECT *
    FROM distinct_stg_users
),

without_duplicates AS 
(
    SELECT DISTINCT(user_key)
    FROM union_all_with_duplicates
)

SELECT *
FROM without_duplicates
FULL JOIN
{{ ref('stg_sql_server_dbo__users') }} AS users
USING (user_key)

{% endsnapshot %}