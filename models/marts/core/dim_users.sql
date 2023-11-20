{{
  config(
    materialized='table'
  )
}}

WITH distinct_stg_orders AS 
(
    SELECT DISTINCT user_id 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

distinct_stg_events AS 
(
    SELECT DISTINCT user_id 
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

distinct_stg_users AS 
(
    SELECT DISTINCT user_id 
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
    SELECT DISTINCT(user_id)
    FROM union_all_with_duplicates
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} AS user_key,
    first_name,
    last_name,
    email,
    phone_number,
    {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_key,
    {{ dbt_utils.generate_surrogate_key(['created_date']) }} AS created_date_key,
    {{ dbt_utils.generate_surrogate_key(['created_time_utc']) }} AS created_time_utc_key,
    {{ dbt_utils.generate_surrogate_key(['updated_date']) }} AS updated_date_key,
    {{ dbt_utils.generate_surrogate_key(['updated_time_utc']) }} AS updated_time_utc_key
FROM without_duplicates
FULL JOIN
{{ ref('stg_sql_server_dbo__users') }} AS users
USING (user_id)