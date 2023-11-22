{{
  config(
    materialized='table'
  )
}}

WITH distinct_stg_addresses AS 
(
    SELECT DISTINCT address_key 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),

distinct_stg_users AS 
(
    SELECT DISTINCT address_key 
    FROM {{ ref('stg_sql_server_dbo__users') }}
),

union_all_with_duplicates AS 
(
    SELECT *
    FROM distinct_stg_addresses
    UNION ALL
    SELECT *
    FROM distinct_stg_users
),

without_duplicates AS 
(
    SELECT DISTINCT(address_key)
    FROM union_all_with_duplicates
)

SELECT *
FROM without_duplicates
FULL JOIN {{ ref('stg_sql_server_dbo__addresses') }}
USING(address_key)
