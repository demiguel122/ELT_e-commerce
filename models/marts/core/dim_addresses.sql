{{
  config(
    materialized='table'
  )
}}

WITH distinct_stg_addresses AS 
(
    SELECT DISTINCT address_id 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),

distinct_stg_users AS 
(
    SELECT DISTINCT address_id 
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
    SELECT DISTINCT(address_id)
    FROM union_all_with_duplicates
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_key,
    address,
    zipcode,
    state,
    country,
    date_loaded
FROM without_duplicates
FULL JOIN {{ ref('stg_sql_server_dbo__addresses') }}
USING(address_id)
