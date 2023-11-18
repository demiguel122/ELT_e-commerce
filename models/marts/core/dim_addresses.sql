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

detecting_duplicates AS 
(
    SELECT
        address_id,
        ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY address_id) AS count
    FROM union_all_with_duplicates
),

only_unique_ids AS 
(
    SELECT address_id
    FROM detecting_duplicates
    WHERE count = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_key,
    address,
    zipcode,
    state,
    country,
    date_loaded
FROM only_unique_ids
LEFT JOIN {{ ref('stg_sql_server_dbo__addresses') }}
USING(address_id)
