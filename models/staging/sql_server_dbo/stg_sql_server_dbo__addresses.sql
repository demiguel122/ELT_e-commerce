{{ config(
    materialized='incremental',
    unique_key = 'address_key'
    ) 
}}

WITH src_addresses AS (
    SELECT
        address_id,
        address,
        zipcode::INT as zipcode,
        state,
        country,
        _fivetran_synced AS date_loaded
    FROM {{ source('sql_server_dbo', 'addresses') }}
{% if is_incremental() %}

	  where _fivetran_synced > (select max(date_loaded) from {{ this }})

{% endif %}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['address_id']) }} AS address_key,
    address,
    zipcode,
    state,
    country,
    date_loaded
FROM src_addresses