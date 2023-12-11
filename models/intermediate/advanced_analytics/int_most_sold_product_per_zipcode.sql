{{
  config(
    materialized='ephemeral'
  )
}}

WITH ranked_products AS (
    SELECT
        a.zipcode,
        o.created_date_key,
        o.product_key,
        SUM(o.QUANTITY) AS units_sold,
        ROW_NUMBER() OVER(PARTITION BY a.zipcode, o.created_date_key ORDER BY SUM(o.QUANTITY) DESC) AS product_rank
    FROM {{ ref('fct_order_items') }} AS o
    JOIN {{ ref('dim_addresses') }} AS a
    USING (address_key)
    GROUP BY 1, 2, 3
)

SELECT
    zipcode,
    created_date_key,
    product_key AS most_sold_product_key,
    units_sold
FROM ranked_products
WHERE product_rank = 1
ORDER BY zipcode, created_date_key