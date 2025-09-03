{{ config(materialized='table') }}

WITH items AS (
  SELECT DISTINCT invoice_id, product_id
  FROM {{ ref('fact_sales') }}
),
pairs AS (
  SELECT a.product_id AS product_a, b.product_id AS product_b
  FROM items a
  JOIN items b
    ON a.invoice_id = b.invoice_id
   AND a.product_id < b.product_id
)
SELECT
  p.product_a,
  pa.product_name AS product_a_name,
  p.product_b,
  pb.product_name AS product_b_name,
  COUNT(*) AS together_count,
  CURRENT_TIMESTAMP() AS ingested_at
FROM pairs p
LEFT JOIN {{ ref('dim_products') }} pa ON pa.product_id = p.product_a
LEFT JOIN {{ ref('dim_products') }} pb ON pb.product_id = p.product_b
GROUP BY 1,2,3,4
ORDER BY together_count DESC
