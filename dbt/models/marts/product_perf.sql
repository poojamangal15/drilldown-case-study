{{ config(materialized='table', cluster_by=['product_id']) }}

SELECT
  fs.product_id,
  dp.product_name,
  dp.category,
  SUM(fs.quantity) AS units_sold,
  SUM(fs.line_amount) AS revenue,
  AVG(fs.line_unit_price) AS avg_price,
  CURRENT_TIMESTAMP() AS ingested_at
FROM {{ ref('fact_sales') }} fs
LEFT JOIN {{ ref('dim_products') }} dp USING (product_id)
GROUP BY 1,2,3
