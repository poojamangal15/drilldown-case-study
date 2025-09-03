# Product Bundles: 👉 Example: “70% of customers who buy Analytics also buy Support” → easy upsell.

CREATE OR REPLACE TABLE `drilldown-case-study.drilldown_mart.product_bundles` AS
WITH items AS (
  SELECT DISTINCT invoice_id, product_id
  FROM `drilldown-case-study.drilldown_core.fact_sales`
),
pairs AS (
  SELECT
    a.product_id AS product_a,
    b.product_id AS product_b
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
  COUNT(*) AS together_count
FROM pairs p
LEFT JOIN `drilldown-case-study.drilldown_core.dim_products` pa
  ON pa.product_id = p.product_a
LEFT JOIN `drilldown-case-study.drilldown_core.dim_products` pb
  ON pb.product_id = p.product_b
GROUP BY 1,2,3,4
ORDER BY together_count DESC;
