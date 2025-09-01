# Product Performance 

CREATE OR REPLACE TABLE `drilldown-case-study.drilldown_mart.product_perf` AS
SELECT
  fs.product_id,
  dp.product_name,
  dp.category,
  SUM(fs.quantity) AS units_sold,
  SUM(fs.line_amount) AS revenue,
  AVG(fs.line_unit_price) AS avg_price
FROM `drilldown-case-study.drilldown_core.fact_sales` fs
LEFT JOIN `drilldown-case-study.drilldown_core.dim_products` dp
  USING (product_id)
GROUP BY 1,2,3;
