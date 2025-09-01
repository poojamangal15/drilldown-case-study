CREATE OR REPLACE TABLE `drilldown-case-study.drilldown_core.dim_products` AS
SELECT
  product_id,
  product_name,
  category,
  unit_price,
  description
FROM `drilldown-case-study.drilldown_raw.products`;
