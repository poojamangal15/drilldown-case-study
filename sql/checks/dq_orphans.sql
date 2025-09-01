WITH fact AS (
  SELECT company_id, product_id
  FROM `drilldown-case-study.drilldown_core.fact_sales`
),
dimc AS (
  SELECT company_id
  FROM `drilldown-case-study.drilldown_core.dim_companies`
),
dimp AS (
  SELECT product_id
  FROM `drilldown-case-study.drilldown_core.dim_products`
)
SELECT
  (SELECT COUNT(*) FROM fact f LEFT JOIN dimc c USING(company_id) WHERE c.company_id IS NULL) AS orphans_company,
  (SELECT COUNT(*) FROM fact f LEFT JOIN dimp p USING(product_id) WHERE p.product_id IS NULL) AS orphans_product;
