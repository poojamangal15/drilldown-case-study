SELECT
  COUNTIF(company_id IS NULL) AS null_company_in_fact_sales,
  COUNTIF(product_id IS NULL) AS null_product_in_fact_sales
FROM `drilldown-case-study.drilldown_core.fact_sales`;
