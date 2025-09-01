CREATE OR REPLACE TABLE `drilldown-case-study.drilldown_core.dim_companies` AS
SELECT
  company_id,
  company_name,
  industry,
  country,
  annual_revenue,
  employee_count,
  CASE
    WHEN employee_count IS NULL THEN 'unknown'
    WHEN employee_count < 50 THEN 'small'
    WHEN employee_count < 250 THEN 'mid'
    ELSE 'enterprise'
  END AS company_size_bucket,
  created_date,
  owner_id AS account_owner_id
FROM `drilldown-case-study.drilldown_raw.companies`;
