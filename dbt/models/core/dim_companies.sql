{{ config(
    materialized='table',
    cluster_by=['industry', 'country_full_name']
) }}

SELECT
  company_id,
  company_name,
  industry,
  country AS country_code,
  CASE
    WHEN country = 'USA' THEN 'United States'
    WHEN country = 'US' THEN 'United States'
    WHEN country = 'GB' THEN 'United Kingdom'
    WHEN country = 'UK' THEN 'United Kingdom'
    WHEN country = 'NL' THEN 'Netherlands'
    WHEN country = 'DE' THEN 'Germany'
    WHEN country = 'FR' THEN 'France'
    WHEN country = 'ES' THEN 'Spain'
    WHEN country = 'IT' THEN 'Italy'
    ELSE country
  END AS country_full_name,
  SAFE_CAST(annual_revenue AS NUMERIC) AS annual_revenue,
  SAFE_CAST(employee_count AS INT64) AS employee_count,
  CASE
    WHEN SAFE_CAST(employee_count AS INT64) IS NULL THEN 'unknown'
    WHEN SAFE_CAST(employee_count AS INT64) < 100 THEN 'small'
    WHEN SAFE_CAST(employee_count AS INT64) < 350 THEN 'mid'
    ELSE 'enterprise'
  END AS company_size_bucket,
  SAFE_CAST(created_date AS DATE) AS created_date,
  owner_id AS account_owner_id,
  CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('raw','companies') }}