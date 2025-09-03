{{ config(materialized='table', cluster_by=['industry','country']) }}

SELECT
  company_id,  -- keep as INT
  company_name,
  industry,
  country,
  SAFE_CAST(annual_revenue AS NUMERIC) AS annual_revenue,
  SAFE_CAST(employee_count AS INT64) AS employee_count,
  CASE
    WHEN SAFE_CAST(employee_count AS INT64) IS NULL THEN 'unknown'
    WHEN SAFE_CAST(employee_count AS INT64) < 50 THEN 'small'
    WHEN SAFE_CAST(employee_count AS INT64) < 250 THEN 'mid'
    ELSE 'enterprise'
  END AS company_size_bucket,
  SAFE_CAST(created_date AS DATE) AS created_date,
  owner_id AS account_owner_id,
  CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('raw','companies') }}
