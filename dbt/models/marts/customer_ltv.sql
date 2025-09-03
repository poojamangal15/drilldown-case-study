{{ config(materialized='table', cluster_by=['company_id']) }}

SELECT
  company_id,
  SUM(line_amount) AS total_revenue,
  COUNT(DISTINCT deal_id) AS num_deals,
  COUNT(DISTINCT invoice_id) AS num_invoices,
  AVG(line_amount) AS avg_line_value,
  CURRENT_TIMESTAMP() AS ingested_at
FROM {{ ref('fact_sales') }}
GROUP BY 1
