{{ config(
    materialized = 'table',
    cluster_by = ['company_id'],
) }}

SELECT
  fs.company_id,
  dc.company_name,
  SUM(fs.line_amount) AS total_revenue,
  COUNT(DISTINCT fs.deal_id) AS num_deals,
  COUNT(DISTINCT fs.invoice_id) AS num_invoices,
  AVG(fs.line_amount) AS avg_line_value,
  CURRENT_TIMESTAMP() AS ingested_at
FROM {{ ref('fact_sales') }} fs
LEFT JOIN {{ ref('dim_companies') }} dc
  ON fs.company_id = dc.company_id
GROUP BY fs.company_id, dc.company_name
