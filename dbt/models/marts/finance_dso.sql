{{ config(materialized='table', cluster_by=['company_id']) }}

WITH inv AS (
  SELECT
    company_id,
    SAFE_CAST(invoice_date AS DATE) AS invoice_date,
    SAFE_CAST(due_date AS DATE) AS due_date,
    SAFE_CAST(paid_date AS DATE) AS paid_date,
    LOWER(status) AS status,
    SAFE_CAST(total_amount AS NUMERIC) AS total_amount
  FROM {{ source('raw','invoices') }}
)
SELECT
  company_id,
  AVG(CASE WHEN status = 'paid' AND paid_date IS NOT NULL
           THEN DATE_DIFF(paid_date, invoice_date, DAY) END) AS avg_days_to_pay,
  SUM(CASE WHEN status = 'overdue' THEN total_amount ELSE 0 END) AS overdue_amount,
  SUM(CASE WHEN status = 'sent' THEN total_amount ELSE 0 END) AS open_amount,
  CURRENT_TIMESTAMP() AS ingested_at
FROM inv
GROUP BY 1
