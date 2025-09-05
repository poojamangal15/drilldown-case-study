{{ config(materialized='view', description='Canonicalized invoices staging') }}

WITH base AS (
  SELECT
    invoice_id,
    company_id,
    deal_id,
    SAFE_CAST(invoice_date AS DATE) AS invoice_date,
    SAFE_CAST(due_date AS DATE) AS due_date,
    SAFE_CAST(paid_date AS DATE) AS paid_date,
    SAFE_CAST(total_amount AS NUMERIC) AS total_amount,
    TRIM(LOWER(status)) AS status_norm
  FROM {{ source('raw','invoices') }}
)
SELECT
  invoice_id,
  company_id,
  deal_id,
  invoice_date,
  due_date,
  paid_date,
  total_amount,
  CASE
    WHEN status_norm IN ('paid','sent','overdue') THEN status_norm
    -- keep others explicit if your domain requires:
    WHEN status_norm IN ('draft') THEN 'draft'
    -- these two often don't belong in invoice.status; leave as 'unknown' unless your raw truly uses them
    WHEN status_norm IN ('closed won','closed lost') THEN 'unknown'
    ELSE 'unknown'
  END AS payment_status_canonical
FROM base
