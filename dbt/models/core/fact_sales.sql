{{ config(
    materialized = 'table',
    cluster_by = ['company_id','invoice_id'],
) }}

WITH deals_won AS (
  SELECT
    deal_id,
    deal_name,
    company_id,
    owner_id,
    amount,
    deal_stage_canonical,
    created_date,
    close_date
  FROM {{ ref('stg_deals') }}
  WHERE deal_stage_canonical = 'closed won'
),
il AS (
  SELECT
    invoice_line_id,
    invoice_id,
    product_id,
    SAFE_CAST(quantity AS INT64) AS quantity,
    SAFE_CAST(unit_price AS NUMERIC) AS line_unit_price,
    SAFE_CAST(line_total AS NUMERIC) AS line_amount
  FROM {{ source('raw','invoice_lines') }}
),
inv AS (
  SELECT
    invoice_id,
    company_id,
    deal_id,
    invoice_date,
    due_date,
    paid_date,
    total_amount AS invoice_total_amount,
    payment_status_canonical AS payment_status
  FROM {{ ref('stg_invoices') }}
)
SELECT
  d.deal_id,
  d.deal_name,
  d.company_id,
  d.owner_id AS sales_owner_id,
  d.amount AS deal_amount,
  i.invoice_id,
  i.invoice_date,
  i.due_date,
  i.paid_date,
  i.payment_status,
  i.invoice_total_amount,
  l.invoice_line_id,
  l.product_id,
  l.quantity,
  l.line_unit_price,
  l.line_amount,
  CURRENT_TIMESTAMP() AS ingested_at
FROM deals_won d
JOIN inv i ON d.deal_id = i.deal_id AND d.company_id = i.company_id
JOIN il  l ON i.invoice_id = l.invoice_id
