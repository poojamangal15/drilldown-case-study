CREATE OR REPLACE TABLE `drilldown-case-study.drilldown_core.fact_sales` AS
WITH deals_won AS (
  SELECT *
  FROM `drilldown-case-study.drilldown_raw.deals`
  WHERE LOWER(deal_stage) = 'closed won'
),
il AS (
  SELECT
    invoice_line_id,
    invoice_id,
    product_id,
    CAST(quantity AS INT64) AS quantity,
    CAST(unit_price AS NUMERIC) AS line_unit_price,
    CAST(line_total AS NUMERIC) AS line_amount
  FROM `drilldown-case-study.drilldown_raw.invoice_lines`
),
inv AS (
  SELECT
    invoice_id,
    company_id,
    deal_id,
    DATE(invoice_date) AS invoice_date,
    DATE(due_date) AS due_date,
    CAST(total_amount AS NUMERIC) AS invoice_total_amount,
    status AS payment_status,
    DATE(paid_date) AS paid_date
  FROM `drilldown-case-study.drilldown_raw.invoices`
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
  l.line_amount
FROM deals_won d
JOIN inv i USING (deal_id, company_id)
JOIN il  l USING (invoice_id);



'''
 combines deal, invoice, and invoice line data into a single fact table
'''