# Finance DSO / Days Sales Outstanding

CREATE OR REPLACE TABLE `drilldown-case-study.drilldown_mart.finance_dso` AS
WITH inv AS (
  SELECT
    company_id,
    invoice_id,
    DATE(invoice_date) AS invoice_date,
    DATE(due_date) AS due_date,
    DATE(paid_date) AS paid_date,
    status,
    CAST(total_amount AS NUMERIC) AS total_amount
  FROM `drilldown-case-study.drilldown_raw.invoices`
)
SELECT
  company_id,
  AVG(CASE WHEN status = 'Paid' AND paid_date IS NOT NULL
           THEN DATE_DIFF(paid_date, invoice_date, DAY) END) AS avg_days_to_pay,
  SUM(CASE WHEN status = 'Overdue' THEN total_amount ELSE 0 END) AS overdue_amount,
  SUM(CASE WHEN status = 'Sent' THEN total_amount ELSE 0 END) AS open_amount
FROM inv
GROUP BY 1;
