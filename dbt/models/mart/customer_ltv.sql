# Customer Lifetime Value

CREATE OR REPLACE TABLE `drilldown-case-study.drilldown_mart.customer_ltv` AS
SELECT
  fs.company_id,
  dc.company_name,
  dc.industry,
  dc.country,
  SUM(fs.line_amount) AS total_revenue,
  COUNT(DISTINCT fs.deal_id) AS num_deals,
  COUNT(DISTINCT fs.invoice_id) AS num_invoices,
  AVG(fs.line_amount) AS avg_line_value
FROM `drilldown-case-study.drilldown_core.fact_sales` fs
LEFT JOIN `drilldown-case-study.drilldown_core.dim_companies` dc
  USING (company_id)
GROUP BY fs.company_id, dc.company_name, dc.industry, dc.country
ORDER BY total_revenue DESC;
