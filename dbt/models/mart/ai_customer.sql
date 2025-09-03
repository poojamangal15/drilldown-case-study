CREATE OR REPLACE VIEW `drilldown-case-study.drilldown_mart.ai__customer_snapshot` AS
SELECT
  c.company_id,
  c.company_name,
  c.industry,
  c.country,
  c.company_size_bucket,
  ltv.total_revenue,
  ltv.num_deals,
  dso.avg_days_to_pay,
  dso.overdue_amount,
  dso.open_amount
FROM `drilldown-case-study.drilldown_core.dim_companies` c
LEFT JOIN `drilldown-case-study.drilldown_mart.customer_ltv` ltv USING (company_id)
LEFT JOIN `drilldown-case-study.drilldown_mart.finance_dso` dso USING (company_id);
