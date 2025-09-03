{{ config(materialized='view') }}

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
FROM {{ ref('dim_companies') }} c
LEFT JOIN {{ ref('customer_ltv') }} ltv USING (company_id)
LEFT JOIN {{ ref('finance_dso') }} dso USING (company_id)
