# Sales Rep Performance

CREATE OR REPLACE TABLE `drilldown-case-study.drilldown_mart.sales_rep_perf` AS
WITH base AS (
  SELECT owner_id AS sales_rep_id, LOWER(deal_stage) AS stage
  FROM `drilldown-case-study.drilldown_raw.deals`
)
SELECT
  sales_rep_id,
  COUNT(*) AS total_deals,
  COUNTIF(stage = 'closed won') AS won_deals,
  COUNTIF(stage = 'closed lost') AS lost_deals,
  SAFE_DIVIDE(COUNTIF(stage = 'closed won'), COUNT(*)) AS win_rate
FROM base
GROUP BY 1;
