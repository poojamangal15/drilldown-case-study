# Sales Rep Performance

{{ config(materialized='table') }}

WITH base AS (
  SELECT owner_id AS sales_rep_id, LOWER(deal_stage) AS stage
  FROM {{ source('raw','deals') }}
)
SELECT
  sales_rep_id,
  COUNT(*) AS total_deals,
  COUNTIF(stage = 'closed won') AS won_deals,
  COUNTIF(stage = 'closed lost') AS lost_deals,
  SAFE_DIVIDE(COUNTIF(stage = 'closed won'), COUNT(*)) AS win_rate,
  CURRENT_TIMESTAMP() AS ingested_at
FROM base
GROUP BY 1
