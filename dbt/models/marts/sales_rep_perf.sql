{{ config(materialized='table', description='Win rates by sales rep, canonical stages used.') }}

WITH base AS (
  SELECT 
    owner_id AS sales_rep_id, 
    deal_stage_canonical AS stage
  FROM {{ ref('stg_deals') }}
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
