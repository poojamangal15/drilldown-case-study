{{ config(materialized='view') }}

SELECT 
  deal_stage,
  LOWER(deal_stage) as deal_stage_lower,
  COUNT(*) as count,
  CASE 
    WHEN LOWER(deal_stage) = 'closed won' THEN 'MATCHES_FILTER'
    ELSE 'DOES_NOT_MATCH'
  END as filter_result
FROM {{ source('raw', 'deals') }}
GROUP BY deal_stage
ORDER BY count DESC
