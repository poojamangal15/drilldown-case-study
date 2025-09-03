{{ config(materialized='view') }}

SELECT 
  *,
  LOWER(deal_stage) as deal_stage_lower,
  CASE 
    WHEN LOWER(deal_stage) = 'closed won' THEN 'MATCH'
    ELSE 'NO_MATCH'
  END as stage_check
FROM {{ source('raw', 'deals') }}
