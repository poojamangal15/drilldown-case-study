{{ config(materialized='view', description='Deals with canonical stage + created/closed dates') }}

WITH d AS (
  -- your canonical stages from stg_deals
  SELECT
    deal_id,
    deal_stage_canonical
  FROM {{ ref('stg_deals') }}
),
t AS (
  -- pull timestamps from raw; cast safely
  SELECT
    deal_id,
    SAFE_CAST(created_date AS DATE) AS created_date,
    SAFE_CAST(close_date  AS DATE) AS close_date
  FROM {{ source('raw','deals') }}
)
SELECT
  t.deal_id,
  d.deal_stage_canonical,
  t.created_date,
  t.close_date
FROM t
LEFT JOIN d USING (deal_id)
