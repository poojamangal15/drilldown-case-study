{{ config(materialized='view', description='Canonicalized deals staging') }}

WITH base AS (
  SELECT
    deal_id,
    deal_name,
    company_id,
    owner_id,
    amount,
    -- normalize: lowercase, collapse underscores/dashes to single space, trim spaces
    TRIM(REGEXP_REPLACE(LOWER(deal_stage), r'[_-]+', ' ')) AS stage_norm
  FROM {{ source('raw','deals') }}
)
SELECT
  deal_id,
  deal_name,
  company_id,
  owner_id,
  amount,
  CASE
    WHEN stage_norm IN ('prospecting','qualification','negotiation','closed won','closed lost','proposal')
      THEN stage_norm
    WHEN stage_norm = 'proposal sent' THEN 'proposal'
    ELSE 'unknown'
  END AS deal_stage_canonical
FROM base
