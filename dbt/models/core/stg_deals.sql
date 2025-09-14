{{ config(
    materialized='view',
    description='Canonicalized deals with all attributes and date validations'
) }}

WITH raw_deals AS (
  SELECT
    deal_id,
    deal_name,
    company_id,
    owner_id,
    amount,
    deal_stage,
    TRIM(REGEXP_REPLACE(LOWER(deal_stage), r'[_-]+', ' ')) AS stage_norm,
    -- Safely cast dates and apply future date validation
    CASE
      WHEN SAFE_CAST(created_date AS DATE) > CURRENT_DATE() THEN NULL
      ELSE SAFE_CAST(created_date AS DATE)
    END AS created_date,
    CASE
      WHEN SAFE_CAST(close_date AS DATE) > CURRENT_DATE() THEN NULL
      ELSE SAFE_CAST(close_date AS DATE)
    END AS close_date
  FROM {{ source('raw','deals') }}
)
SELECT
  deal_id,
  deal_name,
  company_id,
  owner_id,
  amount,
  deal_stage,       -- Include if you need the original stage
  -- Canonicalize the deal stage
  CASE
    WHEN stage_norm IN ('prospecting','qualification','negotiation','closed won','closed lost','proposal')
      THEN stage_norm
    WHEN stage_norm = 'proposal sent' THEN 'proposal'
    ELSE 'unknown'
  END AS deal_stage_canonical,
  created_date,
  close_date
FROM raw_deals