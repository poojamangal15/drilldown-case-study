{{ config(materialized='table', cluster_by=['category']) }}

SELECT
  product_id,
  product_name,
  category,
  SAFE_CAST(unit_price AS NUMERIC) AS unit_price,
  description,
  CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('raw','products') }}
