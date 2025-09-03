{{ config(materialized='view') }}

-- Test 1: Check deals data
WITH test_deals AS (
  SELECT 
    'deals_total' as test_name,
    COUNT(*) as count_value
  FROM {{ source('raw', 'deals') }}
  
  UNION ALL
  
  SELECT 
    'deals_closed_won' as test_name,
    COUNT(*) as count_value
  FROM {{ source('raw', 'deals') }}
  WHERE LOWER(deal_stage) = 'closed won'
  
  UNION ALL
  
  SELECT 
    'deals_stages' as test_name,
    COUNT(DISTINCT deal_stage) as count_value
  FROM {{ source('raw', 'deals') }}
),

-- Test 2: Check invoices data
test_invoices AS (
  SELECT 
    'invoices_total' as test_name,
    COUNT(*) as count_value
  FROM {{ source('raw', 'invoices') }}
  
  UNION ALL
  
  SELECT 
    'invoices_with_deal_id' as test_name,
    COUNT(*) as count_value
  FROM {{ source('raw', 'invoices') }}
  WHERE deal_id IS NOT NULL
),

-- Test 3: Check invoice lines data
test_invoice_lines AS (
  SELECT 
    'invoice_lines_total' as test_name,
    COUNT(*) as count_value
  FROM {{ source('raw', 'invoice_lines') }}
),

-- Test 4: Check join relationships
test_joins AS (
  SELECT 
    'deals_to_invoices_join' as test_name,
    COUNT(*) as count_value
  FROM {{ source('raw', 'deals') }} d
  JOIN {{ source('raw', 'invoices') }} i ON d.deal_id = i.deal_id
  WHERE LOWER(d.deal_stage) = 'closed won'
  
  UNION ALL
  
  SELECT 
    'invoices_to_lines_join' as test_name,
    COUNT(*) as count_value
  FROM {{ source('raw', 'invoices') }} i
  JOIN {{ source('raw', 'invoice_lines') }} il ON i.invoice_id = il.invoice_id
)

SELECT * FROM test_deals
UNION ALL
SELECT * FROM test_invoices
UNION ALL
SELECT * FROM test_invoice_lines
UNION ALL
SELECT * FROM test_joins
ORDER BY test_name
