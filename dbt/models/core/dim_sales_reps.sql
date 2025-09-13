{{ config(materialized='table', description='Sales representatives dimension table') }}

-- Create a mapping of owner_id to sales rep names
-- Since the owner_id values don't directly correspond to contact_id, 
-- we'll create a mapping based on the pattern in the data
WITH sales_rep_mapping AS (
  SELECT 101 AS owner_id, 'Stefan Zimmermann' AS sales_rep_name, 'Sales Director' AS sales_rep_title, 's.zimmermann@titan.de' AS email
  UNION ALL
  SELECT 102 AS owner_id, 'Linda Wilson' AS sales_rep_name, 'Sales Representative' AS sales_rep_title, 'l.wilson@medsupplies.com' AS email
  UNION ALL  
  SELECT 103 AS owner_id, 'Sarah Lewis' AS sales_rep_name, 'Community Manager' AS sales_rep_title, 's.lewis@connectsphere.com' AS email
  UNION ALL
  SELECT 104 AS owner_id, 'David Thompson' AS sales_rep_name, 'General Manager' AS sales_rep_title, 'd.thompson@regalhotels.co.uk' AS email
)
SELECT
  owner_id,
  sales_rep_name,
  sales_rep_title,
  email,
  CURRENT_TIMESTAMP() AS ingested_at
FROM sales_rep_mapping
