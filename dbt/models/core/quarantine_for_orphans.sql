{{ config(materialized='table') }}

SELECT l.*
FROM {{ source('raw','invoice_lines') }} l
LEFT JOIN {{ source('raw','invoices') }} i USING (invoice_id)
WHERE i.invoice_id IS NULL
