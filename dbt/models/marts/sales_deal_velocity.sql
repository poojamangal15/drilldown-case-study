-- models/marts/sales_deal_velocity.sql

-- This model calculates the sales velocity for each deal and flags deals that have "slipped" into a future quarter.
-- Sales velocity is a critical metric for understanding the efficiency of the sales process.
-- Deal slippage is a major concern for sales leadership as it impacts revenue forecasting.

WITH deals AS (
    -- Select all relevant fields from the staged deals table.
    -- We are only interested in deals that have a definitive outcome (Closed Won or Closed Lost).
    SELECT
        deal_id,
        owner_id AS sales_rep_id,
        created_date,
        close_date,
        deal_stage,
        amount
    FROM {{ ref('stg_deals') }}
    WHERE deal_stage IN ('Closed Won', 'Closed Lost')
)

SELECT
    d.deal_id,
    d.sales_rep_id,
    sr.sales_rep_name,
    d.created_date,
    d.close_date,
    d.deal_stage,
    d.amount,

    -- Calculate Sales Velocity: the number of days from deal creation to closure.
    -- This metric helps identify which reps or deal types close faster.
    DATE_DIFF(d.close_date, d.created_date, DAY) AS days_to_close,

    -- Calculate Deal Slippage: Flag deals that were closed in a later quarter than they were created.
    -- This is a key indicator of deals that are taking longer than expected and may be at risk.
    -- A value of 1 indicates the deal slipped; 0 means it closed in the same quarter.
    CASE
        WHEN EXTRACT(QUARTER FROM d.close_date) != EXTRACT(QUARTER FROM d.created_date)
        THEN 1
        ELSE 0
    END AS deal_slippage_flag

FROM deals d
LEFT JOIN {{ ref('dim_sales_reps') }} sr
    ON d.sales_rep_id = sr.sales_rep_id
-- models/marts/customer_cohort_analysis.sql

-- This model performs a cohort analysis to track customer retention and revenue over time.
-- Cohorts are defined by the month each company was acquired.
-- This analysis is crucial for understanding long-term customer value and the effectiveness of acquisition strategies.

WITH company_cohorts AS (
    -- Step 1: Define the cohort for each company based on their creation date.
    -- The cohort is the first day of the month they were created.
    SELECT
        company_id,
        DATE_TRUNC(created_date, MONTH) AS cohort_month
    FROM {{ ref('dim_companies') }}
),

cohort_revenue AS (
    -- Step 2: Join the cohorts with sales data to track monthly revenue from each cohort.
    SELECT
        cc.company_id,
        cc.cohort_month,
        DATE_TRUNC(fs.invoice_date, MONTH) AS invoice_month,
        SUM(fs.line_total) AS monthly_revenue
    FROM {{ ref('fact_sales') }} fs
    JOIN company_cohorts cc
        ON fs.company_id = cc.company_id
    GROUP BY 1, 2, 3
)

-- Step 3: Aggregate the data to calculate cohort metrics like retention and cumulative revenue.
SELECT
    cr.cohort_month,
    EXTRACT(YEAR FROM cr.invoice_month) AS invoice_year,
    EXTRACT(MONTH FROM cr.invoice_month) AS invoice_month_number,

    -- Calculate the number of months that have passed since the cohort was acquired.
    DATE_DIFF(cr.invoice_month, cr.cohort_month, MONTH) AS months_after_acquisition,

    -- Count the number of unique customers from the cohort who made a purchase in this period.
    COUNT(DISTINCT cr.company_id) AS active_customers,

    SUM(cr.monthly_revenue) AS total_monthly_revenue

FROM cohort_revenue cr
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4
