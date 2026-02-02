-- Dimension: Customers
-- Source: raw_customers (from sample_customers.xlsx)

CREATE OR REPLACE TABLE dim_customers AS
SELECT
    customer_id,
    customer_name,
    region,
    segment,
    email,
    CAST(created_date AS DATE) as created_date
FROM raw_customers;

-- Add primary key constraint (documentation only - DuckDB doesn't enforce)
-- PRIMARY KEY: customer_id
