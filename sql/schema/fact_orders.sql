-- Fact: Orders
-- Source: raw_orders (from sample_orders.xlsx)
-- Foreign Keys: customer_id -> dim_customers, product_id -> dim_products

CREATE OR REPLACE TABLE fact_orders AS
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    CAST(unit_price AS DECIMAL(10,2)) as unit_price,
    CAST(total_amount AS DECIMAL(10,2)) as total_amount,
    CAST(order_date AS DATE) as order_date,
    status
FROM raw_orders;

-- Constraints (documentation only - DuckDB doesn't enforce)
-- PRIMARY KEY: order_id
-- FOREIGN KEY: customer_id REFERENCES dim_customers(customer_id)
-- FOREIGN KEY: product_id REFERENCES dim_products(product_id)
