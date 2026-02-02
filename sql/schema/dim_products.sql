-- Dimension: Products
-- Source: raw_products (from sample_products.xlsx)

CREATE OR REPLACE TABLE dim_products AS
SELECT
    product_id,
    product_name,
    category,
    CAST(unit_price AS DECIMAL(10,2)) as unit_price,
    stock_quantity
FROM raw_products;

-- Add primary key constraint (documentation only - DuckDB doesn't enforce)
-- PRIMARY KEY: product_id
