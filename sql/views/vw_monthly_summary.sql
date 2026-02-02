-- View: Monthly Summary
-- Aggregated order metrics by month

CREATE OR REPLACE VIEW vw_monthly_summary AS
SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(DISTINCT order_id) as order_count,
    COUNT(DISTINCT customer_id) as customer_count,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value
FROM fact_orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
