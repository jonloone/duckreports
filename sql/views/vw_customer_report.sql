-- View: Customer Report
-- Customer-level aggregations with lifetime value metrics

CREATE OR REPLACE VIEW vw_customer_report AS
SELECT
    c.customer_id,
    c.customer_name,
    c.region,
    c.segment,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value,
    MIN(f.order_date) as first_order,
    MAX(f.order_date) as last_order
FROM dim_customers c
LEFT JOIN fact_orders f ON c.customer_id = f.customer_id
GROUP BY c.customer_id, c.customer_name, c.region, c.segment
ORDER BY total_revenue DESC;
