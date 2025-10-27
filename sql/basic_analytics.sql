-- ============================================================
-- Top 10 Customers by Total Spending
-- ============================================================
SELECT 
    c.id AS customer_id,
    c.name AS customer_name,
    c.country,
    ROUND(SUM(ti.quantity * p.price)::numeric, 2) AS total_spent
FROM transactions t
JOIN customers c ON t.customer_id = c.id
JOIN transaction_items ti ON t.id = ti.transaction_id
JOIN products p ON ti.product_id = p.id
GROUP BY c.id, c.name, c.country
ORDER BY total_spent DESC
LIMIT 10;

-- ============================================================
-- Best-Selling Products by Category
-- ============================================================
SELECT 
    p.category,
    p.id AS product_id,
    p.name AS product_name,
    SUM(ti.quantity) AS total_quantity_sold,
    ROUND(SUM(ti.quantity * p.price)::numeric, 2) AS total_revenue
FROM transaction_items ti
JOIN products p ON ti.product_id = p.id
GROUP BY p.category, p.id, p.name
ORDER BY p.category, total_quantity_sold DESC;

-- ============================================================
-- Monthly Revenue Trends
-- ============================================================
SELECT 
    DATE_TRUNC('month', t.timestamp)::DATE AS month,
    ROUND(SUM(ti.quantity * p.price)::numeric, 2) AS total_revenue
FROM transactions t
JOIN transaction_items ti ON t.id = ti.transaction_id
JOIN products p ON ti.product_id = p.id
GROUP BY month
ORDER BY month;

-- ============================================================
-- Average Order Value by Country
-- ============================================================
SELECT 
    country_orders.country,
    ROUND(AVG(order_value)::numeric, 2) AS avg_order_value
FROM (
    SELECT 
        t.id AS transaction_id,
        c.country,
        SUM(ti.quantity * p.price) AS order_value
    FROM transactions t
    JOIN customers c ON t.customer_id = c.id
    JOIN transaction_items ti ON t.id = ti.transaction_id
    JOIN products p ON ti.product_id = p.id
    GROUP BY t.id, c.country
) AS country_orders
GROUP BY country_orders.country
ORDER BY avg_order_value DESC;
