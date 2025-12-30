/* P1 Gold Aggregates */

USE CATALOG workspace;
USE SCHEMA lakehouse_demo;

-- 1) Daily revenue (KPI table)
DROP TABLE IF EXISTS gold_daily_revenue;

CREATE TABLE gold_daily_revenue AS
SELECT
  DATE(order_time) AS order_date,
  COUNT(*) AS orders_count,
  SUM(price * quantity) AS revenue,
  AVG(price * quantity) AS avg_order_value
FROM silver_orders_dedup
GROUP BY DATE(order_time)
ORDER BY order_date DESC;

-- Verify
SELECT * FROM gold_daily_revenue ORDER BY order_date DESC;

-- 2) Top products by revenue
DROP TABLE IF EXISTS gold_top_products;

CREATE TABLE gold_top_products AS
SELECT
  product_id,
  SUM(quantity) AS total_quantity,
  SUM(price * quantity) AS total_revenue,
  COUNT(*) AS order_lines
FROM silver_orders_dedup
GROUP BY product_id
ORDER BY total_revenue DESC;

-- Verify
SELECT * FROM gold_top_products LIMIT 10;

SELECT
  (SELECT SUM(price * quantity) FROM silver_orders_dedup) AS silver_total_revenue,
  (SELECT SUM(revenue) FROM gold_daily_revenue) AS gold_total_revenue;
