-- Project 1: Medallion Orders Pipeline (RUN)
-- Rebuild mode: resets tables and rebuilds Bronze -> Silver -> Gold

USE CATALOG workspace;
USE SCHEMA lakehouse_demo;

-- ---------- BRONZE ----------
DROP TABLE IF EXISTS bronze_orders;

CREATE TABLE bronze_orders (
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  price DOUBLE,
  quantity INT,
  order_time STRING
)
USING DELTA;

INSERT INTO bronze_orders
SELECT
  CAST(`order\_id` AS STRING) AS order_id,
  CAST(`customer\_id` AS STRING) AS customer_id,
  CAST(`product\_id` AS STRING) AS product_id,
  CAST(REPLACE(TRIM(price), ',', '.') AS DOUBLE) AS price,
  CAST(TRIM(quantity) AS INT) AS quantity,
  CAST(`order\_time` AS STRING) AS order_time
FROM read_files(
  '/Volumes/workspace/lakehouse_demo/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => false
);

-- ---------- SILVER ----------
DROP TABLE IF EXISTS silver_orders_quarantine;

CREATE TABLE silver_orders_quarantine AS
SELECT
  *,
  CASE
    WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'missing_customer_id'
    WHEN price IS NULL THEN 'missing_price'
    WHEN price <= 0 THEN 'invalid_price'
    WHEN quantity IS NULL THEN 'missing_quantity'
    WHEN quantity <= 0 THEN 'invalid_quantity'
    WHEN TRY_CAST(order_time AS TIMESTAMP) IS NULL THEN 'invalid_order_time'
    ELSE 'unknown_reason'
  END AS quarantine_reason
FROM bronze_orders
WHERE
  customer_id IS NULL OR TRIM(customer_id) = ''
  OR price IS NULL OR price <= 0
  OR quantity IS NULL OR quantity <= 0
  OR TRY_CAST(order_time AS TIMESTAMP) IS NULL;

DROP TABLE IF EXISTS silver_orders;

CREATE TABLE silver_orders AS
SELECT
  order_id,
  customer_id,
  product_id,
  price,
  quantity,
  CAST(order_time AS TIMESTAMP) AS order_time
FROM bronze_orders
WHERE
  customer_id IS NOT NULL AND TRIM(customer_id) <> ''
  AND price > 0
  AND quantity > 0
  AND TRY_CAST(order_time AS TIMESTAMP) IS NOT NULL;

DROP TABLE IF EXISTS silver_orders_dedup;

CREATE TABLE silver_orders_dedup AS
SELECT *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_time DESC) AS rn
  FROM silver_orders
)
WHERE rn = 1;

-- ---------- GOLD ----------
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

-- ---------- FINAL CHECKS ----------
SELECT
  (SELECT COUNT(*) FROM bronze_orders) AS bronze_rows,
  (SELECT COUNT(*) FROM silver_orders) AS silver_rows,
  (SELECT COUNT(*) FROM silver_orders_dedup) AS silver_dedup_rows,
  (SELECT COUNT(*) FROM silver_orders_quarantine) AS quarantine_rows;

SELECT
  (SELECT SUM(price * quantity) FROM silver_orders_dedup) AS silver_total_revenue,
  (SELECT SUM(revenue) FROM gold_daily_revenue) AS gold_total_revenue;


