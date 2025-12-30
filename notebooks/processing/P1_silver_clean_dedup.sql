/* P1 Silver clean up */

USE CATALOG workspace;
USE SCHEMA lakehouse_demo;

/* Dropping table if exsits */
DROP TABLE IF EXISTS silver_orders_quarantine;

/* create a table with bad rows */
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

/* Verifying silver_orders_quarantine */
SELECT quarantine_reason, COUNT(*) AS rows
FROM silver_orders_quarantine
GROUP BY quarantine_reason;

/* droping silver order table */
DROP TABLE IF EXISTS silver_orders;

/* Creating table with good rows */
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

/* verifing the table silver_orders */
SELECT COUNT(*) AS silver_rows FROM silver_orders;
SELECT * FROM silver_orders ORDER BY order_time DESC;

/* cleaning up dedup in silver orders */
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

/* verifying if there is any dedups left in silver_dedup */
SELECT COUNT(*) AS silver_dedup_rows FROM silver_orders_dedup;
