-- Project 1: Medallion Orders
-- Layer: Bronze 

USE CATALOG workspace;
USE SCHEMA lakehouse_demo;

-- 1) Reset 
-- DROP TABLE IF EXISTS bronze_orders;

-- 2) Create Bronze table
CREATE TABLE bronze_orders (
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  price DOUBLE,
  quantity INT,
  order_time STRING
)
USING DELTA;

-- 3) Load raw file with explicit casting
INSERT INTO bronze_orders
SELECT
  CAST(`order\_id` AS STRING) AS order_id,
  CAST(`customer\_id` AS STRING) AS customer_id,
  CAST(`product\_id` AS STRING) AS product_id,
  CAST(REPLACE(TRIM(price), ',', '.') AS DOUBLE) AS price,
  CAST(TRIM(quantity) AS INT) AS quantity,
  CAST(`order\_time` AS STRING) AS order_time
FROM read_files(
  '/Volumes/workspace/lakehouse_demo/raw_data/orders_raw_v2.csv',
  format => 'csv',
  header => true,
  inferSchema => false
);

-- 4) Validate
SELECT COUNT(*) AS bronze_rows FROM bronze_orders;
SELECT * FROM bronze_orders;
