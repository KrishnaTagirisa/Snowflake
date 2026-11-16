-- =============================================================================
-- 06_validation_queries.sql
-- Run after loading sample_data for 2026-11-10 then 2026-11-11.
-- Each query states the expected result so you can prove the edge cases work.
-- =============================================================================
USE ROLE RETAIL_TRANSFORMER;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE RETAIL_PROD;

-- 1. Load audit: 6 files, all LOADED, no errors
SELECT load_date, entity, status, rows_loaded FROM AUDIT.LOAD_LOG ORDER BY load_date, entity;

-- 2. Dedup: C002 appears twice in day-1 file but once in silver -> 4 customers total
SELECT COUNT(*) AS customers FROM SILVER.STG_CUSTOMERS;                     -- expect 4

-- 3. SCD2: C001 has 2 versions; Rugby/CONSUMER closed, Coventry/SMALL_BUSINESS current
SELECT customer_id, city, segment, valid_from, valid_to, is_current
FROM GOLD.DIM_CUSTOMER WHERE customer_id = 'C001' ORDER BY valid_from;     -- expect 2 rows

-- 4. Point-in-time join: O9001 (placed 10 Nov) -> Rugby version; O9003 (11 Nov) -> Coventry version
SELECT f.order_id, c.city, c.segment
FROM GOLD.FCT_ORDER_LINES f
JOIN GOLD.DIM_CUSTOMER c ON c.customer_sk = f.customer_sk
WHERE f.order_id IN ('O9001', 'O9003') AND f.line_number = 1;

-- 5. Latest status wins: O9001 is SHIPPED, O9002 is PAID
SELECT order_id, order_status FROM SILVER.STG_ORDERS ORDER BY order_id;

-- 6. Amended order: O9002 had 2 lines on day 1, only 1 after amendment
SELECT order_id, COUNT(*) AS lines FROM GOLD.FCT_ORDER_LINES GROUP BY order_id ORDER BY 1;
-- expect O9001=2, O9002=1, O9003=1

-- 7. Messy data handled: '1,249.00' parsed to 1249.00, blank currency defaulted to GBP
SELECT product_id, unit_price, currency, is_active FROM SILVER.STG_PRODUCTS ORDER BY 1;

-- 8. Invalid email flagged (not dropped)
SELECT customer_id, email, is_email_valid FROM SILVER.STG_CUSTOMERS WHERE NOT is_email_valid;  -- C003

-- 9. No unknown-member fallbacks expected for this data
SELECT COUNT_IF(customer_sk = '-1') AS unknown_customers, COUNT_IF(product_sk = '-1') AS unknown_products
FROM GOLD.FCT_ORDER_LINES;                                                  -- expect 0, 0

-- 10. Idempotency: rerun the DAG for 2026-11-11 -> LOAD_LOG gets no new LOADED rows,
--     bronze row counts unchanged, gold counts unchanged.
SELECT COUNT(*) FROM BRONZE.ORDERS_RAW;                                     -- expect 5
