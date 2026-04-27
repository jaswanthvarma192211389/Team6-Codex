use role dev1;
use database retail;
--============
--customers
--============

CREATE TABLE curated.dim_customer (
    customer_sk        NUMBER AUTOINCREMENT PRIMARY KEY,   -- Surrogate Key
    customer_id        STRING,                             -- Business Key
    name               STRING,
    city               STRING,
    signup_date        DATE,
    effective_start_date DATE,
    effective_end_date   DATE,
    is_current           BOOLEAN,
    load_timestamp     TIMESTAMP
);

--=========
--Products
--=========
CREATE TABLE curated.dim_product (
    product_sk         NUMBER AUTOINCREMENT PRIMARY KEY,   -- Surrogate Key
    product_id         STRING,                             -- Business Key
    product_name       STRING,
    category           STRING,
    price              NUMBER(10,2),
    effective_start_date DATE,
    effective_end_date   DATE,
    is_current           BOOLEAN,
    load_timestamp     TIMESTAMP
);
drop table dim_product
---==================
--Date
--==================
CREATE TABLE curated.dim_date (

    date_id   DATE PRIMARY KEY,
    day       NUMBER,
    month     NUMBER,
    year      NUMBER,
    quarter   NUMBER,
    week      NUMBER
);
-- =========================================
-- FACT TABLES
-- =========================================

-- FACT_SALES
CREATE TABLE curated.fact_sales (
    order_item_id     STRING PRIMARY KEY,
    order_id          STRING,
    customer_sk       NUMBER,
    product_sk        NUMBER,
    date_id           DATE,
    quantity          NUMBER,
    total_amount      NUMBER(10,2),
    load_timestamp    TIMESTAMP,

);
-- FACT_CUSTOMER_ACTIVITY
CREATE TABLE curated.fact_customer_activity (

    activity_id     STRING PRIMARY KEY,
    customer_sk     NUMBER,
    activity_type   STRING,
    activity_time   TIMESTAMP,
    load_timestamp  TIMESTAMP
);
-- FACT_CUSTOMER_SUMMARY (KPI TABLE)
CREATE TABLE curated.fact_customer_summary (
    customer_sk        NUMBER PRIMARY KEY,
    total_orders       NUMBER,
    total_spent        NUMBER(10,2),
    last_order_date    DATE,
    avg_order_value    NUMBER(10,2),
    churn_flag         STRING,
    load_timestamp     TIMESTAMP
);

CREATE OR REPLACE PROCEDURE CURATED.SP_LOAD_CURATED_RETAIL()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

--------------------------------------------------
-- 1. SCD2 - DIM_CUSTOMERS
--------------------------------------------------
MERGE INTO CURATED.DIM_CUSTOMERS tgt
USING VALIDATED.VALID_CUSTOMERS src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = TRUE

WHEN MATCHED AND (
    NVL(tgt.name,'') <> NVL(src.name,'') OR
    NVL(tgt.city,'') <> NVL(src.city,'')
)
THEN UPDATE SET
    tgt.is_current = FALSE,
    tgt.effective_end_date = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    customer_id,
    name,
    city,
    signup_date,
    effective_start_date,
    effective_end_date,
    is_current
)
VALUES (
    src.customer_id,
    src.name,
    src.city,
    src.signup_date,
    CURRENT_TIMESTAMP,
    NULL,
    TRUE
);

--------------------------------------------------
-- 2. DIM_PRODUCTS 
--------------------------------------------------
MERGE INTO CURATED.DIM_PRODUCTS tgt
USING VALIDATED.VALID_PRODUCTS src
ON tgt.product_id = src.product_id

WHEN MATCHED THEN UPDATE SET
    tgt.product_name = src.product_name,
    tgt.category = src.category,
    tgt.price = src.price,
    tgt.load_ts = src._load_timestamp

WHEN NOT MATCHED THEN INSERT (
    product_id,
    product_name,
    category,
    price,
    load_ts
)
VALUES (
    src.product_id,
    src.product_name,
    src.category,
    src.price,
    src._load_timestamp
);

--------------------------------------------------
-- 3. FACT_ORDERS
--------------------------------------------------
INSERT INTO CURATED.FACT_ORDERS
SELECT
    o.order_id,
    dc.customer_sk,
    o.order_date,
    o.total_amount,
    DATE(o.order_date) AS order_day
FROM VALIDATED.VALID_ORDERS o
JOIN CURATED.DIM_CUSTOMERS dc
ON o.customer_id = dc.customer_id
AND dc.is_current = TRUE
WHERE NOT EXISTS (
    SELECT 1 
    FROM CURATED.FACT_ORDERS f
    WHERE f.order_id = o.order_id
);

--------------------------------------------------
-- 4. FACT_ORDER_ITEMS
--------------------------------------------------
INSERT INTO CURATED.FACT_ORDER_ITEMS
SELECT
    oi.order_item_id,
    oi.order_id,
    dp.product_sk,
    oi.quantity,
    oi.quantity * dp.price AS item_total
FROM VALIDATED.VALID_ORDER_ITEMS oi
JOIN CURATED.DIM_PRODUCTS dp
ON oi.product_id = dp.product_id
WHERE NOT EXISTS (
    SELECT 1
    FROM CURATED.FACT_ORDER_ITEMS f
    WHERE f.order_item_id = oi.order_item_id
);

--------------------------------------------------
-- 5. FACT_USER_ACTIVITY
--------------------------------------------------
INSERT INTO CURATED.FACT_USER_ACTIVITY
SELECT
    ua.activity_id,
    dc.customer_sk,
    ua.activity_type,
    ua.activity_time,
    DATE(ua.activity_time) AS activity_date
FROM VALIDATED.VALID_USER_ACTIVITY ua
JOIN CURATED.DIM_CUSTOMERS dc
ON ua.customer_id = dc.customer_id
AND dc.is_current = TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM CURATED.FACT_USER_ACTIVITY f
    WHERE f.activity_id = ua.activity_id
);

--------------------------------------------------
-- 6. CUSTOMER 360 TABLE 
--------------------------------------------------
INSERT INTO CURATED.CUSTOMER_360
SELECT
    dc.customer_sk,
    dc.customer_id,
    dc.name,
    dc.city,

    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_spend,

    MAX(o.order_date) AS last_order_date,

    COUNT(DISTINCT ua.activity_id) AS total_activities,

    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE) AS days_since_last_order

FROM CURATED.DIM_CUSTOMERS dc

LEFT JOIN CURATED.FACT_ORDERS o
ON dc.customer_sk = o.customer_sk

LEFT JOIN CURATED.FACT_USER_ACTIVITY ua
ON dc.customer_sk = ua.customer_sk

WHERE dc.is_current = TRUE

GROUP BY
    dc.customer_sk,
    dc.customer_id,
    dc.name,
    dc.city;

--------------------------------------------------

RETURN 'CURATED RETAIL LAYER LOADED SUCCESSFULLY';

END;
$$;

CALL CURATED.SP_LOAD_CURATED_RETAIL();



