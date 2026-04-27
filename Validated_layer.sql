CREATE OR REPLACE TABLE validated.valid_customers (
    customer_id STRING PRIMARY KEY,
    name STRING,
    city STRING,
    signup_date DATE,
    _load_timestamp TIMESTAMP
);

CREATE OR REPLACE TABLE validated.valid_products (
    product_id STRING PRIMARY KEY,
    product_name STRING,
    category STRING,
    price FLOAT,
    _load_timestamp TIMESTAMP
);

CREATE OR REPLACE TABLE validated.valid_orders (
    order_id STRING PRIMARY KEY,
    customer_id STRING,
    order_date DATE,
    total_amount FLOAT,
    _load_timestamp TIMESTAMP
);

CREATE OR REPLACE TABLE validated.valid_order_items (
    order_item_id STRING PRIMARY KEY,
    order_id STRING,
    product_id STRING,
    quantity NUMBER,
    _load_timestamp TIMESTAMP
);

CREATE OR REPLACE TABLE validated.valid_user_activity (
    activity_id STRING PRIMARY KEY,
    customer_id STRING,
    activity_type STRING,
    activity_time TIMESTAMP_NTZ,
    _load_timestamp TIMESTAMP_NTZ
);
CREATE OR REPLACE TABLE validated.dq_exception_log (
    error_id STRING DEFAULT UUID_STRING(),
    source_table STRING,
    business_key STRING,
    error_type STRING,
    error_message STRING,
    rejected_record VARIANT,  
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
CREATE OR REPLACE TABLE governance.dq_exception_log (
    error_id STRING DEFAULT UUID_STRING(),
    source_table STRING,
    business_key STRING,
    error_type STRING,
    error_message STRING,
    rejected_record VARIANT,  
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);



CREATE OR REPLACE NOTIFICATION INTEGRATION retail_validation_alerts
    TYPE = EMAIL
    ENABLED = TRUE;

CREATE OR REPLACE PROCEDURE validated.process_retail_data_quality()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- =====================================================
-- 1. CUSTOMERS
-- =====================================================
CREATE OR REPLACE TEMP TABLE cur_customers AS SELECT * FROM raw.customers_stream;

INSERT INTO governance.dq_exception_log 
    (source_table, business_key, error_type, error_message, rejected_record)
SELECT 
    'customers', customer_id, 'NULL/VALUE',
    CASE 
        WHEN customer_id IS NULL THEN 'Missing customer_id'
        WHEN name IS NULL OR TRIM(name) = '' THEN 'Missing or empty full_name'
        WHEN city IS NULL OR TRIM(city) = '' THEN 'Missing or empty city'
        WHEN signup_date IS NULL THEN 'Missing signup_date'
        ELSE 'Unknown Data Quality Issue'
    END,
    OBJECT_CONSTRUCT(*)
FROM cur_customers
WHERE customer_id IS NULL 
   OR name IS NULL OR TRIM(name) = '' 
   OR city IS NULL OR TRIM(city) = '' 
   OR signup_date IS NULL;

MERGE INTO validated.valid_customers tgt
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY _load_timestamp DESC) rn
        FROM cur_customers
        WHERE customer_id IS NOT NULL 
          AND name IS NOT NULL AND TRIM(name) != '' 
          AND city IS NOT NULL AND TRIM(city) != '' 
          AND signup_date IS NOT NULL
    ) WHERE rn = 1
) src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN UPDATE SET tgt.name = src.name, tgt.city = UPPER(src.city), tgt.signup_date = src.signup_date, tgt._load_timestamp = src._load_timestamp
WHEN NOT MATCHED THEN INSERT VALUES (src.customer_id, src.name, UPPER(src.city), src.signup_date, src._load_timestamp);


-- =====================================================
-- 2. PRODUCTS
-- =====================================================
CREATE OR REPLACE TEMP TABLE cur_products AS SELECT * FROM raw.products_stream;

INSERT INTO governance.dq_exception_log 
    (source_table, business_key, error_type, error_message, rejected_record)
SELECT 
    'products', product_id, 'NULL/VALUE',
    CASE 
        WHEN product_id IS NULL THEN 'Missing product_id'
        WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 'Missing product_name'
        WHEN category IS NULL OR TRIM(category) = '' THEN 'Missing category'
        WHEN TRY_CAST(price AS NUMBER(10,2)) IS NULL OR TRY_CAST(price AS NUMBER(10,2)) <= 0 THEN 'Price is NULL or <= 0'
        ELSE 'Unknown Data Quality Issue'
    END,
    OBJECT_CONSTRUCT(*)
FROM cur_products
WHERE product_id IS NULL 
   OR product_name IS NULL OR TRIM(product_name) = '' 
   OR category IS NULL OR TRIM(category) = '' 
   OR TRY_CAST(price AS NUMBER(10,2)) IS NULL OR TRY_CAST(price AS NUMBER(10,2)) <= 0;

MERGE INTO validated.valid_products tgt
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY _load_timestamp DESC) rn
        FROM cur_products
        WHERE product_id IS NOT NULL 
          AND product_name IS NOT NULL AND TRIM(product_name) != '' 
          AND category IS NOT NULL AND TRIM(category) != '' 
          AND TRY_CAST(price AS NUMBER(10,2)) IS NOT NULL AND TRY_CAST(price AS NUMBER(10,2)) > 0
    ) WHERE rn = 1
) src
ON tgt.product_id = src.product_id
WHEN MATCHED THEN UPDATE SET tgt.price = src.price, tgt.category = UPPER(src.category)
WHEN NOT MATCHED THEN INSERT VALUES (src.product_id, src.product_name, UPPER(src.category), src.price, src._load_timestamp);


-- =====================================================
-- 3. ORDERS
-- =====================================================
CREATE OR REPLACE TEMP TABLE cur_orders AS SELECT * FROM raw.orders_stream;

INSERT INTO governance.dq_exception_log 
    (source_table, business_key, error_type, error_message, rejected_record)
SELECT 
    'orders', order_id, 'NULL/REFERENCE/VALUE',
    CASE 
        WHEN order_id IS NULL THEN 'Missing order_id'
        WHEN order_date IS NULL THEN 'Missing order_date'
        WHEN TRY_CAST(total_amount AS NUMBER(10,2)) IS NULL OR TRY_CAST(total_amount AS NUMBER(10,2)) <= 0 THEN 'total_amount is NULL or <= 0'
        WHEN customer_id IS NULL OR customer_id NOT IN (SELECT customer_id FROM validated.valid_customers) THEN 'customer_id is NULL or Invalid FK'
        ELSE 'Unknown Data Quality Issue'
    END,
    OBJECT_CONSTRUCT(*)
FROM cur_orders
WHERE order_id IS NULL 
   OR order_date IS NULL 
   OR TRY_CAST(total_amount AS NUMBER(10,2)) IS NULL OR TRY_CAST(total_amount AS NUMBER(10,2)) <= 0 
   OR customer_id IS NULL OR customer_id NOT IN (SELECT customer_id FROM validated.valid_customers);

MERGE INTO validated.valid_orders tgt
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY _load_timestamp DESC) rn
        FROM cur_orders
        WHERE order_id IS NOT NULL 
          AND order_date IS NOT NULL 
          AND TRY_CAST(total_amount AS NUMBER(10,2)) IS NOT NULL AND TRY_CAST(total_amount AS NUMBER(10,2)) > 0 
          AND customer_id IS NOT NULL AND customer_id IN (SELECT customer_id FROM validated.valid_customers)
    ) WHERE rn = 1
) src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET tgt.total_amount = src.total_amount
WHEN NOT MATCHED THEN INSERT VALUES (src.order_id, src.customer_id, src.order_date, src.total_amount, src._load_timestamp);


-- =====================================================
-- 4. ORDER ITEMS
-- =====================================================
CREATE OR REPLACE TEMP TABLE cur_order_items AS SELECT * FROM raw.order_items_stream;

INSERT INTO governance.dq_exception_log 
    (source_table, business_key, error_type, error_message, rejected_record)
SELECT 
    'order_items', order_item_id, 'NULL/REFERENCE/VALUE',
    CASE 
        WHEN order_item_id IS NULL THEN 'Missing order_item_id'
        WHEN TRY_CAST(quantity AS NUMBER) IS NULL OR TRY_CAST(quantity AS NUMBER) <= 0 THEN 'quantity is NULL or <= 0'
        WHEN order_id IS NULL OR order_id NOT IN (SELECT order_id FROM validated.valid_orders) THEN 'order_id is NULL or Invalid FK'
        WHEN product_id IS NULL OR product_id NOT IN (SELECT product_id FROM validated.valid_products) THEN 'product_id is NULL or Invalid FK'
        ELSE 'Unknown Data Quality Issue'
    END,
    OBJECT_CONSTRUCT(*)
FROM cur_order_items
WHERE order_item_id IS NULL 
   OR TRY_CAST(quantity AS NUMBER) IS NULL OR TRY_CAST(quantity AS NUMBER) <= 0 
   OR order_id IS NULL OR order_id NOT IN (SELECT order_id FROM validated.valid_orders) 
   OR product_id IS NULL OR product_id NOT IN (SELECT product_id FROM validated.valid_products);

MERGE INTO validated.valid_order_items tgt
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY order_item_id ORDER BY _load_timestamp DESC) rn
        FROM cur_order_items
        WHERE order_item_id IS NOT NULL 
          AND TRY_CAST(quantity AS NUMBER) IS NOT NULL AND TRY_CAST(quantity AS NUMBER) > 0 
          AND order_id IS NOT NULL AND order_id IN (SELECT order_id FROM validated.valid_orders) 
          AND product_id IS NOT NULL AND product_id IN (SELECT product_id FROM validated.valid_products)
    ) WHERE rn = 1
) src
ON tgt.order_item_id = src.order_item_id
WHEN MATCHED THEN UPDATE SET tgt.quantity = src.quantity
WHEN NOT MATCHED THEN INSERT VALUES (src.order_item_id, src.order_id, src.product_id, src.quantity, src._load_timestamp);


-- =====================================================
-- 5. USER ACTIVITY
-- =====================================================
CREATE OR REPLACE TEMP TABLE cur_activity AS SELECT * FROM raw.user_activity_stream;

select * from cur_activity;

INSERT INTO governance.dq_exception_log 
    (source_table, business_key, error_type, error_message, rejected_record)
SELECT 
    'user_activity', activity_id, 'NULL/REFERENCE/VALUE',
    CASE 
        WHEN activity_id IS NULL THEN 'Missing activity_id'
        WHEN activity_type IS NULL OR TRIM(activity_type) = '' THEN 'Missing activity_type'
        WHEN activity_time IS NULL THEN 'Missing activity_time'
        WHEN customer_id IS NULL OR customer_id NOT IN (SELECT customer_id FROM validated.valid_customers) THEN 'customer_id is NULL or Invalid FK'
        ELSE 'Unknown Data Quality Issue'
    END,
    OBJECT_CONSTRUCT(*)
FROM cur_activity
WHERE activity_id IS NULL 
   OR activity_type IS NULL OR TRIM(activity_type) = '' 
   OR activity_time IS NULL 
   OR customer_id IS NULL OR customer_id NOT IN (SELECT customer_id FROM validated.valid_customers);

MERGE INTO validated.valid_user_activity tgt
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY activity_id ORDER BY _load_timestamp DESC) rn
        FROM cur_activity
        WHERE activity_id IS NOT NULL 
          AND activity_type IS NOT NULL AND TRIM(activity_type) != '' 
          AND activity_time IS NOT NULL 
          AND customer_id IS NOT NULL AND customer_id IN (SELECT customer_id FROM validated.valid_customers)
    ) WHERE rn = 1
) src
ON tgt.activity_id = src.activity_id
WHEN MATCHED THEN UPDATE SET tgt.activity_type = src.activity_type
WHEN NOT MATCHED THEN INSERT VALUES (src.activity_id, src.customer_id, src.activity_type, src.activity_time, src._load_timestamp);


RETURN 'Retail Data Quality Processing Completed Successfully';
EXCEPTION
    WHEN OTHER THEN
        CALL SYSTEM$SEND_EMAIL(
            'retail_validation_alerts',       
            'jaswanthvarmajaswanthvarma@gmail.com,            
            '🚨 CRITICAL: Retail Data Pipeline Failed'
        );
        RETURN 'PIPELINE FAILED: ' || :error_msg;
END;
$$;