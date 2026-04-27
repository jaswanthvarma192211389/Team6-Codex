Create Database if not exists retail;

create schema if not exists raw;
create schema if not exists validated;
create schema if not exists curated;
create schema if not exists analytics;
create schema if not exists governance;

create role dev1;
create role dev2;
create role dev3;
create role dev4;


CREATE USER charan PASSWORD='dev1' DEFAULT_ROLE=role_dev1 MUST_CHANGE_PASSWORD=TRUE;
CREATE USER siva PASSWORD='dev2' DEFAULT_ROLE=role_dev2 MUST_CHANGE_PASSWORD=TRUE;
CREATE USER nithin PASSWORD='dev3' DEFAULT_ROLE=role_dev3 MUST_CHANGE_PASSWORD=TRUE;
CREATE USER sai PASSWORD='dev4' DEFAULT_ROLE=role_dev4 MUST_CHANGE_PASSWORD=TRUE;

grant all privileges on schema validated to role dev2;
grant all privileges on schema curated to role dev1;
grant all privileges on schema governance to role dev1;
grant all privileges on schema analytics to role dev3;

GRANT USAGE ON WAREHOUSE compute_wh TO ROLE dev1;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE dev2;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE dev3;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE dev4;



GRANT USAGE ON DATABASE retail TO ROLE dev1;
GRANT USAGE ON DATABASE retail TO ROLE dev2;
GRANT USAGE ON DATABASE retail TO ROLE dev3;
GRANT USAGE ON DATABASE retail TO ROLE dev4;

GRANT USAGE ON ALL SCHEMAS IN DATABASE retail TO ROLE dev1;
GRANT USAGE ON ALL SCHEMAS IN DATABASE retail TO ROLE dev2;
GRANT USAGE ON ALL SCHEMAS IN DATABASE retail TO ROLE dev3;
GRANT USAGE ON ALL SCHEMAS IN DATABASE retail TO ROLE dev4;


GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE retail TO ROLE dev1;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE retail TO ROLE dev2;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE retail TO ROLE dev3;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE retail TO ROLE dev4;

GRANT ALL PRIVILEGES ON all TABLES IN schema validated TO ROLE accountadmin;

GRANT ALL PRIVILEGES ON FUTURE PROCEDURES IN DATABASE retail TO ROLE dev1;
GRANT ALL PRIVILEGES ON FUTURE PROCEDURES IN DATABASE retail TO ROLE dev2;
GRANT ALL PRIVILEGES ON FUTURE PROCEDURES IN DATABASE retail TO ROLE dev3;
GRANT ALL PRIVILEGES ON FUTURE PROCEDURES IN DATABASE retail TO ROLE dev4;

GRANT ALL PRIVILEGES ON FUTURE TASKS IN DATABASE retail TO ROLE dev1;
GRANT ALL PRIVILEGES ON FUTURE TASKS IN DATABASE retail TO ROLE dev2;
GRANT ALL PRIVILEGES ON FUTURE TASKS IN DATABASE retail TO ROLE dev3;
GRANT ALL PRIVILEGES ON FUTURE TASKS IN DATABASE retail TO ROLE dev4;

GRANT INSERT, SELECT ON TABLE hospital.governance.dq_exception_log TO ROLE ACCOUNTADMIN;

grant role dev1 to user charan;
grant role dev3 to user nithin;
grant role dev1 to user sai;
grant role dev2 to user siva;

CREATE OR REPLACE FILE FORMAT raw.csv_format
  TYPE = 'CSV'
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

create or replace schema external_stages;

CREATE OR REPLACE STORAGE INTEGRATION S3_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('s3://somalaraju/files/')
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::749069982982:role/jaswanth';

desc storage integration s3_int;


CREATE OR REPLACE STAGE external_stages.aws_s3_csv1
URL = 's3://somalaraju/files/'
STORAGE_INTEGRATION = s3_int;

list @external_stages.aws_s3_csv1;


CREATE OR REPLACE TABLE raw.customers (
    customer_id STRING,
    name STRING,
    city STRING,
    signup_date DATE,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.products (
    product_id STRING,
    product_name STRING,
    category STRING,
    price NUMBER(10,2),
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE raw.orders (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    total_amount NUMBER(10,2),
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.order_items (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity NUMBER,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE raw.user_activity (
    activity_id STRING,
    customer_id STRING,
    activity_type STRING,
    activity_time TIMESTAMP_NTZ,
    _load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

create schema if not exists pipe;

CREATE OR REPLACE PIPE retail.PIPE.customer_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL.RAW.CUSTOMERS
FROM @retail.external_stages.aws_s3_csv1/customers/
FILE_FORMAT = retail.raw.csv_format
PATTERN ='.*customers.*\.csv'
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE retail.PIPE.product_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL.RAW.PRODUCTS
FROM @retail.external_stages.aws_s3_csv1/products/
FILE_FORMAT = retail.raw.csv_format
PATTERN = '.*products.*\.csv'
ON_ERROR = CONTINUE;

desc pipe retail.PIPE.customer_pipe;

CREATE OR REPLACE PIPE retail.PIPE.orders_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL.RAW.ORDERS
FROM @retail.external_stages.aws_s3_csv1/orders/
FILE_FORMAT = retail.raw.csv_format
PATTERN = '.*orders.*\.csv'
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE retail.PIPE.order_items_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL.RAW.ORDER_ITEMS
FROM @retail.external_stages.aws_s3_csv1/order_items/
FILE_FORMAT = retail.raw.csv_format
PATTERN = '.*order_items.*\.csv'
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE retail.PIPE.user_activity_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RETAIL.RAW.USER_ACTIVITY
FROM @retail.external_stages.aws_s3_csv1/user_activity/
FILE_FORMAT = retail.raw.csv_format
PATTERN = '.*user_activity.*\.csv'
ON_ERROR = CONTINUE;

CREATE OR REPLACE STREAM raw.customers_stream ON TABLE raw.customers;
CREATE OR REPLACE STREAM raw.products_stream ON TABLE raw.products;
CREATE OR REPLACE STREAM raw.orders_stream ON TABLE raw.orders;
CREATE OR REPLACE STREAM raw.order_items_stream ON TABLE raw.order_items;
CREATE OR REPLACE STREAM raw.user_activity_stream ON TABLE RETAIL.RAW.USER_ACTIVITY ;

select * from raw.products_stream;

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


CREATE OR REPLACE TASK validated.task_run_data_quality
    WAREHOUSE = compute_wh
    SCHEDULE = '5 MINUTE'
    WHEN 
        SYSTEM$STREAM_HAS_DATA('raw.customers_stream') OR
        SYSTEM$STREAM_HAS_DATA('raw.products_stream') OR
        SYSTEM$STREAM_HAS_DATA('raw.orders_stream') OR
        SYSTEM$STREAM_HAS_DATA('raw.order_items_stream') OR
        SYSTEM$STREAM_HAS_DATA('raw.user_activity_stream')
AS
    CALL validated.process_retail_data_quality();

    

CREATE OR REPLACE PROCEDURE governance.run_anomaly_engine()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- 1. SALES SPIKE (daily revenue anomaly)
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 
        'SALES_SPIKE',
        TO_VARCHAR(order_date),
        CONCAT('High daily sales: ', SUM(total_amount))
    FROM validated.valid_orders
    GROUP BY order_date
    HAVING SUM(total_amount) > 2 * (
        SELECT AVG(daily_total)
        FROM (
            SELECT SUM(total_amount) AS daily_total
            FROM validated.valid_orders
            GROUP BY order_date
        )
    );


    -- 2. HIGH SPENDER (customer anomaly)
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 
        'HIGH_SPENDER',
        customer_id,
        CONCAT('Customer spent unusually high: ', SUM(total_amount))
    FROM validated.valid_orders
    GROUP BY customer_id
    HAVING SUM(total_amount) > (
        SELECT AVG(customer_total) * 3
        FROM (
            SELECT customer_id, SUM(total_amount) AS customer_total
            FROM validated.valid_orders
            GROUP BY customer_id
        )
    );

    -- 3. INACTIVE CUSTOMER (no activity in 30 days)
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 
        'INACTIVE_CUSTOMER',
        customer_id,
        'No activity in last 30 days'
    FROM validated.valid_customers
    WHERE customer_id NOT IN (
        SELECT DISTINCT customer_id
        FROM validated.valid_orders
        WHERE order_date >= CURRENT_DATE - 30
    );


    -- 4. ORDER OUTLIER (very high order)
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 
        'ORDER_OUTLIER',
        order_id,
        CONCAT('High order value: ', total_amount)
    FROM validated.valid_orders
    WHERE total_amount > (
        SELECT AVG(total_amount) * 3 
        FROM validated.valid_orders
    );


    -- 5. PRODUCT NO SALES (dead products)
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 
        'PRODUCT_NO_SALES',
        product_id,
        'No sales in last 30 days'
    FROM validated.valid_products
    WHERE product_id NOT IN (
        SELECT DISTINCT product_id
        FROM validated.valid_order_items oi
        JOIN validated.valid_orders o 
            ON oi.order_id = o.order_id
        WHERE o.order_date >= CURRENT_DATE - 30
    );


    -- 6. ACTIVITY DROP (user behavior anomaly)
    INSERT INTO governance.anomaly_rule_hits (rule_name, business_key, anomaly_desc)
    SELECT 
        'LOW_ACTIVITY',
        customer_id,
        'Very low activity count'
    FROM validated.valid_user_activity
    GROUP BY customer_id
    HAVING COUNT(*) < 2;


    RETURN 'Anomaly Engine (validated layer) executed successfully';

END;
$$;




CREATE MASKING POLICY mask_name_policy
AS (val STRING) RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('ADMIN') THEN val
    ELSE CONCAT(SUBSTR(val,1,1),'***')
END;

ALTER TABLE curated.dim_customer
MODIFY COLUMN name 
SET MASKING POLICY mask_name_policy;


CREATE OR REPLACE TABLE curated.dim_customer (

    customer_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id STRING NOT NULL,

    name STRING,
    city STRING,
    signup_date DATE,

    effective_start_date DATE DEFAULT CURRENT_DATE(),
    effective_end_date DATE DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,

    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- DIM_PRODUCT (SCD TYPE 2)
CREATE OR REPLACE TABLE curated.dim_product (

    product_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    product_id STRING NOT NULL,

    product_name STRING,
    category STRING,
    price NUMBER(10,2),

    effective_start_date DATE DEFAULT CURRENT_DATE(),
    effective_end_date DATE DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,

    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- DIM_DATE
CREATE OR REPLACE TABLE curated.dim_date (

    date_id DATE PRIMARY KEY,
    day NUMBER,
    month NUMBER,
    year NUMBER,
    quarter NUMBER,
    week NUMBER,
    day_name STRING,
    month_name STRING,
    is_weekend BOOLEAN
);

-- FACT_SALES
CREATE OR REPLACE TABLE curated.fact_sales (

    order_item_id STRING,
    order_id STRING,

    customer_sk NUMBER,
    product_sk NUMBER,
    date_id DATE,
    order_date DATE,

    quantity NUMBER(10,0),
    total_amount NUMBER(12,2),

    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- FACT_CUSTOMER_ACTIVITY
CREATE OR REPLACE TABLE curated.fact_customer_activity (

    activity_id STRING,
    customer_sk NUMBER,

    activity_type STRING,
    activity_time TIMESTAMP,

    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- FACT_CUSTOMER_SUMMARY (KPI SNAPSHOT TABLE)
CREATE OR REPLACE TABLE curated.fact_customer_summary (

    customer_sk NUMBER,
    snapshot_date DATE,

    total_orders NUMBER,
    total_spent NUMBER(12,2),
    last_order_date DATE,
    avg_order_value NUMBER(12,2),
    churn_flag STRING,

    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (customer_sk, snapshot_date)
);

INSERT INTO curated.dim_date (date_id, day, month, year, quarter, week, day_name, month_name, is_weekend)
SELECT
    DATEADD(DAY, SEQ4(), '2020-01-01')::DATE AS date_id,
    EXTRACT(DAY FROM DATEADD(DAY, SEQ4(), '2020-01-01')),
    EXTRACT(MONTH FROM DATEADD(DAY, SEQ4(), '2020-01-01')),
    EXTRACT(YEAR FROM DATEADD(DAY, SEQ4(), '2020-01-01')),
    EXTRACT(QUARTER FROM DATEADD(DAY, SEQ4(), '2020-01-01')),
    EXTRACT(WEEK FROM DATEADD(DAY, SEQ4(), '2020-01-01')),
    DAYNAME(DATEADD(DAY, SEQ4(), '2020-01-01')),
    MONTHNAME(DATEADD(DAY, SEQ4(), '2020-01-01')),
    CASE WHEN DAYOFWEEK(DATEADD(DAY, SEQ4(), '2020-01-01')) IN (0, 6) THEN TRUE ELSE FALSE END
FROM TABLE(GENERATOR(ROWCOUNT => 3650));


CREATE OR REPLACE PROCEDURE curated.populate_retail_curated_layer()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    -- =========================================================
    -- 1. DIM_CUSTOMER (SCD Type 2)
    -- =========================================================
    
    -- Step 1A: Expire old active records if their details changed
    UPDATE curated.dim_customer tgt
    SET tgt.effective_end_date = CURRENT_DATE(), 
        tgt.is_current = FALSE
    FROM validated.valid_customers src
    WHERE tgt.customer_id = src.customer_id 
      AND tgt.is_current = TRUE 
      AND (tgt.name != src.name OR tgt.city != src.city);

    -- Step 1B: Insert brand new customers OR the new versions of updated customers
    MERGE INTO curated.dim_customer tgt
    USING validated.valid_customers src
    ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
    WHEN NOT MATCHED THEN INSERT 
        (customer_id, name, city, signup_date, effective_start_date, effective_end_date, is_current)
    VALUES 
        (src.customer_id, src.name, src.city, src.signup_date, CURRENT_DATE(), NULL, TRUE);

    -- =========================================================
    -- 2. DIM_PRODUCT (SCD Type 2)
    -- =========================================================
    
    UPDATE curated.dim_product tgt
    SET tgt.effective_end_date = CURRENT_DATE(), 
        tgt.is_current = FALSE
    FROM validated.valid_products src
    WHERE tgt.product_id = src.product_id 
      AND tgt.is_current = TRUE 
      AND (tgt.product_name != src.product_name OR tgt.category != src.category OR tgt.price != src.price);

    MERGE INTO curated.dim_product tgt
    USING validated.valid_products src
    ON tgt.product_id = src.product_id AND tgt.is_current = TRUE
    WHEN NOT MATCHED THEN INSERT 
        (product_id, product_name, category, price, effective_start_date, effective_end_date, is_current)
    VALUES 
        (src.product_id, src.product_name, src.category, src.price, CURRENT_DATE(), NULL, TRUE);

    -- =========================================================
    -- 3. FACT_SALES
    -- =========================================================
    -- This joins the validated orders to the Dimensions to retrieve the Surrogate Keys (_sk)
    MERGE INTO curated.fact_sales tgt
    USING (
        SELECT 
            oi.order_item_id,
            o.order_id,
            c.customer_sk,
            p.product_sk,
            o.order_date as date_id,
            o.order_date,
            oi.quantity,
            (oi.quantity * p.price) as total_amount
        FROM validated.valid_order_items oi
        JOIN validated.valid_orders o ON oi.order_id = o.order_id
        JOIN curated.dim_customer c ON o.customer_id = c.customer_id AND c.is_current = TRUE
        JOIN curated.dim_product p ON oi.product_id = p.product_id AND p.is_current = TRUE
    ) src
    ON tgt.order_item_id = src.order_item_id
    WHEN MATCHED THEN UPDATE SET 
        tgt.quantity = src.quantity, 
        tgt.total_amount = src.total_amount
    WHEN NOT MATCHED THEN INSERT 
        (order_item_id, order_id, customer_sk, product_sk, date_id, order_date, quantity, total_amount)
    VALUES 
        (src.order_item_id, src.order_id, src.customer_sk, src.product_sk, src.date_id, src.order_date, src.quantity, src.total_amount);

    -- =========================================================
    -- 4. FACT_CUSTOMER_ACTIVITY
    -- =========================================================
    MERGE INTO curated.fact_customer_activity tgt
    USING (
        SELECT 
            a.activity_id,
            c.customer_sk,
            a.activity_type,
            a.activity_time
        FROM validated.valid_user_activity a
        JOIN curated.dim_customer c ON a.customer_id = c.customer_id AND c.is_current = TRUE
    ) src
    ON tgt.activity_id = src.activity_id
    WHEN MATCHED THEN UPDATE SET tgt.activity_type = src.activity_type, tgt.activity_time = src.activity_time
    WHEN NOT MATCHED THEN INSERT (activity_id, customer_sk, activity_type, activity_time) 
    VALUES (src.activity_id, src.customer_sk, src.activity_type, src.activity_time);

    -- =========================================================
    -- 5. FACT_CUSTOMER_SUMMARY (Snapshot logic)
    -- =========================================================
    MERGE INTO curated.fact_customer_summary tgt
    USING (
        WITH sales AS (
            SELECT customer_sk, COUNT(DISTINCT order_id) as total_orders, SUM(total_amount) as total_spent, MAX(order_date) as last_order_date
            FROM curated.fact_sales GROUP BY customer_sk
        ),
        activity AS (
            SELECT customer_sk, MAX(activity_time) as last_activity
            FROM curated.fact_customer_activity GROUP BY customer_sk
        )
        SELECT 
            c.customer_sk,
            CURRENT_DATE() as snapshot_date,
            COALESCE(s.total_orders, 0) as total_orders,
            COALESCE(s.total_spent, 0) as total_spent,
            s.last_order_date,
            COALESCE(s.total_spent / NULLIF(s.total_orders, 0), 0) as avg_order_value,
            -- Define churn as inactive/no-purchases for more than 90 days
            CASE WHEN DATEDIFF(day, COALESCE(DATE(a.last_activity), s.last_order_date, c.signup_date), CURRENT_DATE()) > 90 THEN 'Yes' ELSE 'No' END as churn_flag
        FROM curated.dim_customer c
        LEFT JOIN sales s ON c.customer_sk = s.customer_sk
        LEFT JOIN activity a ON c.customer_sk = a.customer_sk
        WHERE c.is_current = TRUE
    ) src
    ON tgt.customer_sk = src.customer_sk AND tgt.snapshot_date = src.snapshot_date
    WHEN MATCHED THEN UPDATE SET 
        tgt.total_orders = src.total_orders, tgt.total_spent = src.total_spent, 
        tgt.last_order_date = src.last_order_date, tgt.avg_order_value = src.avg_order_value, tgt.churn_flag = src.churn_flag
    WHEN NOT MATCHED THEN INSERT 
        (customer_sk, snapshot_date, total_orders, total_spent, last_order_date, avg_order_value, churn_flag)
    VALUES 
        (src.customer_sk, src.snapshot_date, src.total_orders, src.total_spent, src.last_order_date, src.avg_order_value, src.churn_flag);

    RETURN 'Curated Storage Layer Population Completed Successfully';
    EXCEPTION
    WHEN OTHER THEN
        CALL SYSTEM$SEND_EMAIL(
            'retail_validation_alerts',       
            'jaswanthvarmajaswanthvarma@gmail.com,            
            '🚨 CRITICAL: curated Data Pipeline Failed'
        );
        RETURN 'PIPELINE FAILED: ' || :error_msg;
END;
$$;
ALTER TABLE curated.dim_customer
MODIFY COLUMN name 
SET MASKING POLICY mask_name_policy;

CREATE MASKING POLICY mask_name_policy
AS (val STRING) RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('ADMIN') THEN val
    ELSE CONCAT(SUBSTR(val,1,1),'***')
END;

CREATE OR REPLACE TASK validated.task_run_curated
WAREHOUSE = compute_wh
AFTER validated.task_run_data_quality
AS
CALL curated.populate_retail_curated_layer();



create or replace view RETAIL.ANALYTICS.kpi1
as
SELECT 
    d.year,
    d.month_name,
    SUM(fs.total_amount) AS monthly_revenue,
    LAG(SUM(fs.total_amount)) OVER (ORDER BY d.year, d.month) AS prev_month_revenue,
    ((SUM(fs.total_amount) - LAG(SUM(fs.total_amount)) OVER (ORDER BY d.year, d.month)) / 
      NULLIF(LAG(SUM(fs.total_amount)) OVER (ORDER BY d.year, d.month), 0)) * 100 AS mom_growth_percentage
FROM curated.fact_sales fs
JOIN curated.dim_date d ON fs.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year DESC, d.month DESC;

create or replace view RETAIL.ANALYTICS.kpi2
as
SELECT 
    COUNT(CASE WHEN churn_flag = 'Yes' THEN 1 END) AS churned_customers,
    COUNT(customer_sk) AS total_customers,
    ROUND((COUNT(CASE WHEN churn_flag = 'Yes' THEN 1 END) * 100.0 / NULLIF(COUNT(customer_sk), 0)), 2) AS churn_rate_pct
FROM curated.fact_customer_summary
WHERE snapshot_date = CURRENT_DATE();

create or replace view RETAIL.ANALYTICS.kpi3
as
SELECT 
    -- Compute the segments dynamically using the snapshot
    CASE 
        WHEN total_spent >= 1000 AND total_orders >= 5 THEN 'VIP'
        WHEN total_spent >= 500 THEN 'High Value'
        WHEN total_orders > 1 THEN 'Active Regular'
        WHEN total_orders = 1 THEN 'One-Time Buyer'
        ELSE 'Prospect' 
    END AS customer_segment,
    COUNT(customer_sk) AS total_customers_in_bucket,
    SUM(total_spent) AS total_segment_revenue,
    ROUND(AVG(total_spent), 2) AS avg_cltv_per_customer
FROM curated.fact_customer_summary
WHERE snapshot_date = CURRENT_DATE()
GROUP BY 1
ORDER BY total_segment_revenue DESC;

create or replace view RETAIL.ANALYTICS.kpi4
as
SELECT 
    p.category,
    SUM(fs.total_amount) AS total_revenue,
    COUNT(DISTINCT fs.order_id) AS total_unique_orders,
    ROUND(SUM(fs.total_amount) / NULLIF(COUNT(DISTINCT fs.order_id), 0), 2) AS category_aov
FROM curated.fact_sales fs
JOIN curated.dim_product p ON fs.product_sk = p.product_sk
GROUP BY p.category
ORDER BY category_aov DESC;


create or replace view RETAIL.ANALYTICS.kpi5
as
SELECT 
    COUNT(DISTINCT a.customer_sk) AS customers_with_activity_this_month,
    (SELECT COUNT(DISTINCT customer_sk) FROM curated.dim_customer WHERE is_current = TRUE) AS total_active_customer_base,
    
    ROUND((COUNT(DISTINCT a.customer_sk) * 100.0) / 
    NULLIF((SELECT COUNT(DISTINCT customer_sk) FROM curated.dim_customer WHERE is_current = TRUE), 0), 2) AS user_engagement_rate_pct
FROM curated.fact_customer_activity a
-- Fixed: Extract the date dynamically since date_id isn't in the activity table
WHERE DATE(a.activity_time) >= DATEADD(day, -30, CURRENT_DATE());







