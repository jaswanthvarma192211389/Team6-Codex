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