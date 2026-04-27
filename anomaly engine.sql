CREATE MASKING POLICY mask_name_policy
AS (val STRING) RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('ADMIN') THEN val
    ELSE CONCAT(SUBSTR(val,1,1),'***')
END;

ALTER TABLE curated.dim_customer
MODIFY COLUMN name 
SET MASKING POLICY mask_name_policy;

select * from curated.dim_customer;



------ANOMALY----

CREATE TABLE governance.anomaly_rule_hits (
    rule_name STRING,
    business_key STRING,
    anomaly_desc STRING,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


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



-----EMAIL NOTIFICATION----
CREATE OR REPLACE NOTIFICATION INTEGRATION retail_validation_alerts
    TYPE = EMAIL
    ENABLED = TRUE;