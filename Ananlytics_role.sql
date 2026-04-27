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
