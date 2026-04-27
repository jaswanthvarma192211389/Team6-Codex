# Customer Intelligence & Sales Insights Platform (Snowflake)

##  Overview

This project builds a scalable **data pipeline and analytics platform** using Snowflake to help retail businesses:

* Understand customer behavior
* Improve sales performance
* Make data-driven decisions

The system processes data from multiple sources and generates actionable insights such as **customer segmentation, churn detection, and sales trends**.

---

##  Architecture

```
S3 → Snowpipe → RAW → Streams → VALIDATED → CURATED → DATA MART → KPIs → Alerts
```

### Key Components:

* **Snowpipe** → Automated data ingestion
* **Streams** → Change Data Capture (CDC)
* **Tasks** → Workflow automation
* **SCD Type 2** → Historical data tracking
* **Data Mart** → Optimized analytics layer

---

## 🗂️ Datasets Used

* **Customers** → customer_id, name, city, signup_date
* **Products** → product_id, product_name, category, price
* **Orders** → order_id, customer_id, order_date, total_amount
* **Order Items** → order_item_id, order_id, product_id, quantity
* **User Activity** → activity_id, customer_id, activity_type, activity_time

---

##  Data Model (Curated Layer)

###  Dimension Tables

* `dim_customer` → SCD Type 2 (tracks history)
* `dim_product` → SCD Type 2
* `dim_date` → Time-based analysis

###  Fact Tables

* `fact_sales` → Transaction data
* `fact_customer_activity` → User behavior
* `fact_customer_summary` → KPI metrics

---

## Pipeline Flow

1. Data ingestion from S3 using Snowpipe
2. Change tracking using Streams
3. Data validation and cleansing
4. Transformation into curated schema (SCD2, facts, dimensions)
5. Anomaly detection
6. Data mart creation
7. KPI generation and reporting
8. Alert system for failures

---

##  Key Features

* Customer 360 view
* Customer segmentation (High / Medium / Low value)
* Churn detection
* Sales trend analysis
* Automated pipeline using Tasks
* Real-time change tracking with Streams

---

##  Sample Insights

* Top customers by revenue
* Monthly sales trends
* Most popular product categories
* Inactive customers (churn risk)

---

##  Technologies Used

* Snowflake (Data Warehouse)
* SQL
* AWS S3 (Data Source)
* Snowpipe (Ingestion)
* Streams & Tasks (Automation)

---




## Conclusion

This project demonstrates how Snowflake can be used to build a **modern, scalable, and automated data pipeline** for real-time analytics and business intelligence.
