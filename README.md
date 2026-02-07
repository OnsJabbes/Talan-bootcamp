# 📊 Amazon Sales Lakehouse — Real-Time Data Pipeline (Kafka → Spark → HDFS → Postgres → Streamlit/Power BI)

## 1) Project Overview

This project implements a **real-time analytics platform** for Amazon sales data using a modern **Lakehouse architecture**:

- **Kafka** ingests sales events (JSON).
- **Spark Structured Streaming** processes events continuously.
- Data is stored in **HDFS** in multiple layers:
  - **Bronze**: raw events (JSON)
  - **Silver**: cleaned/enriched events (Parquet)
  - **Gold**: aggregated KPIs (Parquet)
- Spark also writes to **PostgreSQL** for operational and analytical use:
  - **orders_live** (OLTP-style table for live orders)
  - **kpi_1min** (OLAP-style KPI table in 1-minute windows)
- **Streamlit dashboard** reads from Postgres and visualizes near real-time analytics.
- **Power BI** can connect to Postgres for BI reporting.

✅ Goal: Provide a **reliable, explainable, and deployable** streaming lakehouse pipeline suitable for a jury demo.

---

## 2) Architecture (End-to-End)

### 2.1 Data Flow

`amazon.csv (or synthetic generator)` → **Kafka topic** → **Spark Streaming job** → **HDFS Lakehouse (Bronze/Silver/Gold)** + **Postgres marts** → **Streamlit/Power BI**

### 2.2 Layers Definition

#### 🥉 Bronze Layer (Raw)
- Stores exactly what comes from Kafka (JSON)
- Used for traceability and replay
- Path:  
`/datalake/bronze/amazon_sales`

#### 🥈 Silver Layer (Cleaned & Enriched)
- Converts types (OrderDate → date)
- Adds partitions `year` and `month`
- Adds derived metrics:
  - `Revenue = Quantity * UnitPrice`
  - `DiscountAmount = Revenue * Discount`
  - `NetRevenue = Revenue - DiscountAmount`
- Path:  
`/datalake/silver/amazon_sales` (partitioned by year/month)

#### 🥇 Gold Layer (Aggregated KPIs)
- Computes KPI windows (1 minute)
- Dimensions:
  - Category
  - Country
  - PaymentMethod
- Metrics:
  - TotalOrders
  - TotalQuantity
  - TotalRevenue
  - TotalNetRevenue
  - AvgOrderValue
  - UniqueCustomers
- Path:  
`/datalake/gold/amazon_sales_aggregated`

---

## 3) Technologies Used

| Component | Technology |
|----------|------------|
| Data Ingestion | Apache Kafka |
| Stream Processing | Spark Structured Streaming |
| Data Lake Storage | HDFS |
| Lakehouse Layers | Bronze (JSON), Silver (Parquet), Gold (Parquet) |
| OLTP / Mart DB | PostgreSQL |
| Visualization | Streamlit (Plotly) |
| BI Tool (optional) | Power BI |
| Deployment | Docker Compose |

---

## 4) Containers / Services

Depending on your compose file, the stack usually includes:

- `zookeeper`
- `kafka`
- `producer-amazon` (sends messages into Kafka)
- `namenode` (+ optional datanode)
- `spark-master` + `spark-worker`
- `spark-streaming` (your streaming job container)
- `postgres-mart`
- `streamlit`

---

## 5) Deployment From Zero (Docker)

### 5.1 Start fresh (clean all volumes)

⚠️ This removes all persisted data (HDFS + Postgres + Kafka offsets + checkpoints):

```bash
docker compose down -v
docker compose up -d --build
```

### 5.2 Check containers are running

```bash
docker ps
docker compose logs -f --tail 50
```

---

## 6) Runtime / Launch Order (Important)

To ensure data flows correctly:

### Step 1 — Verify Kafka topic and producer

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic amazon-sales \
  --from-beginning \
  --max-messages 3
```

You should see JSON messages.

---

### Step 2 — Verify Spark Streaming job is running

```bash
docker logs -f spark-streaming --tail 200
```

✅ Expected: no crash, and micro-batches running.

---

### Step 3 — Check HDFS layers

```bash
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/bronze/amazon_sales | tail"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/silver/amazon_sales | tail"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/gold/amazon_sales_aggregated | tail"
```

✅ Expected:
- Bronze: `part-*.json`
- Silver: parquet + partitions (`year=.../month=...`)
- Gold: parquet aggregations

---

### Step 4 — Check Postgres tables are filling

```bash
docker exec -it postgres-mart psql -U mart -d martdb -c "select count(*) from orders_live;"
docker exec -it postgres-mart psql -U mart -d martdb -c "select count(*) from kpi_1min;"
```

✅ Expected: counts > 0 after a few micro-batches.

---

### Step 5 — Launch Streamlit

Open the Streamlit URL (from docker compose port mapping)  
Example: `http://localhost:8501`

---

## 7) Database Tables (Mart Schema)

### 7.1 `orders_live` (Operational / Live Orders)

Contains cleaned/enriched events at record level. Used by Streamlit dashboard for “live order feed”.

Example columns:
- processing_timestamp
- OrderID
- OrderDate
- CustomerID
- ProductID
- Category
- Quantity
- UnitPrice
- TotalAmount
- Revenue
- NetRevenue
- Country
- PaymentMethod
- OrderStatus
...

### 7.2 `kpi_1min` (Analytics / OLAP)

Contains aggregated metrics by 1-minute windows:

- window_start
- window_end
- category
- country
- paymentmethod
- totalorders
- totalquantity
- totalrevenue
- totalnetrevenue
- avgordervalue
- uniquecustomers

---

## 8) Streaming Logic (Spark Structured Streaming)

### 8.1 Input
- Kafka topic: `amazon-sales`
- Message value is JSON, parsed using a Spark schema.

### 8.2 Transformations
**Bronze**
- Adds metadata: topic, partition, offset, kafka timestamp.
- Adds `ingestion_timestamp`

**Silver**
- Cast OrderDate into Spark Date
- Derives:
  - Revenue
  - DiscountAmount
  - NetRevenue
- Filters invalid rows:
  - Quantity > 0
  - TotalAmount > 0

**Gold KPI**
- Window of 1 minute (based on processing timestamp)
- Aggregations per category/country/payment

### 8.3 Output
- HDFS: Bronze JSON / Silver Parquet / Gold Parquet
- Postgres: orders_live / kpi_1min

✅ Important design choice: **Gold Parquet must be written using `append` or `foreachBatch`**, not `complete` mode, because Parquet sink does not support complete output mode.

---

## 9) Streamlit Dashboard Features

### ✅ KPIs
- Total Orders
- Total Revenue
- Avg Order Value
- Unique Customers
- Top Category (by revenue)
- Top Country (by revenue)

### ✅ Charts
- Revenue over time (minute buckets)
- Orders over time
- Top categories by revenue
- Payment methods share
- Order status distribution
- Revenue vs quantity scatter plot
- Histogram of order amount distribution

### ✅ Filters
- Date range
- Category
- Country
- Payment method
- Order status

---

## 10) Power BI Integration (Optional)

Power BI can connect directly to Postgres.

### Connection (example)
- Host: `localhost`
- Port: `5433` (depending on docker mapping)
- DB: `martdb`
- User: `mart`
- Password: `mart`

Use tables:
- `kpi_1min`
- `kpi_daily` (if implemented)

---

## 11) Troubleshooting (Most Common Issues)

### Issue A — “Gold layer missing”
✅ Cause: Spark job crashed before creating Gold output or no writes happened.

Check logs:
```bash
docker logs -f spark-streaming --tail 200
```

---

### Issue B — `AnalysisException: parquet does not support Complete output mode`
✅ Cause: writing streaming aggregations to parquet using `complete`.

✅ Fix: Use `append` or `foreachBatch` for parquet output.

---

### Issue C — Postgres counts are 0
Likely causes:
- Spark job crashed
- Kafka topic empty
- JDBC connectivity issue
- Wrong table name
- Checkpoint/offset mismatch

Verify:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic amazon-sales --from-beginning --max-messages 3
docker logs -f spark-streaming --tail 200
```

---

### Issue D — Only `_spark_metadata` appears in HDFS
Spark started the sink but wrote **no data** (job stopped or no messages processed).  
Re-check producer, Kafka consumption, and Spark logs.

---

## 12) Jury Demo Script (Suggested)

1) Show architecture diagram (Kafka → Spark → HDFS Layers → Postgres → Streamlit)
2) Run consumer to show Kafka messages
3) Show Spark UI (port 4040) micro-batch activity
4) Show HDFS directories for Bronze/Silver/Gold
5) Show Postgres row counts
6) Show Streamlit live dashboard + filters + charts
7) Explain lakehouse benefits:
   - traceability (Bronze)
   - cleaned analytical layer (Silver)
   - KPIs for BI (Gold)
8) Explain real-time value:
   - near real-time KPIs for decision making

---

## 13) Useful Commands Summary

### Kafka
```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic amazon-sales --from-beginning --max-messages 3
```

### Spark logs
```bash
docker logs -f spark-streaming --tail 200
```

### HDFS check
```bash
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/bronze/amazon_sales | tail"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/silver/amazon_sales | tail"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/gold/amazon_sales_aggregated | tail"
```

### Postgres check
```bash
docker exec -it postgres-mart psql -U mart -d martdb -c "select count(*) from orders_live;"
docker exec -it postgres-mart psql -U mart -d martdb -c "select count(*) from kpi_1min;"
```
