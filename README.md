# Amazon Sales Lakehouse - Airflow Orchestrated OLTP + OLAP

## 1) Project Overview

This project implements two coordinated data pipelines (OLTP + OLAP) orchestrated by Apache Airflow, processing 100,000 Amazon sales records through a complete lakehouse architecture.

### OLTP Pipeline (Streaming / Aggregated)
```
Amazon.csv -> Kafka -> Spark Structured Streaming (GroupBy aggregation) -> Postgres -> Streamlit
```
Spark Streaming consumes Kafka messages and produces **aggregated daily KPIs** (grouped by day, category, country, payment method) with metrics: total orders, total quantity, total revenue, total net revenue, average order value, and unique customers.

### OLAP Pipeline (Batch / Medallion Architecture)
```
Amazon.csv -> HDFS Bronze (CSV) -> Spark Batch -> HDFS Silver (Parquet) -> Spark Batch -> HDFS Gold (Parquet) -> Hive -> Trino
```
Three-layer medallion architecture with Bronze (raw CSV), Silver (cleaned/enriched Parquet partitioned by year/month), and Gold (aggregated Parquet KPIs). Trino provides SQL analytics over all layers via Hive metastore.

---

## 2) Architecture

### 2.1 OLTP Flow
```
Amazon.csv ──> Kafka ──> Spark Streaming ──> Postgres (aggregated) ──> Streamlit Dashboard
                         (GroupBy: day,       (amazon_sales_aggregated)
                          category, country,
                          paymentmethod)
```

### 2.2 OLAP Flow
```
Amazon.csv ──> HDFS Bronze ──> Spark Batch ──> HDFS Silver ──> Spark Batch ──> HDFS Gold ──> Hive/Trino
               (CSV)           (clean/enrich)  (Parquet)       (aggregate)     (Parquet)
```

### 2.3 Airflow DAG
```
start
├── produce_csv_to_kafka ──> spark_oltp_streaming ──> restart_streamlit ──┐
└── csv_to_bronze_hdfs ──> bronze_to_silver ──> silver_to_gold ────────────┘──> end
```

---

## 3) Technologies Used

| Layer | Technology | Version |
|-------|-----------|---------|
| Orchestration | Apache Airflow | 2.8.3 |
| Messaging | Apache Kafka | 7.5.0 (Confluent) |
| Processing | Apache Spark | 3.0.0 |
| Storage | HDFS (Hadoop) | 3.1.3 |
| Metastore | Apache Hive | 2.3.2 |
| Analytics | Trino | 447 |
| OLTP Database | PostgreSQL | 15 |
| Visualization | Streamlit | latest |
| Deployment | Docker Compose | - |

---

## 4) Services

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| Zookeeper | zookeeper | 2181 | Kafka coordination |
| Kafka | kafka | 9092, 29092 | Message broker |
| HDFS Namenode | namenode | 9870, 8020 | HDFS metadata |
| HDFS Datanode | datanode | 9864 | HDFS storage |
| HDFS Init | hdfs-init | - | Creates datalake directories |
| Hive Metastore DB | hive-metastore-postgresql | 5432 | Metastore backend (PostgreSQL 15) |
| Hive Metastore Init | hive-metastore-init | - | Initializes Hive schema (schematool) |
| Hive Metastore | hive-metastore | 9083 | Thrift metastore service |
| Hive Server | hive-server | 10000 | HiveServer2 + table creation |
| Spark Master | spark-master | 8080, 7077 | Spark cluster coordinator |
| Spark Worker | spark-worker-1 | 8081 | Spark executor |
| Postgres Mart | postgres-mart | 5433 | OLTP aggregated data |
| Trino | trino | 8090 | SQL analytics engine |
| Streamlit | streamlit-oltp | 8502 | Dashboard |
| Airflow DB | airflow-db | - | Airflow metadata |
| Airflow Webserver | airflow-webserver | 8088 | Airflow UI |
| Airflow Scheduler | airflow-scheduler | - | DAG scheduling |

Build-only images (used as Docker containers by Airflow):
- `amazon-spark-jobs` — All Spark jobs (OLAP batch + OLTP streaming)
- `amazon-producer` — Kafka producer (CSV to Kafka)

---

## 5) Quick Start

### 5.1 Clean start
```bash
docker compose down -v --remove-orphans
```

### 5.2 Build images
```bash
docker compose --profile build build
```

### 5.3 Start the stack
```bash
docker compose up -d
```

### 5.4 Wait for initialization (~90 seconds)
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### 5.5 Trigger the DAG
1. Open Airflow: http://localhost:8088 (admin/admin)
2. Enable DAG: `amazon_dual_pipeline_orchestration`
3. Trigger manually — typical run completes in ~2 minutes

---

## 6) Data Schema

### OLTP: `orders_live` (Postgres)
| Column | Type | Description |
|--------|------|-------------|
| day | DATE | Aggregation date |
| category | TEXT | Product category |
| country | TEXT | Customer country |
| paymentmethod | TEXT | Payment method |
| totalorders | BIGINT | Number of orders |
| totalquantity | BIGINT | Total items sold |
| totalrevenue | DOUBLE PRECISION | Gross revenue |
| totalnetrevenue | DOUBLE PRECISION | Revenue after discounts |
| avgordervalue | DOUBLE PRECISION | Average order value |
| uniquecustomers | BIGINT | Distinct customers |
| processing_timestamp | TIMESTAMP | When record was processed |

### OLAP: Hive/Trino Tables
- `hive.datalake.bronze_amazon_sales_csv` — Raw CSV (21 columns)
- `hive.datalake.silver_amazon_sales` — Cleaned Parquet partitioned by year/month (24 columns)
- `hive.datalake.gold_amazon_sales_daily` — Aggregated Parquet (10 columns, same as OLTP minus processing_timestamp)

---

## 7) Trino Analytics

Trino is exposed on http://localhost:8090.

```sql
-- Top revenue categories
SELECT category, SUM(totalrevenue) AS revenue
FROM hive.datalake.gold_amazon_sales_daily
GROUP BY category ORDER BY revenue DESC;

-- Daily revenue trend
SELECT day, SUM(totalrevenue) AS revenue
FROM hive.datalake.gold_amazon_sales_daily
GROUP BY day ORDER BY day;

-- Revenue by country
SELECT country, SUM(totalrevenue) AS revenue
FROM hive.datalake.gold_amazon_sales_daily
GROUP BY country ORDER BY revenue DESC;
```

Power BI connection: Host `localhost`, Port `8090`, Catalog `hive`, Schema `datalake`.

---

## 8) Streamlit Dashboard

Open: http://localhost:8502

Shows aggregated sales KPIs from Postgres:
- Total Revenue, Orders, Average Order Value, Unique Customers
- Daily revenue trend chart
- Revenue by category (bar chart)
- Payment method distribution (pie chart)
- Revenue by country (bar chart)
- Revenue vs Net Revenue comparison
- Sidebar filters: date range, category, country

---

## 9) Troubleshooting

### Airflow DockerOperator fails
Ensure Docker socket is mounted and `DOCKER_NETWORK` matches your compose network (default: `talan-bootcamp-final-stage_netw`).

### No data in Postgres
Check Spark streaming task logs in Airflow. Verify Kafka topic has data:
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic amazon-sales --from-beginning --max-messages 3
```

### Trino cannot see tables
Check Hive metastore init and table creation:
```bash
docker logs hive-metastore-init
docker logs hive-server
docker exec trino trino --execute "SHOW TABLES FROM hive.datalake"
```

### Hive metastore CDS error (legacy)
The `bde2020/hive-metastore-postgresql:2.3.0` image has a corrupted pre-built schema. This project uses `schematool -initSchema` (via `hive-metastore-init` service) with a standard PostgreSQL 15 instance to avoid this issue.

---

## 10) Project Structure

```
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── amazon_orchestration_dag.py
├── data/
│   └── Amazon.csv                    # 100,000 sales records
├── hive/
│   ├── hive-site.xml                 # Hive configuration
│   └── init_metastore_db.sql         # PostgreSQL init (user + database)
├── postgres/
│   └── init_mart.sql                 # OLTP table schema (aggregated)
├── producer/
│   ├── Dockerfile
│   ├── kafka_producer_amazon.py      # CSV -> Kafka producer
│   └── requirements.txt
├── spark/
│   ├── Dockerfile
│   ├── spark_olap_csv_to_bronze_hdfs.py    # CSV -> HDFS Bronze
│   ├── spark_olap_bronze_to_silver.py      # Bronze -> Silver
│   ├── spark_olap_silver_to_gold.py        # Silver -> Gold
│   └── spark_oltp_streaming_to_postgres.py # Kafka -> aggregate -> Postgres
├── streamlit/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── streamlit_dashboard.py
├── trino/
│   └── etc/
│       ├── config.properties
│       ├── catalog/hive.properties
│       └── hadoop/
├── create_hive_tables.sql            # Hive DDL for Bronze/Silver/Gold
├── docker-compose.yml
├── commands.txt                      # Quick reference commands
└── README.md
```
