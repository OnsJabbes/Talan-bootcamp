# Amazon Sales Lakehouse - Airflow Orchestrated OLTP + OLAP

## 1) Project Overview

This project implements two coordinated data pipelines (OLTP + OLAP) orchestrated by Apache Airflow, processing 100,000 Amazon sales records through a complete lakehouse architecture.

### OLTP Pipeline (Real-Time Streaming)
```
Amazon.csv -> Kafka (progressive) -> Spark Structured Streaming -> Postgres -> Streamlit (live refresh)
```
The Kafka producer sends CSV rows **progressively** (1000 msg/min) while Spark Streaming runs **in parallel**, consuming messages and writing aggregated KPIs to Postgres every 10 seconds. The Streamlit dashboard auto-refreshes every 5 seconds, showing metrics increase in real time.

Aggregated KPIs (grouped by day, category, country, payment method): total orders, total quantity, total revenue, total net revenue, average order value, and unique customers.

### OLAP Pipeline (Batch / Medallion Architecture)
```
Amazon.csv -> HDFS Bronze (CSV) -> Spark Batch -> HDFS Silver (Parquet) -> Spark Batch -> HDFS Gold (Parquet) -> Hive -> Trino
```
Three-layer medallion architecture with Bronze (raw CSV), Silver (cleaned/enriched Parquet partitioned by year/month), and Gold (aggregated Parquet KPIs). Trino provides SQL analytics over all layers via Hive metastore.

---

## 2) Architecture

### 2.1 OLTP Flow (Real-Time)
```
Amazon.csv ──> Kafka Producer ──────────────────────────────────────┐
               (progressive,     runs IN PARALLEL                   │
                1000 msg/min)                                       │
                                                                    │
               Spark Streaming ──> Postgres (orders_live) ──> Streamlit
               (reads Kafka,       (aggregated KPIs)         (auto-refresh 5s)
                writes every 10s)
```

### 2.2 OLAP Flow (Batch)
```
Amazon.csv ──> HDFS Bronze ──> Spark Batch ──> HDFS Silver ──> Spark Batch ──> HDFS Gold ──> Hive/Trino
               (CSV)           (clean/enrich)  (Parquet)       (aggregate)     (Parquet)
```

### 2.3 Airflow DAG
```
start
├── oltp_pipeline (PARALLEL tasks):
│   ├── produce_csv_to_kafka        (sends 100K rows progressively ~2 min)
│   ├── spark_oltp_streaming        (Kafka -> aggregate -> Postgres every 10s)
│   └── [both finish] >> restart_streamlit_dashboard
│
└── olap_pipeline (SEQUENTIAL tasks):
    ├── csv_to_bronze_hdfs           (CSV -> HDFS Bronze)
    ├── bronze_to_silver             (Bronze -> Silver Parquet)
    └── silver_to_gold               (Silver -> Gold Parquet)
        └── end
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
| Visualization | Streamlit | 1.32.0 |
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
| Hive Metastore DB | hive-metastore-postgresql | 5432 | Metastore backend (PostgreSQL) |
| Hive Metastore Init | hive-metastore-init | - | Initializes Hive schema (schematool) |
| Hive Metastore | hive-metastore | 9083 | Thrift metastore service |
| Hive Server | hive-server | 10000 | HiveServer2 + table creation |
| Spark Master | spark-master | 8080, 7077 | Spark cluster coordinator |
| Spark Worker | spark-worker-1 | 8081 | Spark executor |
| Postgres Mart | postgres-mart | 5433 | OLTP aggregated data |
| Trino | trino | 8090 | SQL analytics engine |
| Streamlit | streamlit-oltp | 8502 | Real-time dashboard (auto-refresh 5s) |
| Airflow DB | airflow-db | - | Airflow metadata |
| Airflow Webserver | airflow-webserver | 8088 | Airflow UI |
| Airflow Scheduler | airflow-scheduler | - | DAG scheduling |

Build-only images (used as Docker containers by Airflow DockerOperator):
- `amazon-spark-jobs` — All Spark jobs (OLAP batch + OLTP streaming)
- `amazon-producer` — Kafka producer (progressive CSV to Kafka)

---

## 5) Quick Start

### Step 1: Clean environment
```bash
docker compose down -v --remove-orphans
```

### Step 2: Build custom images
```bash
# Build spark-jobs image (all 4 Spark scripts + Amazon.csv)
docker compose --profile build build

# Build producer image (Kafka CSV producer with progressive sending)
docker compose --profile manual build
```

### Step 3: Start all services
```bash
docker compose up -d
```

### Step 4: Wait ~90 seconds for initialization
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```
Init containers (`hdfs-init`, `hive-metastore-init`) will show `Exited (0)` — this is normal.

### Step 5: Restart Airflow Scheduler
The scheduler exits because it starts before the webserver finishes `airflow db init`. Just restart it:
```bash
docker start airflow-scheduler
```

### Step 6: Open web interfaces
- **Streamlit Dashboard** (open BEFORE triggering DAG): http://localhost:8502
- **Airflow UI**: http://localhost:8088 (admin / admin)
- **Spark Master UI**: http://localhost:8080
- **HDFS Namenode UI**: http://localhost:9870
- **Trino**: http://localhost:8090

### Step 7: Trigger the DAG
1. Go to http://localhost:8088
2. Find DAG: `amazon_dual_pipeline_orchestration`
3. Toggle the switch ON (left side) to enable it
4. Click the Play button → "Trigger DAG"
5. Watch Streamlit — **metrics increase in real time** as the pipeline runs!

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
- `hive.datalake.gold_amazon_sales_daily` — Aggregated Parquet (10 columns)

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

**Real-time features:**
- Auto-refreshes every 5 seconds during streaming
- Watch Total Orders and Total Revenue increase live as the DAG runs

**KPIs displayed:**
- Total Revenue, Orders, Net Revenue, Average Order Value, Quantity, Unique Customers
- Daily revenue trend chart
- Revenue by category (bar chart)
- Payment method distribution (pie chart)
- Revenue by country (bar chart)
- Revenue vs Net Revenue comparison
- Sidebar filters: date range, category, country, payment method

---

## 9) Verification Commands

After the DAG completes, verify both pipelines:

```bash
# OLTP — Check Kafka messages
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic amazon-sales --from-beginning --max-messages 5

# OLTP — Check Postgres aggregated data
docker exec -it postgres-mart psql -U mart -d martdb -c "SELECT count(*) FROM orders_live;"
docker exec -it postgres-mart psql -U mart -d martdb -c "SELECT day, category, country, totalorders, totalrevenue FROM orders_live LIMIT 10;"

# OLAP — Check HDFS layers
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/bronze/amazon_sales_csv | tail"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/silver/amazon_sales | tail"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/gold/amazon_sales_gold | tail"

# OLAP — Check Hive tables
docker exec -it hive-server bash -lc "/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n hive -e 'SHOW TABLES IN datalake;'"

# OLAP — Query via Trino
docker exec trino trino --execute "SELECT count(*) FROM hive.datalake.gold_amazon_sales_daily"
docker exec trino trino --execute "SELECT * FROM hive.datalake.gold_amazon_sales_daily LIMIT 5"
```

---

## 10) Troubleshooting

### Airflow Scheduler exits on first start
This is expected. The scheduler starts before `airflow db init` completes. Fix:
```bash
docker start airflow-scheduler
```

### Airflow DockerOperator fails
Ensure Docker socket is mounted and `DOCKER_NETWORK` matches your compose network (default: `talan-bootcamp-final-stage_netw`).
```bash
docker exec airflow-webserver ls -la /var/run/docker.sock
```

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

### General log inspection
```bash
docker logs -f airflow-webserver
docker logs -f airflow-scheduler
docker logs hive-metastore-init
docker logs hdfs-init
```

---

## 11) Shutdown

```bash
# Stop all services (keep data volumes for next time)
docker compose stop

# Stop and remove everything including all data
docker compose down -v --remove-orphans
```

---

## 12) Project Structure

```
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── amazon_orchestration_dag.py   # Airflow DAG (parallel OLTP + sequential OLAP)
├── data/
│   └── Amazon.csv                        # 100,000 sales records
├── hive/
│   ├── hive-site.xml                     # Hive configuration
│   └── init_metastore_db.sql             # PostgreSQL init (user + database)
├── postgres/
│   └── init_mart.sql                     # OLTP table schema (orders_live)
├── producer/
│   ├── Dockerfile
│   ├── kafka_producer_amazon.py          # Progressive CSV -> Kafka producer
│   └── requirements.txt
├── spark/
│   ├── Dockerfile
│   ├── spark_olap_csv_to_bronze_hdfs.py  # CSV -> HDFS Bronze
│   ├── spark_olap_bronze_to_silver.py    # Bronze -> Silver (Parquet)
│   ├── spark_olap_silver_to_gold.py      # Silver -> Gold (Parquet)
│   └── spark_oltp_streaming_to_postgres.py  # Kafka -> aggregate -> Postgres (every 10s)
├── streamlit/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── streamlit_dashboard.py            # Real-time dashboard (auto-refresh 5s)
├── trino/
│   └── etc/
│       ├── config.properties
│       ├── catalog/hive.properties
│       └── hadoop/
├── create_hive_tables.sql                # Hive DDL for Bronze/Silver/Gold
├── docker-compose.yml
├── commands.txt                          # Quick reference launch commands
└── README.md
```
