# Amazon Sales Lakehouse - Airflow Orchestrated OLTP + OLAP

## 1) Project Overview

This project implements two coordinated pipelines (OLTP + OLAP) orchestrated by Apache Airflow.

### OLTP (Operational / Real-time)
- CSV -> Kafka -> Spark Structured Streaming -> Postgres -> Streamlit
- The Streamlit dashboard reads `orders_live` in Postgres for live KPIs and visualizations.

### OLAP (Analytical / Lakehouse)
- Kafka -> Spark batch consumer -> HDFS Bronze (CSV)
- Spark batch -> HDFS Silver (Parquet)
- Spark batch -> HDFS Gold (Parquet)
- Trino exposes Gold tables for Power BI

Airflow orchestrates both workflows inside the same project.

---

## 2) Architecture (End-to-End)

### 2.1 OLTP Flow
`Amazon.csv -> Kafka -> Spark Streaming -> Postgres -> Streamlit`

### 2.2 OLAP Flow
`Kafka -> Spark Batch -> HDFS Bronze -> Spark Batch -> HDFS Silver -> Spark Batch -> HDFS Gold -> Trino -> Power BI`

---

## 3) Technologies Used

| Layer | Technology |
|------|------------|
| Orchestration | Apache Airflow |
| Ingestion | Apache Kafka |
| Processing | Spark Structured Streaming + Spark Batch |
| Storage | HDFS (Bronze/Silver/Gold) |
| OLTP DB | PostgreSQL |
| BI Query Engine | Trino |
| Visualization | Streamlit + Power BI |
| Deployment | Docker Compose |

---

## 4) Containers / Services

Core services:
- zookeeper, kafka
- spark-master, spark-worker-1
- namenode, datanode, hdfs-init
- hive-metastore, hive-server
- postgres-mart
- trino
- streamlit
- airflow-webserver, airflow-scheduler

Build-only images:
- amazon-spark-jobs (Spark jobs used by Airflow)
- amazon-producer (Kafka producer used by Airflow)

---

## 5) Quick Start (Docker)

### 5.1 Clean start
```bash
docker compose down -v
```

### 5.2 Build images
```bash
docker compose --profile build build
```

### 5.3 Start the stack
```bash
docker compose up -d
```

---

## 6) Airflow Orchestration

Airflow runs both pipelines in the DAG:
`amazon_dual_pipeline_orchestration`

Open Airflow UI:
`http://localhost:8088`

Trigger the DAG manually (recommended for demo). The DAG includes:

### OLTP Task Group
1. Truncate `orders_live`
2. Run Kafka producer (CSV -> Kafka)
3. Run Spark Streaming (Kafka -> Postgres) for ~5 minutes

### OLAP Task Group
1. Kafka -> HDFS Bronze (CSV)
2. Bronze -> Silver (Parquet)
3. Silver -> Gold (Parquet)

---

## 7) Trino + Power BI

Trino is exposed on `http://localhost:8090`.

Sample query:
```sql
SELECT day, category, totalrevenue
FROM hive.datalake.gold_amazon_sales_daily
ORDER BY day DESC
LIMIT 20;
```

Power BI can connect to Trino:
- Host: localhost
- Port: 8090
- Catalog: hive
- Schema: datalake

---

## 8) Streamlit Dashboard

Open Streamlit:
`http://localhost:8502`

Dashboard reads Postgres table `orders_live`.

---

## 9) Troubleshooting

### Issue A - Airflow DockerOperator fails
Ensure Docker socket is mounted inside Airflow containers and `DOCKER_NETWORK` matches your compose network name.

### Issue B - No data in Postgres
Check Spark streaming task logs in Airflow and Kafka topic has data.

### Issue C - Trino cannot see tables
Confirm Hive tables were created and HDFS paths exist:
```bash
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake"
```

---

## 10) Useful Commands

### Kafka check
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic amazon-sales --from-beginning --max-messages 3
```

### HDFS check
```bash
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/bronze/amazon_sales_csv | tail"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/silver/amazon_sales | tail"
docker exec -it namenode bash -lc "/opt/hadoop-3.1.3/bin/hdfs dfs -ls /datalake/gold/amazon_sales_gold | tail"
```

### Postgres check
```bash
docker exec -it postgres-mart psql -U mart -d martdb -c "select count(*) from orders_live;"
```
