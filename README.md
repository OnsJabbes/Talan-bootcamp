# 🚀 Data Value Chain in the AI Era - Amazon Sales Pipeline

## Overview
This project demonstrates a complete **Data Value Chain** implementation showing how data flows through the **Collect → Store → Process** stages using modern big data technologies.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA VALUE CHAIN PIPELINE                         │
└─────────────────────────────────────────────────────────────────────┘

1. 📊 COLLECT (Data Ingestion)
   └─> Amazon.csv → Python Producer → Kafka Topic (supermarket-sales)

2. 🚛 TRANSPORT (Message Streaming)  
   └─> Apache Kafka (Topic: supermarket-sales)

3. ⚙️ PROCESS (Stream Processing)
   └─> Spark Structured Streaming

4. 💾 STORE (Data Lake - 3 Layers)
   ├─> 🥉 Bronze Layer: Raw JSON in HDFS
   ├─> 🥈 Silver Layer: Clean Parquet in HDFS (partitioned)
   └─> 🥇 Gold Layer: Aggregated KPIs in Hive Tables
```

---

## 🏗️ Architecture Components

### **1. Data Collection**
- **Source**: `Amazon.csv` (100K+ sales records)
- **Producer**: Python Kafka producer reads CSV and streams to Kafka
- **Topic**: `supermarket-sales`

### **2. Message Streaming**
- **Kafka**: Message broker for real-time data streaming
- **Zookeeper**: Manages Kafka cluster coordination

### **3. Stream Processing & Storage**
- **Spark Structured Streaming**: Processes Kafka stream
- **HDFS**: Hadoop Distributed File System for storage
- **Data Lake Layers**:
  - **Bronze** (Raw): JSON format, exact copy from Kafka
  - **Silver** (Clean): Parquet format, cleaned & enriched data
  - **Gold** (Curated): Hive tables with aggregated KPIs

---

## 📁 Project Structure

```
first_week_bootcamp_1/
├── Amazon.csv                        # Source data
├── docker-compose.yml                # Docker orchestration
├── Dockerfile                        # Python producer image
├── requirements.txt                  # Python dependencies
├── kafka_producer_amazon.py          # Kafka producer script
├── spark_kafka_hdfs_consumer.py      # Spark streaming consumer
└── data/                             # Generated data directories
    ├── namenode/                     # HDFS namenode data
    ├── datanode/                     # HDFS datanode data
    └── spark/                        # Spark working directory
```

---

## 🚀 How to Run

### Prerequisites
- Docker & Docker Compose installed
- At least 8GB RAM allocated to Docker
- Ports available: 2181, 9092, 8080, 7077, 9870, 8020

### Step 1: Start the Infrastructure
```powershell
# Start all services (Kafka, Zookeeper, Spark, Hadoop)
docker-compose up -d zookeeper kafka namenode datanode spark-master spark-worker-1 spark-worker-2 spark-worker-3

# Wait for all services to be healthy (30-60 seconds)
docker-compose ps
```

### Step 2: Create Kafka Topic
```powershell
# Create the Kafka topic
docker exec kafka-iot kafka-topics --create --topic supermarket-sales --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Verify topic creation
docker exec kafka-iot kafka-topics --list --bootstrap-server localhost:9092
```

### Step 3: Start the Data Pipeline
```powershell
# Start Spark streaming consumer (listens to Kafka and writes to HDFS)
docker-compose up -d spark-streaming

# Start Kafka producer (reads CSV and sends to Kafka)
docker-compose up -d producer
```

### Step 4: Monitor the Pipeline
```powershell
# Monitor producer logs
docker logs -f producer-amazon

# Monitor Spark streaming logs
docker logs -f spark-streaming-consumer

# Check Kafka messages
docker exec kafka-iot kafka-console-consumer --bootstrap-server localhost:9092 --topic supermarket-sales --from-beginning --max-messages 10
```

---

## 📊 Accessing the Data

### **HDFS Web UI**
- URL: http://localhost:9870
- Navigate to: `Utilities > Browse the file system`
- Data paths:
  - Bronze: `/datalake/bronze/amazon_sales/`
  - Silver: `/datalake/silver/amazon_sales/`
  - Gold: `/datalake/gold/amazon_sales_aggregated/`

### **Spark Web UI**
- Master: http://localhost:8080
- Application: http://localhost:4040 (when running)

### **Query Data from HDFS**
```powershell
# List files in Bronze layer
docker exec namenode hdfs dfs -ls /datalake/bronze/amazon_sales/

# List files in Silver layer (partitioned by Year/Month)
docker exec namenode hdfs dfs -ls -R /datalake/silver/amazon_sales/

# View sample data
docker exec namenode hdfs dfs -cat /datalake/bronze/amazon_sales/*.json | head -n 5
```

---

## 🎯 Data Flow Details

### **Bronze Layer** (Raw History)
- **Format**: JSON
- **Schema**: Exact copy from Kafka (all 20 fields)
- **Partitioning**: None
- **Purpose**: Immutable raw data for reprocessing

### **Silver Layer** (Clean Data)
- **Format**: Parquet (optimized columnar format)
- **Partitioning**: By `Year` and `Month`
- **Enrichments**:
  - Date components (Year, Month, Quarter, DayOfWeek)
  - Calculated fields (Revenue, DiscountAmount, NetRevenue)
  - Processing timestamp
- **Quality**: Filtered (TotalAmount > 0, Quantity > 0)

### **Gold Layer** (KPIs for Analytics)
- **Format**: Hive tables (Parquet)
- **Aggregations**:
  - Total Orders
  - Total Quantity Sold
  - Total Revenue & Net Revenue
  - Average Order Value
  - Unique Customers
- **Dimensions**: Category, Country, PaymentMethod
- **Time Window**: 1-minute aggregations

---

## 🔧 Configuration

### Environment Variables (docker-compose.yml)
```yaml
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=supermarket-sales
HDFS_URL=hdfs://namenode:8020
```

### Tuning Parameters
- **Producer delay**: 0.5 seconds between messages (configurable in script)
- **Spark batch interval**: 10 seconds for Bronze/Silver, 30 seconds for Gold
- **Kafka partitions**: 3 (for parallel processing)

---

## 🛠️ Troubleshooting

### Services not starting?
```powershell
# Check service status
docker-compose ps

# View logs for specific service
docker logs zookeeper-iot
docker logs kafka-iot
docker logs namenode
```

### Can't connect to Kafka?
```powershell
# Verify Kafka is ready
docker exec kafka-iot kafka-broker-api-versions --bootstrap-server localhost:9092
```

### HDFS not accessible?
```powershell
# Check namenode status
docker exec namenode hdfs dfsadmin -report
```

---

## 🧹 Cleanup

```powershell
# Stop all services
docker-compose down

# Remove data volumes (WARNING: deletes all data)
docker-compose down -v
rm -rf data/
```

---

## 📈 Next Steps (Future Enhancements)

- [ ] **Orchestrate**: Add Apache Airflow for workflow orchestration
- [ ] **Analyze**: Connect Jupyter notebook for data analysis
- [ ] **AI/ML**: Add ML models for predictions (demand forecasting, customer segmentation)
- [ ] **Visualize**: Connect Power BI / Grafana for dashboards
- [ ] **Act**: Real-time alerts and automated decision-making

---

## 📚 Technologies Used

| Component | Technology | Version |
|-----------|-----------|---------|
| **Message Broker** | Apache Kafka | 5.1.0 |
| **Coordination** | Apache Zookeeper | 5.1.0 |
| **Stream Processing** | Apache Spark | 3.0.0 |
| **Storage** | Hadoop HDFS | 3.1.3 |
| **Producer** | Python | 3.8 |
| **Libraries** | kafka-python, pyspark | 2.0.2, 3.0.0 |

---

## 📄 License
Educational project for learning data engineering concepts.

---

## 👥 Contributors
Talan Bootcamp - Week 1 Project
