# -*- coding: utf-8 -*-
"""
Spark Structured Streaming Consumer
Reads from Kafka topic and writes to HDFS in Parquet format (Bronze, Silver, Gold layers)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'supermarket-sales')
HDFS_URL = os.getenv('HDFS_URL', 'hdfs://namenode:8020')

# HDFS Paths
BRONZE_PATH = "{}/datalake/bronze/amazon_sales".format(HDFS_URL)
SILVER_PATH = "{}/datalake/silver/amazon_sales".format(HDFS_URL)
GOLD_PATH = "{}/datalake/gold/amazon_sales_aggregated".format(HDFS_URL)

# Schema for Amazon sales data
schema = StructType([
    StructField("OrderID", StringType(), True),
    StructField("OrderDate", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Brand", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Tax", DoubleType(), True),
    StructField("ShippingCost", DoubleType(), True),
    StructField("TotalAmount", DoubleType(), True),
    StructField("PaymentMethod", StringType(), True),
    StructField("OrderStatus", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("SellerID", StringType(), True)
])


def create_spark_session():
    """Create and configure Spark session"""
    print("Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName("AmazonSalesKafkaToHDFS") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
        .config("spark.hadoop.fs.defaultFS", HDFS_URL) \
        .config("spark.sql.streaming.checkpointLocation", "{}/checkpoints".format(HDFS_URL)) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session created successfully")
    print("Spark version: {}".format(spark.version))
    print("HDFS URL: {}".format(HDFS_URL))
    
    return spark


def read_from_kafka(spark):
    """Read streaming data from Kafka"""
    print("\nReading from Kafka topic: {}".format(KAFKA_TOPIC))
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("Connected to Kafka stream")
    return df


def process_bronze_layer(kafka_df):
    """
    Bronze Layer: Raw data ingestion (JSON format)
    Store data exactly as received from Kafka
    """
    print("\nProcessing BRONZE layer (Raw History)...")
    
    # Parse JSON from Kafka value
    parsed_df = kafka_df.select(
        col("timestamp").alias("kafka_timestamp"),
        col("topic"),
        col("partition"),
        col("offset"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("kafka_timestamp", "topic", "partition", "offset", "data.*")
    
    # Add ingestion timestamp
    bronze_df = parsed_df.withColumn("ingestion_timestamp", current_timestamp())
    
    print("Bronze schema created with {} columns".format(len(bronze_df.columns)))
    return bronze_df


def process_silver_layer(bronze_df):
    """
    Silver Layer: Cleaned and validated data (Parquet format)
    Apply data quality rules and transformations
    """
    print("\nProcessing SILVER layer (Clean Data)...")
    
    silver_df = bronze_df \
        .withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd")) \
        .withColumn("Year", year(col("OrderDate"))) \
        .withColumn("Month", month(col("OrderDate"))) \
        .withColumn("Quarter", quarter(col("OrderDate"))) \
        .withColumn("DayOfWeek", dayofweek(col("OrderDate"))) \
        .withColumn("Revenue", col("Quantity") * col("UnitPrice")) \
        .withColumn("DiscountAmount", col("Revenue") * col("Discount")) \
        .withColumn("NetRevenue", col("Revenue") - col("DiscountAmount")) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .filter(col("TotalAmount") > 0) \
        .filter(col("Quantity") > 0)
    
    print("Silver layer enriched with additional columns")
    return silver_df


def write_to_bronze(bronze_df):
    """Write raw data to Bronze layer in JSON format"""
    print("\nWriting to BRONZE layer: {}".format(BRONZE_PATH))
    
    query = bronze_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", BRONZE_PATH) \
        .option("checkpointLocation", "{}/checkpoints/bronze".format(HDFS_URL)) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("Bronze layer stream started")
    return query


def write_to_silver(silver_df):
    """Write cleaned data to Silver layer in Parquet format"""
    print("\nWriting to SILVER layer: {}".format(SILVER_PATH))
    
    query = silver_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .partitionBy("Year", "Month") \
        .option("path", SILVER_PATH) \
        .option("checkpointLocation", "{}/checkpoints/silver".format(HDFS_URL)) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("Silver layer stream started (partitioned by Year, Month)")
    return query


def write_to_gold(silver_df):
    """
    Write aggregated data to Gold layer (KPIs and Analytics)
    Aggregations by Category, Country, Payment Method
    """
    print("\nWriting to GOLD layer: {}".format(GOLD_PATH))
    
    # Add watermark for handling late data
    silver_with_watermark = silver_df.withWatermark("processing_timestamp", "10 minutes")
    
    # Aggregation by Category and Country
    gold_df = silver_with_watermark \
        .groupBy(
            window(col("processing_timestamp"), "1 minute"),
            col("Category"),
            col("Country"),
            col("PaymentMethod")
        ) \
        .agg(
            count("OrderID").alias("TotalOrders"),
            sum("Quantity").alias("TotalQuantity"),
            sum("TotalAmount").alias("TotalRevenue"),
            sum("NetRevenue").alias("TotalNetRevenue"),
            avg("TotalAmount").alias("AvgOrderValue"),
            approx_count_distinct("CustomerID").alias("UniqueCustomers")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("Category"),
            col("Country"),
            col("PaymentMethod"),
            col("TotalOrders"),
            col("TotalQuantity"),
            col("TotalRevenue"),
            col("TotalNetRevenue"),
            col("AvgOrderValue"),
            col("UniqueCustomers")
        )
    
    query = gold_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", GOLD_PATH) \
        .option("checkpointLocation", "{}/checkpoints/gold".format(HDFS_URL)) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("Gold layer stream started (aggregated KPIs)")
    return query


def main():
    """Main execution function"""
    print("=" * 80)
    print("SPARK STRUCTURED STREAMING - Kafka to HDFS Data Lake")
    print("=" * 80)
    print("Kafka Broker: {}".format(KAFKA_BROKER))
    print("Kafka Topic: {}".format(KAFKA_TOPIC))
    print("HDFS URL: {}".format(HDFS_URL))
    print("=" * 80)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Read from Kafka
        kafka_df = read_from_kafka(spark)
        
        # Process layers
        bronze_df = process_bronze_layer(kafka_df)
        silver_df = process_silver_layer(bronze_df)
        
        # Start streaming queries
        bronze_query = write_to_bronze(bronze_df)
        silver_query = write_to_silver(silver_df)
        gold_query = write_to_gold(silver_df)
        
        print("\n" + "=" * 80)
        print("ALL STREAMING QUERIES STARTED SUCCESSFULLY")
        print("=" * 80)
        print("\nData Lake Structure:")
        print("   Bronze (Raw JSON):     {}".format(BRONZE_PATH))
        print("   Silver (Clean Parquet): {}".format(SILVER_PATH))
        print("   Gold (Aggregated):      {}".format(GOLD_PATH))
        print("\nStreaming is running... Press Ctrl+C to stop")
        print("=" * 80)
        
        # Wait for all queries to terminate
        bronze_query.awaitTermination()
        silver_query.awaitTermination()
        gold_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStreaming stopped by user")
    except Exception as e:
        print("\nERROR: {}".format(e))
        raise
    finally:
        spark.stop()
        print("\nSpark session closed")


if __name__ == "__main__":
    main()
