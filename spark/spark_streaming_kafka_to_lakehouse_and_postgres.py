# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_date, year, month,
    window, count, sum, avg, approx_count_distinct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# -----------------------
# ENV
# -----------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "amazon-sales")
HDFS_URL     = os.getenv("HDFS_URL", "hdfs://namenode:8020")

PG_URL  = os.getenv("PG_URL", "jdbc:postgresql://postgres-mart:5432/martdb")
PG_USER = os.getenv("PG_USER", "mart")
PG_PASS = os.getenv("PG_PASS", "mart")

BRONZE_PATH = "/datalake/bronze/amazon_sales"
SILVER_PATH = "/datalake/silver/amazon_sales"
GOLD_PATH   = "/datalake/gold/amazon_sales_aggregated"

# Separate checkpoints per sink (important)
CKPT_BRONZE = "/checkpoints/bronze"
CKPT_SILVER = "/checkpoints/silver"
CKPT_OLTP   = "/checkpoints/oltp_orders_pg"
CKPT_KPI    = "/checkpoints/olap_kpi_1min_pg_complete"
CKPT_GOLD   = "/checkpoints/gold_parquet_complete"

# -----------------------
# SCHEMA
# -----------------------
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
    StructField("SellerID", StringType(), True),
])

# -----------------------
# SPARK
# -----------------------
def spark_session():
    return (
        SparkSession.builder
        .appName("AmazonSalesStreaming")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

# -----------------------
# READ KAFKA -> BRONZE DF
# -----------------------
def read_kafka(spark):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load())

def to_bronze(kafka_df):
    parsed = kafka_df.select(
        col("timestamp").alias("kafka_timestamp"),
        col("topic"), col("partition"), col("offset"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("kafka_timestamp", "topic", "partition", "offset", "data.*")

    return parsed.withColumn("ingestion_timestamp", current_timestamp())

# -----------------------
# SILVER TRANSFORM
# -----------------------
def to_silver(bronze_df):
    df = (bronze_df
        .withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))
        .withColumn("year", year(col("OrderDate")))
        .withColumn("month", month(col("OrderDate")))
        .withColumn("Revenue", col("Quantity") * col("UnitPrice"))
        .withColumn("DiscountAmount", col("Revenue") * col("Discount"))
        .withColumn("NetRevenue", col("Revenue") - col("DiscountAmount"))
        .withColumn("processing_timestamp", current_timestamp())
        .filter(col("TotalAmount") > 0)
        .filter(col("Quantity") > 0)
    )
    return df

# -----------------------
# SINKS: HDFS
# -----------------------
def write_bronze(bronze_df):
    return (bronze_df.writeStream
        .outputMode("append")
        .format("json")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CKPT_BRONZE)
        .trigger(processingTime="10 seconds")
        .start()
    )

def write_silver(silver_df):
    return (silver_df.writeStream
        .outputMode("append")
        .format("parquet")
        .partitionBy("year", "month")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CKPT_SILVER)
        .trigger(processingTime="10 seconds")
        .start()
    )

# -----------------------
# JDBC HELPERS
# -----------------------
def foreach_batch_jdbc(dbtable, mode="append"):
    def write_fn(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        (batch_df.write
         .format("jdbc")
         .option("url", PG_URL)
         .option("dbtable", dbtable)
         .option("user", PG_USER)
         .option("password", PG_PASS)
         .option("driver", "org.postgresql.Driver")
         .mode(mode)
         .save())
    return write_fn

# -----------------------
# OLTP -> POSTGRES (append)
# -----------------------
def write_oltp_orders_to_postgres(silver_df):
    orders_df = silver_df.select(
        "processing_timestamp","OrderID","OrderDate","CustomerID","CustomerName",
        "ProductID","ProductName","Category","Brand","Quantity","UnitPrice",
        "Discount","Tax","ShippingCost","TotalAmount","Revenue","DiscountAmount",
        "NetRevenue","PaymentMethod","OrderStatus","City","State","Country","SellerID"
    )

    return (orders_df.writeStream
        .foreachBatch(foreach_batch_jdbc("orders_live", mode="append"))
        .option("checkpointLocation", CKPT_OLTP)
        .trigger(processingTime="10 seconds")
        .start()
    )

# -----------------------
# KPI DF (computed once)
# -----------------------
def build_kpi_1min(silver_df):
    # For demo: processing-time windows produce results immediately with COMPLETE mode
    return (silver_df.groupBy(
            window(col("processing_timestamp"), "1 minute"),
            col("Category"), col("Country"), col("PaymentMethod")
        )
        .agg(
            count("OrderID").alias("totalorders"),
            sum("Quantity").alias("totalquantity"),
            sum("TotalAmount").alias("totalrevenue"),
            sum("NetRevenue").alias("totalnetrevenue"),
            avg("TotalAmount").alias("avgordervalue"),
            approx_count_distinct("CustomerID").alias("uniquecustomers"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("Category").alias("category"),
            col("Country").alias("country"),
            col("PaymentMethod").alias("paymentmethod"),
            "totalorders","totalquantity","totalrevenue","totalnetrevenue","avgordervalue","uniquecustomers",
            current_timestamp().alias("processing_timestamp")
        )
    )

# -----------------------
# OLAP KPI -> POSTGRES (complete + overwrite)
# -----------------------
def write_olap_kpi_1min_to_postgres(kpi_df):
    # overwrite each micro-batch so Power BI reads current state easily
    return (kpi_df.writeStream
        .outputMode("complete")
        .foreachBatch(foreach_batch_jdbc("kpi_1min", mode="overwrite"))
        .option("checkpointLocation", CKPT_KPI)
        .trigger(processingTime="30 seconds")
        .start()
    )

# -----------------------
# GOLD PARQUET from KPI (complete + overwrite path each batch)
# -----------------------
def foreach_batch_parquet(path):
    def write_fn(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        (batch_df
         .write
         .mode("append")
         .parquet(path))
    return write_fn

def write_gold_parquet_from_kpi(kpi_df):
    return (kpi_df.writeStream
        .outputMode("update")  # or "append" (update is fine because sink is foreachBatch)
        .foreachBatch(foreach_batch_parquet(GOLD_PATH))
        .option("checkpointLocation", "/checkpoints/gold_parquet")
        .trigger(processingTime="30 seconds")
        .start())

# -----------------------
# MAIN
# -----------------------
def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df  = read_kafka(spark)
    bronze_df = to_bronze(kafka_df)
    silver_df = to_silver(bronze_df)

    kpi_df = build_kpi_1min(silver_df)

    # Start queries
    q1 = write_bronze(bronze_df)
    q2 = write_silver(silver_df)
    q3 = write_oltp_orders_to_postgres(silver_df)
    q4 = write_olap_kpi_1min_to_postgres(kpi_df)
    q5 = write_gold_parquet_from_kpi(kpi_df)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
