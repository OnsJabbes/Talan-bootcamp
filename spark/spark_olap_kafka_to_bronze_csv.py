# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "amazon-sales")
HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:8020")

BRONZE_PATH = "/datalake/bronze/amazon_sales_csv"

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


def spark_session():
    return (
        SparkSession.builder
        .appName("AmazonOLAPKafkaToBronzeCSV")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .getOrCreate()
    )


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = (spark.read.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BROKER)
          .option("subscribe", KAFKA_TOPIC)
          .option("startingOffsets", "earliest")
          .option("endingOffsets", "latest")
          .option("failOnDataLoss", "false")
          .load())

    parsed = df.select(
        col("timestamp").alias("kafka_timestamp"),
        col("topic"), col("partition"), col("offset"),
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("kafka_timestamp", "topic", "partition", "offset", "data.*")

    bronze = parsed.withColumn("ingestion_timestamp", current_timestamp())

    (bronze.write
     .mode("overwrite")
     .option("header", "true")
     .csv(BRONZE_PATH))

    spark.stop()


if __name__ == "__main__":
    main()
