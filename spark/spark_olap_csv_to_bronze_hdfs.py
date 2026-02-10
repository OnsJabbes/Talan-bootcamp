# -*- coding: utf-8 -*-
"""
OLAP Bronze Layer: Load Amazon.csv directly into HDFS Bronze (CSV).
Replaces the old Kafka-to-Bronze approach.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:8020")
CSV_FILE = os.getenv("CSV_FILE", "/opt/spark/jobs/data/Amazon.csv")
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
        .appName("AmazonOLAPCsvToBronzeHDFS")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .getOrCreate()
    )


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read CSV from the local filesystem inside the container
    local_csv = "file://" + CSV_FILE
    print("Reading CSV from: " + local_csv)
    df = (spark.read
          .option("header", "true")
          .schema(schema)
          .csv(local_csv))

    bronze = df.withColumn("ingestion_timestamp", current_timestamp())

    row_count = bronze.count()
    print("Loaded %d rows from CSV" % row_count)

    (bronze.write
     .mode("overwrite")
     .option("header", "true")
     .csv(BRONZE_PATH))

    print("Bronze layer written to " + BRONZE_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
