# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:8020")

BRONZE_PATH = "/datalake/bronze/amazon_sales_csv"
SILVER_PATH = "/datalake/silver/amazon_sales"

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
    StructField("ingestion_timestamp", TimestampType(), True),
])


def spark_session():
    return (
        SparkSession.builder
        .appName("AmazonOLAPBronzeToSilver")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = (spark.read
          .option("header", "true")
          .schema(schema)
          .csv(BRONZE_PATH))

    silver = (df
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

    (silver.write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(SILVER_PATH))

    spark.stop()


if __name__ == "__main__":
    main()
