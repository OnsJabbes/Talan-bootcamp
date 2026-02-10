# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum, avg, approx_count_distinct

HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:8020")

SILVER_PATH = "/datalake/silver/amazon_sales"
GOLD_PATH = "/datalake/gold/amazon_sales_gold"


def spark_session():
    return (
        SparkSession.builder
        .appName("AmazonOLAPSilverToGold")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .getOrCreate()
    )


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SILVER_PATH)

    df = df.withColumn("day", to_date(col("OrderDate")))

    gold = (df.groupBy("day", "Category", "Country", "PaymentMethod")
        .agg(
            count("OrderID").alias("totalorders"),
            sum("Quantity").alias("totalquantity"),
            sum("TotalAmount").alias("totalrevenue"),
            sum("NetRevenue").alias("totalnetrevenue"),
            avg("TotalAmount").alias("avgordervalue"),
            approx_count_distinct("CustomerID").alias("uniquecustomers"),
        )
        .select(
            col("day"),
            col("Category").alias("category"),
            col("Country").alias("country"),
            col("PaymentMethod").alias("paymentmethod"),
            "totalorders", "totalquantity", "totalrevenue", "totalnetrevenue", "avgordervalue", "uniquecustomers"
        )
    )

    (gold.write
     .mode("overwrite")
     .parquet(GOLD_PATH))

    spark.stop()


if __name__ == "__main__":
    main()
