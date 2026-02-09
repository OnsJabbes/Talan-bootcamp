# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_date, year, month
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "amazon-sales")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres-mart:5432/martdb")
PG_USER = os.getenv("PG_USER", "mart")
PG_PASS = os.getenv("PG_PASS", "mart")

CKPT_OLTP = "/checkpoints/oltp_orders_pg"

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
        .appName("AmazonOLTPStreaming")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_kafka(spark):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", KAFKA_STARTING_OFFSETS)
            .option("failOnDataLoss", "false")
            .load())


def transform(kafka_df):
    parsed = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    return (parsed
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


def write_oltp_orders_to_postgres(silver_df):
    orders_df = silver_df.select(
        "processing_timestamp", "OrderID", "OrderDate", "CustomerID", "CustomerName",
        "ProductID", "ProductName", "Category", "Brand", "Quantity", "UnitPrice",
        "Discount", "Tax", "ShippingCost", "TotalAmount", "Revenue", "DiscountAmount",
        "NetRevenue", "PaymentMethod", "OrderStatus", "City", "State", "Country", "SellerID"
    )

    return (orders_df.writeStream
        .foreachBatch(foreach_batch_jdbc("orders_live", mode="append"))
        .option("checkpointLocation", CKPT_OLTP)
        .trigger(processingTime="10 seconds")
        .start()
    )


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka(spark)
    silver_df = transform(kafka_df)

    write_oltp_orders_to_postgres(silver_df)

    run_seconds = int(os.getenv("RUN_SECONDS", "0"))
    if run_seconds > 0:
        spark.streams.awaitAnyTermination(run_seconds * 1000)
        for q in spark.streams.active:
            q.stop()
    else:
        spark.streams.awaitAnyTermination()

    spark.stop()


if __name__ == "__main__":
    main()
