# -*- coding: utf-8 -*-
"""
OLTP Streaming: Kafka -> Spark Structured Streaming -> Postgres (aggregated).
Reads raw order JSON from Kafka, computes the same aggregations as the OLAP
Gold layer (day, category, country, paymentmethod), and upserts the result
into Postgres table `orders_live` so Streamlit can display live KPIs.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, count, sum as _sum, avg,
    approx_count_distinct, current_timestamp
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
        .appName("AmazonOLTPStreamingAggregated")
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


def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka(spark)

    # Parse JSON from Kafka value
    parsed = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Filter invalid rows and compute derived columns
    cleaned = (parsed
        .filter(col("TotalAmount") > 0)
        .filter(col("Quantity") > 0)
        .withColumn("day", to_date(col("OrderDate"), "yyyy-MM-dd"))
        .withColumn("Revenue", col("Quantity") * col("UnitPrice"))
        .withColumn("DiscountAmount",
                     col("Quantity") * col("UnitPrice") * col("Discount"))
        .withColumn("NetRevenue",
                     col("Quantity") * col("UnitPrice")
                     - col("Quantity") * col("UnitPrice") * col("Discount"))
    )

    # Aggregate per micro-batch (same logic as OLAP Gold layer)
    def aggregate_and_write(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        gold = (batch_df
            .groupBy("day", "Category", "Country", "PaymentMethod")
            .agg(
                count("OrderID").alias("totalorders"),
                _sum("Quantity").alias("totalquantity"),
                _sum("TotalAmount").alias("totalrevenue"),
                _sum("NetRevenue").alias("totalnetrevenue"),
                avg("TotalAmount").alias("avgordervalue"),
                approx_count_distinct("CustomerID").alias("uniquecustomers"),
            )
            .select(
                col("day"),
                col("Category").alias("category"),
                col("Country").alias("country"),
                col("PaymentMethod").alias("paymentmethod"),
                "totalorders", "totalquantity", "totalrevenue",
                "totalnetrevenue", "avgordervalue", "uniquecustomers",
            )
            .withColumn("processing_timestamp", current_timestamp())
        )
        (gold.write
         .format("jdbc")
         .option("url", PG_URL)
         .option("dbtable", "orders_live")
         .option("user", PG_USER)
         .option("password", PG_PASS)
         .option("driver", "org.postgresql.Driver")
         .mode("append")
         .save())

    query = (cleaned.writeStream
        .foreachBatch(aggregate_and_write)
        .option("checkpointLocation", CKPT_OLTP)
        .trigger(processingTime="10 seconds")
        .start()
    )

    run_seconds = int(os.getenv("RUN_SECONDS", "0"))
    if run_seconds > 0:
        spark.streams.awaitAnyTermination(run_seconds)
        for q in spark.streams.active:
            q.stop()
    else:
        spark.streams.awaitAnyTermination()

    spark.stop()


if __name__ == "__main__":
    main()
