import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum, approx_count_distinct

HDFS_URL  = os.getenv("HDFS_URL", "hdfs://namenode:8020")
SILVER_PATH = "/datalake/silver/amazon_sales"

PG_URL  = os.getenv("PG_URL", "jdbc:postgresql://postgres-mart:5432/martdb")
PG_USER = os.getenv("PG_USER", "mart")
PG_PASS = os.getenv("PG_PASS", "mart")

def spark_session():
    return (SparkSession.builder
        .appName("AmazonDailyKPIBatch")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", HDFS_URL)
        .getOrCreate())

def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SILVER_PATH)

    df = df.withColumn("day", to_date(col("OrderDate")))

    daily = (df.groupBy("day", "Category", "Country")
        .agg(
            count("OrderID").alias("totalorders"),
            sum("Quantity").alias("totalquantity"),
            sum("TotalAmount").alias("totalrevenue"),
            sum("NetRevenue").alias("totalnetrevenue"),
            approx_count_distinct("CustomerID").alias("uniquecustomers")
        )
        .select(
            col("day"),
            col("Category").alias("category"),
            col("Country").alias("country"),
            "totalorders","totalquantity","totalrevenue","totalnetrevenue","uniquecustomers"
        )
    )

    # Replace daily table each run (simple for demo)
    (daily.write.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "kpi_daily")
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save())

    spark.stop()

if __name__ == "__main__":
    main()
