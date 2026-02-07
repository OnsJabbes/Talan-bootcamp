CREATE DATABASE IF NOT EXISTS datalake;
USE datalake;

-- BRONZE (JSON) : lecture raw
DROP TABLE IF EXISTS bronze_amazon_sales;
CREATE EXTERNAL TABLE bronze_amazon_sales (
  kafka_timestamp TIMESTAMP,
  topic STRING,
  partition INT,
  `offset` BIGINT,
  OrderID STRING,
  OrderDate STRING,
  CustomerID STRING,
  CustomerName STRING,
  ProductID STRING,
  ProductName STRING,
  Category STRING,
  Brand STRING,
  Quantity INT,
  UnitPrice DOUBLE,
  Discount DOUBLE,
  Tax DOUBLE,
  ShippingCost DOUBLE,
  TotalAmount DOUBLE,
  PaymentMethod STRING,
  OrderStatus STRING,
  City STRING,
  State STRING,
  Country STRING,
  SellerID STRING,
  ingestion_timestamp TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/datalake/bronze/amazon_sales';

-- SILVER (Parquet) : data clean + enrichie
DROP TABLE IF EXISTS silver_amazon_sales;
CREATE EXTERNAL TABLE silver_amazon_sales (
  kafka_timestamp TIMESTAMP,
  topic STRING,
  partition INT,
  `offset` BIGINT,
  OrderID STRING,
  OrderDate DATE,
  CustomerID STRING,
  CustomerName STRING,
  ProductID STRING,
  ProductName STRING,
  Category STRING,
  Brand STRING,
  Quantity INT,
  UnitPrice DOUBLE,
  Discount DOUBLE,
  Tax DOUBLE,
  ShippingCost DOUBLE,
  TotalAmount DOUBLE,
  Revenue DOUBLE,
  DiscountAmount DOUBLE,
  NetRevenue DOUBLE,
  PaymentMethod STRING,
  OrderStatus STRING,
  City STRING,
  State STRING,
  Country STRING,
  SellerID STRING,
  processing_timestamp TIMESTAMP
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/datalake/silver/amazon_sales';

MSCK REPAIR TABLE silver_amazon_sales;

-- GOLD (Parquet) : KPIs
DROP TABLE IF EXISTS gold_amazon_sales_aggregated;
CREATE EXTERNAL TABLE gold_amazon_sales_aggregated (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  Category STRING,
  Country STRING,
  PaymentMethod STRING,
  TotalOrders BIGINT,
  TotalQuantity BIGINT,
  TotalRevenue DOUBLE,
  TotalNetRevenue DOUBLE,
  AvgOrderValue DOUBLE,
  UniqueCustomers BIGINT
)
STORED AS PARQUET
LOCATION '/datalake/gold/amazon_sales_aggregated';
