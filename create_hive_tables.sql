CREATE DATABASE IF NOT EXISTS datalake;
USE datalake;

-- BRONZE (CSV) : raw from Kafka
DROP TABLE IF EXISTS bronze_amazon_sales_csv;
CREATE EXTERNAL TABLE bronze_amazon_sales_csv (
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/bronze/amazon_sales_csv'
TBLPROPERTIES ("skip.header.line.count"="1");

-- SILVER (Parquet) : cleaned/enriched
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

-- GOLD (Parquet) : daily KPIs
DROP TABLE IF EXISTS gold_amazon_sales_daily;
CREATE EXTERNAL TABLE gold_amazon_sales_daily (
  day DATE,
  category STRING,
  country STRING,
  paymentmethod STRING,
  totalorders BIGINT,
  totalquantity BIGINT,
  totalrevenue DOUBLE,
  totalnetrevenue DOUBLE,
  avgordervalue DOUBLE,
  uniquecustomers BIGINT
)
STORED AS PARQUET
LOCATION '/datalake/gold/amazon_sales_gold';
