-- Create database for datalake
CREATE DATABASE IF NOT EXISTS datalake;
USE datalake;

-- Bronze Layer: Raw JSON data
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_amazon_sales (
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
    ShipmentProvider STRING,
    DeliveryDate STRING,
    FeedbackScore INT,
    CustomerEmail STRING,
    PhoneNumber STRING
)
STORED AS PARQUET
LOCATION '/datalake/bronze/amazon_sales/';

-- Silver Layer: Cleaned and enriched data
CREATE EXTERNAL TABLE IF NOT EXISTS silver_amazon_sales (
    OrderID STRING,
    OrderDate TIMESTAMP,
    Year INT,
    Month INT,
    Quarter INT,
    DayOfWeek INT,
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
    PaymentMethod STRING,
    OrderStatus STRING,
    City STRING,
    State STRING,
    Country STRING,
    SellerID STRING
)
PARTITIONED BY (year_part INT, month_part INT)
STORED AS PARQUET
LOCATION '/datalake/silver/amazon_sales/';

-- Repair partitions for Silver table
MSCK REPAIR TABLE silver_amazon_sales;

-- Gold Layer: Aggregated KPIs (schema detected from Parquet)
CREATE EXTERNAL TABLE IF NOT EXISTS gold_amazon_sales_aggregated (
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
LOCATION '/datalake/gold/amazon_sales_aggregated/';

-- Optional view with normalized column names for easier querying
DROP VIEW IF EXISTS gold_amazon_sales_aggregated_view;
CREATE VIEW gold_amazon_sales_aggregated_view AS
SELECT
    window_start,
    window_end,
    Category AS category,
    Country AS country,
    PaymentMethod AS payment_method,
    TotalOrders AS total_orders,
    TotalQuantity AS total_quantity,
    TotalRevenue AS total_revenue,
    TotalNetRevenue AS total_net_revenue,
    AvgOrderValue AS avg_order_value,
    UniqueCustomers AS unique_customers
FROM gold_amazon_sales_aggregated;
