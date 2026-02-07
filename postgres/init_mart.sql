-- OLTP: live orders
CREATE TABLE IF NOT EXISTS orders_live (
  processing_timestamp TIMESTAMP,
  orderid TEXT,
  orderdate DATE,
  customerid TEXT,
  customername TEXT,
  productid TEXT,
  productname TEXT,
  category TEXT,
  brand TEXT,
  quantity INT,
  unitprice DOUBLE PRECISION,
  discount DOUBLE PRECISION,
  tax DOUBLE PRECISION,
  shippingcost DOUBLE PRECISION,
  totalamount DOUBLE PRECISION,
  revenue DOUBLE PRECISION,
  discountamount DOUBLE PRECISION,
  netrevenue DOUBLE PRECISION,
  paymentmethod TEXT,
  orderstatus TEXT,
  city TEXT,
  state TEXT,
  country TEXT,
  sellerid TEXT
);

CREATE INDEX IF NOT EXISTS idx_orders_live_ts ON orders_live(processing_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_orders_live_status ON orders_live(orderstatus);

-- OLAP: 1-minute KPIs for Power BI
CREATE TABLE IF NOT EXISTS kpi_1min (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  category TEXT,
  country TEXT,
  paymentmethod TEXT,
  totalorders BIGINT,
  totalquantity BIGINT,
  totalrevenue DOUBLE PRECISION,
  totalnetrevenue DOUBLE PRECISION,
  avgordervalue DOUBLE PRECISION,
  uniquecustomers BIGINT,
  processing_timestamp TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_kpi_1min_ws ON kpi_1min(window_start DESC);

-- OLAP: daily rollups (batch) for Power BI
CREATE TABLE IF NOT EXISTS kpi_daily (
  day DATE,
  category TEXT,
  country TEXT,
  totalorders BIGINT,
  totalquantity BIGINT,
  totalrevenue DOUBLE PRECISION,
  totalnetrevenue DOUBLE PRECISION,
  uniquecustomers BIGINT,
  refreshed_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_kpi_daily_day ON kpi_daily(day DESC);
