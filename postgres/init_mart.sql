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
