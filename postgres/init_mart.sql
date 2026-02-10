-- OLTP: aggregated live sales (same shape as OLAP Gold layer)
CREATE TABLE IF NOT EXISTS orders_live (
  day                   DATE,
  category              TEXT,
  country               TEXT,
  paymentmethod         TEXT,
  totalorders           BIGINT,
  totalquantity         BIGINT,
  totalrevenue          DOUBLE PRECISION,
  totalnetrevenue       DOUBLE PRECISION,
  avgordervalue         DOUBLE PRECISION,
  uniquecustomers       BIGINT,
  processing_timestamp  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_live_day ON orders_live(day);
CREATE INDEX IF NOT EXISTS idx_orders_live_cat ON orders_live(category);
CREATE INDEX IF NOT EXISTS idx_orders_live_country ON orders_live(country);
