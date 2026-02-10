-- Initialization script for Hive Metastore PostgreSQL
-- Creates the 'hive' user and 'metastore' database
-- The actual Hive schema is initialized by schematool (hive-metastore-init service)

CREATE USER hive WITH PASSWORD 'hive';
CREATE DATABASE metastore OWNER hive;
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
