CREATE DATABASE IF NOT EXISTS datacoolie_test;
USE datacoolie_test;

CREATE TABLE IF NOT EXISTS customers (
    customer_id  INT,
    first_name   STRING,
    last_name    STRING,
    email        STRING,
    country      STRING
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS orders (
    order_id    INT,
    customer_id INT,
    amount      DECIMAL(10,2),
    status      STRING,
    order_date  DATE
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS products (
    product_id  INT,
    name        STRING,
    category    STRING,
    price       DECIMAL(10,2)
) STORED AS PARQUET;
