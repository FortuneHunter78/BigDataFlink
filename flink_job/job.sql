CREATE TABLE kafka_source (
    `id` STRING,
    `customer_first_name` STRING,
    `customer_last_name` STRING,
    `customer_age` INT,
    `customer_email` STRING,
    `customer_country` STRING,
    `product_name` STRING,
    `product_category` STRING,
    `product_price` DECIMAL(10,2),
    `product_brand` STRING,
    `store_name` STRING,
    `store_city` STRING,
    `store_country` STRING,
    `sale_quantity` INT,
    `sale_total_price` DECIMAL(10,2),
    `sale_date` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales_topic',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_group',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE pg_dim_customer (
    `customer_first_name` STRING,
    `customer_last_name` STRING,
    `customer_age` INT,
    `customer_email` STRING,
    `customer_country` STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/flinkdb',
    'table-name' = 'dim_customer',
    'username' = 'flinkuser',
    'password' = 'flinkpassword'
);

INSERT INTO pg_dim_customer
SELECT DISTINCT
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country
FROM kafka_source;
