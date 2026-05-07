from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            global_id STRING,
            id STRING,
            customer_first_name STRING,
            customer_last_name STRING,
            customer_age INT,
            customer_email STRING,
            customer_country STRING,
            customer_postal_code STRING,
            customer_pet_type STRING,
            customer_pet_name STRING,
            customer_pet_breed STRING,
            seller_first_name STRING,
            seller_last_name STRING,
            seller_email STRING,
            seller_country STRING,
            seller_postal_code STRING,
            product_name STRING,
            product_category STRING,
            product_price DOUBLE,
            product_quantity INT,
            sale_date STRING,
            sale_customer_id INT,
            sale_seller_id INT,
            sale_product_id INT,
            sale_quantity INT,
            sale_total_price DOUBLE,
            store_name STRING,
            store_location STRING,
            store_city STRING,
            store_state STRING,
            store_country STRING,
            store_phone STRING,
            store_email STRING,
            pet_category STRING,
            product_weight DOUBLE,
            product_color STRING,
            product_size STRING,
            product_brand STRING,
            product_material STRING,
            product_description STRING,
            product_rating DOUBLE,
            product_reviews INT,
            product_release_date STRING,
            product_expiry_date STRING,
            supplier_name STRING,
            supplier_contact STRING,
            supplier_email STRING,
            supplier_phone STRING,
            supplier_address STRING,
            supplier_city STRING,
            supplier_country STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales_topic',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink_group_dedup',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    pg_sinkBase = """
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/flinkdb',
        'username' = 'flinkuser',
        'password' = 'flinkpassword'
    """

    t_env.execute_sql(f"""
        CREATE TABLE pg_dim_customer (
            first_name STRING, last_name STRING, age INT, email STRING,
            country STRING, postal_code STRING, pet_type STRING, pet_name STRING, pet_breed STRING,
            PRIMARY KEY(first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed) NOT ENFORCED
        ) WITH ({pg_sinkBase}, 'table-name' = 'dim_customer')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pg_dim_seller (
            first_name STRING, last_name STRING, email STRING, country STRING, postal_code STRING,
            PRIMARY KEY(email) NOT ENFORCED
        ) WITH ({pg_sinkBase}, 'table-name' = 'dim_seller')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pg_dim_supplier (
            name STRING, contact_person STRING, email STRING, phone STRING, address STRING, city STRING, country STRING,
            PRIMARY KEY(name) NOT ENFORCED
        ) WITH ({pg_sinkBase}, 'table-name' = 'dim_supplier')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pg_dim_location (
            store_city STRING, store_state STRING, store_country STRING,
            PRIMARY KEY(store_city, store_state, store_country) NOT ENFORCED
        ) WITH ({pg_sinkBase}, 'table-name' = 'dim_location')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pg_dim_store (
            store_name STRING, store_location STRING, store_phone STRING, store_email STRING,
            PRIMARY KEY(store_name, store_location) NOT ENFORCED
        ) WITH ({pg_sinkBase}, 'table-name' = 'dim_store')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pg_dim_brand (
            brand_name STRING,
            PRIMARY KEY(brand_name) NOT ENFORCED
        ) WITH ({pg_sinkBase}, 'table-name' = 'dim_brand')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pg_dim_product_category (
            category_name STRING,
            PRIMARY KEY(category_name) NOT ENFORCED
        ) WITH ({pg_sinkBase}, 'table-name' = 'dim_product_category')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pg_dim_product (
            product_name STRING, product_price DOUBLE, product_color STRING, product_size STRING, product_weight DOUBLE,
            product_material STRING, product_description STRING,
            product_rating DOUBLE, product_reviews INT, product_release_date STRING, product_expiry_date STRING,
            pet_category STRING, brand_name STRING, category_name STRING,
            PRIMARY KEY(product_name, product_price, product_color, product_size) NOT ENFORCED
        ) WITH ({pg_sinkBase}, 'table-name' = 'dim_product')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE pg_fact_sales (
            global_id STRING, sale_date STRING, quantity INT, total_price DOUBLE,
            customer_first_name STRING, customer_last_name STRING, customer_email STRING,
            seller_email STRING, product_name STRING, product_price DOUBLE,
            store_name STRING, store_location STRING, supplier_name STRING,
            PRIMARY KEY(global_id) NOT ENFORCED
        ) WITH ({pg_sinkBase}, 'table-name' = 'fact_sales')
    """)

    statement_set = t_env.create_statement_set()

    statement_set.add_insert_sql("""
        INSERT INTO pg_dim_customer
        SELECT customer_first_name, customer_last_name, MAX(customer_age), customer_email,
               MAX(customer_country), MAX(customer_postal_code), MAX(customer_pet_type), MAX(customer_pet_name), MAX(customer_pet_breed)
        FROM kafka_source WHERE customer_first_name IS NOT NULL AND customer_email IS NOT NULL
        GROUP BY customer_first_name, customer_last_name, customer_email
    """)

    statement_set.add_insert_sql("""
        INSERT INTO pg_dim_seller
        SELECT seller_first_name, seller_last_name, seller_email, MAX(seller_country), MAX(seller_postal_code)
        FROM kafka_source WHERE seller_email IS NOT NULL
        GROUP BY seller_first_name, seller_last_name, seller_email
    """)

    statement_set.add_insert_sql("""
        INSERT INTO pg_dim_supplier
        SELECT supplier_name, MAX(supplier_contact), MAX(supplier_email), MAX(supplier_phone), MAX(supplier_address), MAX(supplier_city), MAX(supplier_country)
        FROM kafka_source WHERE supplier_name IS NOT NULL AND supplier_name <> '' GROUP BY supplier_name
    """)

    statement_set.add_insert_sql("""
        INSERT INTO pg_dim_location
        SELECT store_city, store_state, store_country
        FROM kafka_source
        WHERE store_city IS NOT NULL AND store_city <> '' AND store_state IS NOT NULL AND store_country IS NOT NULL
        GROUP BY store_city, store_state, store_country
    """)

    statement_set.add_insert_sql("""
        INSERT INTO pg_dim_store
        SELECT store_name, store_location, MAX(store_phone), MAX(store_email)
        FROM kafka_source WHERE store_name IS NOT NULL AND store_location IS NOT NULL GROUP BY store_name, store_location
    """)

    statement_set.add_insert_sql("""
        INSERT INTO pg_dim_brand
        SELECT product_brand FROM kafka_source WHERE product_brand IS NOT NULL AND product_brand <> '' GROUP BY product_brand
    """)

    statement_set.add_insert_sql("""
        INSERT INTO pg_dim_product_category
        SELECT product_category FROM kafka_source WHERE product_category IS NOT NULL AND product_category <> '' GROUP BY product_category
    """)

    statement_set.add_insert_sql("""
        INSERT INTO pg_dim_product
        SELECT product_name, product_price, product_color, product_size, MAX(product_weight), MAX(product_material), MAX(product_description),
               MAX(product_rating), MAX(product_reviews), MAX(product_release_date), MAX(product_expiry_date),
               MAX(pet_category), MAX(product_brand), MAX(product_category)
        FROM kafka_source WHERE product_name IS NOT NULL
        GROUP BY product_name, product_price, product_color, product_size
    """)

    statement_set.add_insert_sql("""
        INSERT INTO pg_fact_sales
        SELECT global_id, sale_date, sale_quantity, sale_total_price,
               customer_first_name, customer_last_name, customer_email,
               seller_email, product_name, product_price,
               store_name, store_location, supplier_name
        FROM kafka_source
    """)

    statement_set.execute()

if __name__ == '__main__':
    main()
