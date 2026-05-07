CREATE TABLE IF NOT EXISTS dim_customer (
    first_name VARCHAR,
    last_name VARCHAR,
    age INT,
    email VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    pet_type VARCHAR,
    pet_name VARCHAR,
    pet_breed VARCHAR,
    PRIMARY KEY(first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed)
);

CREATE TABLE IF NOT EXISTS dim_seller (
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    country VARCHAR,
    postal_code VARCHAR,
    PRIMARY KEY(email)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    name VARCHAR PRIMARY KEY,
    contact_person VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    country VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_location (
    store_city VARCHAR,
    store_state VARCHAR,
    store_country VARCHAR,
    PRIMARY KEY(store_city, store_state, store_country)
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_name VARCHAR,
    store_location VARCHAR,
    store_phone VARCHAR,
    store_email VARCHAR,
    PRIMARY KEY(store_name, store_location)
);

CREATE TABLE IF NOT EXISTS dim_brand (
    brand_name VARCHAR PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_product_category (
    category_name VARCHAR PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_name VARCHAR,
    product_price DOUBLE PRECISION,
    product_color VARCHAR,
    product_size VARCHAR,
    product_weight DOUBLE PRECISION,
    product_material VARCHAR,
    product_description TEXT,
    product_rating DOUBLE PRECISION,
    product_reviews INT,
    product_release_date VARCHAR,
    product_expiry_date VARCHAR,
    pet_category VARCHAR,
    brand_name VARCHAR,
    category_name VARCHAR,
    PRIMARY KEY(product_name, product_price, product_color, product_size)
);

CREATE TABLE IF NOT EXISTS fact_sales (
    global_id VARCHAR PRIMARY KEY,
    sale_date VARCHAR,
    quantity INT,
    total_price DOUBLE PRECISION,
    customer_first_name VARCHAR,
    customer_last_name VARCHAR,
    customer_email VARCHAR,
    seller_email VARCHAR,
    product_name VARCHAR,
    product_price DOUBLE PRECISION,
    store_name VARCHAR,
    store_location VARCHAR,
    supplier_name VARCHAR
);