-- ============================================================
-- DDL: создание таблиц измерений и фактов
-- Запускать после ddl_sc.sql, copy_data.sql, change_id.sql
-- и перед dml.sql
-- ============================================================

-- На случай повторного запуска удаляем таблицы в правильном порядке
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_stores CASCADE;
DROP TABLE IF EXISTS dim_suppliers CASCADE;
DROP TABLE IF EXISTS dim_sellers CASCADE;
DROP TABLE IF EXISTS dim_pets CASCADE;
DROP TABLE IF EXISTS dim_dates CASCADE;
DROP TABLE IF EXISTS dim_categories CASCADE;
DROP TABLE IF EXISTS dim_brands CASCADE;
DROP TABLE IF EXISTS dim_materials CASCADE;
DROP TABLE IF EXISTS dim_colors CASCADE;
DROP TABLE IF EXISTS dim_sizes CASCADE;
DROP TABLE IF EXISTS dim_locations CASCADE;

-- ============================================================
-- Измерения
-- ============================================================

CREATE TABLE dim_sellers (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(50)
);

CREATE TABLE dim_locations (
    location_id SERIAL PRIMARY KEY,
    location VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE dim_stores (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    location_id INT REFERENCES dim_locations(location_id),
    phone VARCHAR(50),
    email VARCHAR(50)
);

CREATE TABLE dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    contact VARCHAR(50),
    email VARCHAR(50),
    phone VARCHAR(50),
    address VARCHAR(50),
    city VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE dim_dates (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

CREATE TABLE dim_pets (
    pet_id SERIAL PRIMARY KEY,
    pet_type VARCHAR(50),
    pet_name VARCHAR(50),
    pet_breed VARCHAR(50)
);

CREATE TABLE dim_customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    email VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(50),
    pet_id INT REFERENCES dim_pets(pet_id)
);

CREATE TABLE dim_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(50)
);

CREATE TABLE dim_brands (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(50)
);

CREATE TABLE dim_materials (
    material_id SERIAL PRIMARY KEY,
    material_name VARCHAR(50)
);

CREATE TABLE dim_colors (
    color_id SERIAL PRIMARY KEY,
    color_name VARCHAR(50)
);

CREATE TABLE dim_sizes (
    size_id SERIAL PRIMARY KEY,
    size_name VARCHAR(50)
);

CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    category_id INT REFERENCES dim_categories(category_id),
    price FLOAT4,
    weight FLOAT4,
    color_id INT REFERENCES dim_colors(color_id),
    size_id INT REFERENCES dim_sizes(size_id),
    brand_id INT REFERENCES dim_brands(brand_id),
    material_id INT REFERENCES dim_materials(material_id),
    description VARCHAR(1024),
    rating FLOAT4,
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    pet_category VARCHAR(50)
);

-- ============================================================
-- Таблица фактов
-- ============================================================

CREATE TABLE fact_sales (
    fact_id SERIAL PRIMARY KEY,
    date_id DATE REFERENCES dim_dates(date_id),
    customer_id INT REFERENCES dim_customers(customer_id),
    seller_id INT REFERENCES dim_sellers(seller_id),
    product_id INT REFERENCES dim_products(product_id),
    store_id INT REFERENCES dim_stores(store_id),
    supplier_id INT REFERENCES dim_suppliers(supplier_id),
    quantity INT,
    total_price FLOAT4
);
