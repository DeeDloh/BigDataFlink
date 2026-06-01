-- Продавцы

INSERT INTO dim_sellers (
    first_name,
    last_name,
    email,
    country,
    postal_code
)
SELECT DISTINCT
    m.seller_first_name,
    m.seller_last_name,
    m.seller_email,
    m.seller_country,
    m.seller_postal_code
FROM mock_data m
WHERE m.seller_email IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_sellers ds
      WHERE ds.email = m.seller_email
  );


-- Локации магазинов

INSERT INTO dim_locations (
    location,
    city,
    state,
    country
)
SELECT DISTINCT
    m.store_location,
    m.store_city,
    m.store_state,
    m.store_country
FROM mock_data m
WHERE (m.store_location IS NOT NULL
     OR m.store_city IS NOT NULL
     OR m.store_state IS NOT NULL
     OR m.store_country IS NOT NULL)
    AND NOT EXISTS (
      SELECT 1
      FROM dim_locations dl
      WHERE dl.location IS NOT DISTINCT FROM m.store_location
        AND dl.city IS NOT DISTINCT FROM m.store_city
        AND dl.state IS NOT DISTINCT FROM m.store_state
        AND dl.country IS NOT DISTINCT FROM m.store_country
  );


-- Магазины

INSERT INTO dim_stores (
    name,
    location_id,
    phone,
    email
)
SELECT DISTINCT
    m.store_name,
    l.location_id,
    m.store_phone,
    m.store_email
FROM mock_data m
JOIN dim_locations l ON
    m.store_location IS NOT DISTINCT FROM l.location
    AND m.store_city IS NOT DISTINCT FROM l.city
    AND m.store_state IS NOT DISTINCT FROM l.state
    AND m.store_country IS NOT DISTINCT FROM l.country
WHERE m.store_name IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_stores ds
      WHERE ds.name IS NOT DISTINCT FROM m.store_name
        AND ds.location_id = l.location_id
        AND ds.phone IS NOT DISTINCT FROM m.store_phone
        AND ds.email IS NOT DISTINCT FROM m.store_email
  );


-- Поставщики

INSERT INTO dim_suppliers (
    name,
    contact,
    email,
    phone,
    address,
    city,
    country
)
SELECT DISTINCT
    m.supplier_name,
    m.supplier_contact,
    m.supplier_email,
    m.supplier_phone,
    m.supplier_address,
    m.supplier_city,
    m.supplier_country
FROM mock_data m
WHERE m.supplier_name IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_suppliers ds
      WHERE ds.name IS NOT DISTINCT FROM m.supplier_name
        AND ds.email IS NOT DISTINCT FROM m.supplier_email
        AND ds.phone IS NOT DISTINCT FROM m.supplier_phone
  );


-- Даты продаж

INSERT INTO dim_dates (
    date_id,
    day,
    month,
    year,
    quarter,
    day_of_week,
    is_weekend
)
SELECT DISTINCT
    TO_DATE(m.sale_date, 'MM/DD/YYYY') AS date_id,
    EXTRACT(DAY FROM TO_DATE(m.sale_date, 'MM/DD/YYYY'))::INT AS day,
    EXTRACT(MONTH FROM TO_DATE(m.sale_date, 'MM/DD/YYYY'))::INT AS month,
    EXTRACT(YEAR FROM TO_DATE(m.sale_date, 'MM/DD/YYYY'))::INT AS year,
    EXTRACT(QUARTER FROM TO_DATE(m.sale_date, 'MM/DD/YYYY'))::INT AS quarter,
    EXTRACT(DOW FROM TO_DATE(m.sale_date, 'MM/DD/YYYY'))::INT + 1 AS day_of_week,
    EXTRACT(DOW FROM TO_DATE(m.sale_date, 'MM/DD/YYYY'))::INT IN (0, 6) AS is_weekend
FROM mock_data m
WHERE m.sale_date IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_dates dd
      WHERE dd.date_id = TO_DATE(m.sale_date, 'MM/DD/YYYY')
  );


-- Домашние животные

INSERT INTO dim_pets (
    pet_type,
    pet_name,
    pet_breed
)
SELECT DISTINCT
    m.customer_pet_type,
    m.customer_pet_name,
    m.customer_pet_breed
FROM mock_data m
WHERE (m.customer_pet_type IS NOT NULL
     OR m.customer_pet_name IS NOT NULL
     OR m.customer_pet_breed IS NOT NULL)
    AND NOT EXISTS (
      SELECT 1
      FROM dim_pets dp
      WHERE dp.pet_type IS NOT DISTINCT FROM m.customer_pet_type
        AND dp.pet_name IS NOT DISTINCT FROM m.customer_pet_name
        AND dp.pet_breed IS NOT DISTINCT FROM m.customer_pet_breed
  );


-- Покупатели

INSERT INTO dim_customers (
    first_name,
    last_name,
    age,
    email,
    country,
    postal_code,
    pet_id
)
SELECT DISTINCT
    m.customer_first_name,
    m.customer_last_name,
    m.customer_age,
    m.customer_email,
    m.customer_country,
    m.customer_postal_code,
    p.pet_id
FROM mock_data m
LEFT JOIN dim_pets p ON
    m.customer_pet_type IS NOT DISTINCT FROM p.pet_type
    AND m.customer_pet_name IS NOT DISTINCT FROM p.pet_name
    AND m.customer_pet_breed IS NOT DISTINCT FROM p.pet_breed
WHERE m.customer_email IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_customers dc
      WHERE dc.email = m.customer_email
  );


-- Категории товаров

INSERT INTO dim_categories (
    category_name
)
SELECT DISTINCT
    m.product_category
FROM mock_data m
WHERE m.product_category IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_categories dc
      WHERE dc.category_name = m.product_category
  );


-- Бренды

INSERT INTO dim_brands (
    brand_name
)
SELECT DISTINCT
    m.product_brand
FROM mock_data m
WHERE m.product_brand IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_brands db
      WHERE db.brand_name = m.product_brand
  );


-- Материалы

INSERT INTO dim_materials (
    material_name
)
SELECT DISTINCT
    m.product_material
FROM mock_data m
WHERE m.product_material IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_materials dm
      WHERE dm.material_name = m.product_material
  );



-- Цвета

INSERT INTO dim_colors (
    color_name
)
SELECT DISTINCT
    m.product_color
FROM mock_data m
WHERE m.product_color IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_colors dc
      WHERE dc.color_name = m.product_color
  );



-- Размеры

INSERT INTO dim_sizes (
    size_name
)
SELECT DISTINCT
    m.product_size
FROM mock_data m
WHERE m.product_size IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_sizes ds
      WHERE ds.size_name = m.product_size
  );



-- Продукты

INSERT INTO dim_products (
    name,
    category_id,
    price,
    weight,
    color_id,
    size_id,
    brand_id,
    material_id,
    description,
    rating,
    reviews,
    release_date,
    expiry_date,
    pet_category
)
SELECT DISTINCT
    m.product_name,
    c.category_id,
    m.product_price,
    m.product_weight,
    co.color_id,
    sz.size_id,
    b.brand_id,
    mat.material_id,
    m.product_description,
    m.product_rating,
    m.product_reviews,
    TO_DATE(m.product_release_date, 'MM/DD/YYYY'),
    TO_DATE(m.product_expiry_date, 'MM/DD/YYYY'),
    m.pet_category
FROM mock_data m
LEFT JOIN dim_categories c ON
    m.product_category IS NOT DISTINCT FROM c.category_name
LEFT JOIN dim_colors co ON
    m.product_color IS NOT DISTINCT FROM co.color_name
LEFT JOIN dim_sizes sz ON
    m.product_size IS NOT DISTINCT FROM sz.size_name
LEFT JOIN dim_brands b ON
    m.product_brand IS NOT DISTINCT FROM b.brand_name
LEFT JOIN dim_materials mat ON
    m.product_material IS NOT DISTINCT FROM mat.material_name
WHERE m.product_name IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dim_products dp
      WHERE dp.name IS NOT DISTINCT FROM m.product_name
        AND dp.category_id IS NOT DISTINCT FROM c.category_id
        AND dp.price IS NOT DISTINCT FROM m.product_price
        AND dp.weight IS NOT DISTINCT FROM m.product_weight
        AND dp.color_id IS NOT DISTINCT FROM co.color_id
        AND dp.size_id IS NOT DISTINCT FROM sz.size_id
        AND dp.brand_id IS NOT DISTINCT FROM b.brand_id
        AND dp.material_id IS NOT DISTINCT FROM mat.material_id
        AND dp.description IS NOT DISTINCT FROM m.product_description
        AND dp.rating IS NOT DISTINCT FROM m.product_rating
        AND dp.reviews IS NOT DISTINCT FROM m.product_reviews
        AND dp.release_date IS NOT DISTINCT FROM TO_DATE(m.product_release_date, 'MM/DD/YYYY')
        AND dp.expiry_date IS NOT DISTINCT FROM TO_DATE(m.product_expiry_date, 'MM/DD/YYYY')
        AND dp.pet_category IS NOT DISTINCT FROM m.pet_category
  );



-- Факты продаж

INSERT INTO fact_sales (
    date_id,
    customer_id,
    seller_id,
    product_id,
    store_id,
    supplier_id,
    quantity,
    total_price
)
SELECT
    TO_DATE(m.sale_date, 'MM/DD/YYYY') AS date_id,
    c.customer_id,
    s.seller_id,
    p.product_id,
    st.store_id,
    sp.supplier_id,
    m.sale_quantity,
    m.sale_total_price
FROM mock_data m

JOIN dim_dates d ON
    d.date_id = TO_DATE(m.sale_date, 'MM/DD/YYYY')

JOIN dim_customers c ON
    m.customer_email IS NOT DISTINCT FROM c.email

JOIN dim_sellers s ON
    m.seller_email IS NOT DISTINCT FROM s.email

JOIN dim_locations l ON
    m.store_location IS NOT DISTINCT FROM l.location
    AND m.store_city IS NOT DISTINCT FROM l.city
    AND m.store_state IS NOT DISTINCT FROM l.state
    AND m.store_country IS NOT DISTINCT FROM l.country

JOIN dim_stores st ON
    m.store_name IS NOT DISTINCT FROM st.name
    AND st.location_id = l.location_id
    AND m.store_phone IS NOT DISTINCT FROM st.phone
    AND m.store_email IS NOT DISTINCT FROM st.email

JOIN dim_suppliers sp ON
    m.supplier_name IS NOT DISTINCT FROM sp.name
    AND m.supplier_email IS NOT DISTINCT FROM sp.email
    AND m.supplier_phone IS NOT DISTINCT FROM sp.phone

LEFT JOIN dim_categories cat ON
    m.product_category IS NOT DISTINCT FROM cat.category_name

LEFT JOIN dim_colors col ON
    m.product_color IS NOT DISTINCT FROM col.color_name

LEFT JOIN dim_sizes size_dim ON
    m.product_size IS NOT DISTINCT FROM size_dim.size_name

LEFT JOIN dim_brands brand ON
    m.product_brand IS NOT DISTINCT FROM brand.brand_name

LEFT JOIN dim_materials material ON
    m.product_material IS NOT DISTINCT FROM material.material_name

JOIN dim_products p ON
    m.product_name IS NOT DISTINCT FROM p.name
    AND cat.category_id IS NOT DISTINCT FROM p.category_id
    AND m.product_price IS NOT DISTINCT FROM p.price
    AND m.product_weight IS NOT DISTINCT FROM p.weight
    AND col.color_id IS NOT DISTINCT FROM p.color_id
    AND size_dim.size_id IS NOT DISTINCT FROM p.size_id
    AND brand.brand_id IS NOT DISTINCT FROM p.brand_id
    AND material.material_id IS NOT DISTINCT FROM p.material_id
    AND m.product_description IS NOT DISTINCT FROM p.description
    AND m.product_rating IS NOT DISTINCT FROM p.rating
    AND m.product_reviews IS NOT DISTINCT FROM p.reviews
    AND TO_DATE(m.product_release_date, 'MM/DD/YYYY') IS NOT DISTINCT FROM p.release_date
    AND TO_DATE(m.product_expiry_date, 'MM/DD/YYYY') IS NOT DISTINCT FROM p.expiry_date
    AND m.pet_category IS NOT DISTINCT FROM p.pet_category;
