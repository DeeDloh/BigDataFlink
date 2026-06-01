-- Быстрая проверка результата
SELECT 'dim_customers' AS table_name, COUNT(*) FROM dim_customers
UNION ALL SELECT 'dim_sellers', COUNT(*) FROM dim_sellers
UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_stores', COUNT(*) FROM dim_stores
UNION ALL SELECT 'dim_suppliers', COUNT(*) FROM dim_suppliers
UNION ALL SELECT 'dim_dates', COUNT(*) FROM dim_dates
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;

-- Проверка связей фактов с измерениями
SELECT COUNT(*) AS facts_without_customer
FROM fact_sales f
LEFT JOIN dim_customers c ON c.customer_id = f.customer_id
WHERE c.customer_id IS NULL;

-- Продажи по категориям
SELECT c.category_name, SUM(f.total_price) AS total_sales
FROM fact_sales f
JOIN dim_products p ON p.product_id = f.product_id
JOIN dim_categories c ON c.category_id = p.category_id
GROUP BY c.category_name
ORDER BY total_sales DESC;
