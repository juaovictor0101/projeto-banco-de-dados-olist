
\set cleaned_data_path '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos'



-- 1. geo_location
\echo 'Carregando geo_location...'
COPY geo_location FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/olist_geolocation_dataset_cleaned.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',') ;

-- 2. product
\echo 'Carregando product...'
COPY product FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/olist_products_dataset_cleaned.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',') ;

-- 3. seller
\echo 'Carregando seller...'
COPY seller FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/olist_sellers_dataset_cleaned.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',') ;


-- 4. customer
\echo 'Carregando customer...'
COPY customer FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/olist_customers_dataset_cleaned.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',') ;

-- 5. "order"
\echo 'Carregando order...'
COPY "order" FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/olist_orders_dataset_cleaned.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',') ;


-- 6. order_item
\echo 'Carregando order_item...'
COPY order_item FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/olist_order_items_dataset_cleaned.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',') ;

-- 7. order_payment
\echo 'Carregando order_payment...'
COPY order_payment FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/olist_order_payments_dataset_cleaned.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',') ;

-- 8. order_review
\echo 'Carregando order_review...'
COPY order_review FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/olist_order_reviews_dataset_cleaned.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',') ;


SELECT
    'geo_location' AS table_name, count(*) AS total_rows FROM geo_location UNION ALL
SELECT 'product', count(*) FROM product UNION ALL
SELECT 'seller', count(*) FROM seller UNION ALL
SELECT 'customer', count(*) FROM customer UNION ALL
SELECT 'order', count(*) FROM "order" UNION ALL
SELECT 'order_item', count(*) FROM order_item UNION ALL
SELECT 'order_payment', count(*) FROM order_payment UNION ALL
SELECT 'order_review', count(*) FROM order_review;