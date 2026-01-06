-- PROJETO OLIST - SCRIPT COMPLETO

-- 0. LIMPEZA INICIAL (Reset do Banco para rodar limpo)
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;

-- 1. DDL: CRIAÇÃO DAS TABELAS

-- Tabela: geo_location (Sem PK devido a duplicatas no dataset)
CREATE TABLE geo_location (
    geolocation_zip_code_prefix VARCHAR(5) NOT NULL,
    geolocation_lat DECIMAL(9, 6),
    geolocation_lng DECIMAL(9, 6),
    geolocation_city VARCHAR(90),
    geolocation_state VARCHAR(2)
);

-- Tabela: product
CREATE TABLE product (
    product_id VARCHAR(32) NOT NULL,
    product_category_name VARCHAR(50),
    product_name_lenght FLOAT, 
    product_description_lenght FLOAT,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    PRIMARY KEY (product_id)
);

-- Tabela: seller
CREATE TABLE seller (
    seller_id VARCHAR(32) NOT NULL,
    seller_zip_code_prefix VARCHAR(5),
    seller_city VARCHAR(50),
    seller_state VARCHAR(2),
    PRIMARY KEY (seller_id)
);

-- Tabela: customer
CREATE TABLE customer (
    customer_id VARCHAR(32) NOT NULL,
    customer_unique_id VARCHAR(32),
    customer_zip_code_prefix VARCHAR(5),
    customer_city VARCHAR(32),
    customer_state VARCHAR(2),
    PRIMARY KEY (customer_id)
);

-- Tabela: order
CREATE TABLE "order" (
    order_id VARCHAR(32) NOT NULL,
    customer_id VARCHAR(32),
    order_status VARCHAR(25),
    order_purchase_timestamp TIMESTAMP WITHOUT TIME ZONE,
    order_approved_at TIMESTAMP WITHOUT TIME ZONE,
    order_delivered_carrier_date TIMESTAMP WITHOUT TIME ZONE,
    order_delivered_customer_date TIMESTAMP WITHOUT TIME ZONE,
    order_estimated_delivery_date TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (order_id)
);

-- Tabela: order_item
CREATE TABLE order_item (
    order_id VARCHAR(32) NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id VARCHAR(32),
    seller_id VARCHAR(32),
    shipping_limit_date TIMESTAMP WITHOUT TIME ZONE,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, order_item_id)
);

-- Tabela: order_payment
CREATE TABLE order_payment (
    order_id VARCHAR(32) NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type VARCHAR(20),
    payment_installments INTEGER,
    payment_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, payment_sequential)
);

-- Tabela: order_review
CREATE TABLE order_review (
    review_id VARCHAR(32) NOT NULL,
    order_id VARCHAR(32),
    review_score INTEGER,
    review_comment_title VARCHAR(150),
    review_comment_message TEXT,
    review_creation_date TIMESTAMP WITHOUT TIME ZONE,
    review_answer_timestamp TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (review_id)
);

-- 2. CARGA DE DADOS (ETL)

\echo 'Carregando geo_location...'
\copy geo_location FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/geolocation.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '');

\echo 'Carregando product...'
\copy product FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/product.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '');

\echo 'Carregando seller...'
\copy seller FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/seller.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '');

\echo 'Carregando customer...'
\copy customer FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/customer.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '');

\echo 'Carregando order...'
\copy "order" FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/order.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '');

\echo 'Carregando order_item...'
\copy order_item FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/order_item.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '');

\echo 'Carregando order_payment...'
\copy order_payment FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/order_payment.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '');

\echo 'Carregando order_review...'
\copy order_review FROM '/home/jaum/Downloads/projeto-olist-postgres/dados_limpos/order_review.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '');


-- 3. AJUSTES PÓS-CARGA

-- Correção de Tipagem: Converter Float para Integer na tabela Product
ALTER TABLE product ALTER COLUMN product_name_lenght TYPE INTEGER USING product_name_lenght::INTEGER;
ALTER TABLE product ALTER COLUMN product_description_lenght TYPE INTEGER USING product_description_lenght::INTEGER;

-- 4. RESTRIÇÕES DE INTEGRIDADE (CONSTRAINTS)

-- 4.1 Checagens de Valores (CHECK)
ALTER TABLE order_review ADD CONSTRAINT chk_review_score CHECK (review_score BETWEEN 1 AND 5);
ALTER TABLE order_item ADD CONSTRAINT chk_item_price_positive CHECK (price >= 0 AND freight_value >= 0);
ALTER TABLE order_payment ADD CONSTRAINT chk_payment_value_positive CHECK (payment_value >= 0);
ALTER TABLE "order" ADD CONSTRAINT chk_order_status CHECK (order_status IN ('delivered', 'shipped', 'canceled', 'invoiced', 'processing', 'unavailable', 'created', 'approved'));

-- 4.2 Chaves Estrangeiras (FK)
-- RESTRICT para tabelas mestres (histórico seguro) e CASCADE para transacionais.

ALTER TABLE "order" ADD CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE order_review ADD CONSTRAINT fk_order_review_order FOREIGN KEY (order_id) REFERENCES "order" (order_id) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE order_payment ADD CONSTRAINT fk_order_payment_order FOREIGN KEY (order_id) REFERENCES "order" (order_id) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE order_item ADD CONSTRAINT fk_order_item_order FOREIGN KEY (order_id) REFERENCES "order" (order_id) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE order_item ADD CONSTRAINT fk_order_item_product FOREIGN KEY (product_id) REFERENCES product (product_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE order_item ADD CONSTRAINT fk_order_item_seller FOREIGN KEY (seller_id) REFERENCES seller (seller_id) ON UPDATE CASCADE ON DELETE RESTRICT;

-- 5. ÍNDICES DE PERFORMANCE

CREATE INDEX idx_customer_unique_id ON customer(customer_unique_id);
CREATE INDEX idx_order_purchase_timestamp ON "order"(order_purchase_timestamp);
CREATE INDEX idx_product_category_name ON product(product_category_name);
CREATE INDEX idx_customer_state ON customer(customer_state);
-- Índice composto criado na tentativa de otimização 
CREATE INDEX idx_order_status_date ON "order" (order_status, order_purchase_timestamp);

-- 6. SEGURANÇA (USUÁRIO BI)

DROP USER IF EXISTS bi_analyst;
CREATE USER bi_analyst WITH PASSWORD 'bi_123456';
GRANT CONNECT ON DATABASE apdb_database TO bi_analyst;
GRANT USAGE ON SCHEMA public TO bi_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bi_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bi_analyst;

-- 7. FUNÇÃO (Geolocalização)

CREATE OR REPLACE FUNCTION calcular_distancia_km(lat1 float, lon1 float, lat2 float, lon2 float)
RETURNS float AS $func$
DECLARE
    R integer := 6371; 
    dlat float;
    dlon float;
    a float;
    c float;
BEGIN
    IF lat1 IS NULL OR lon1 IS NULL OR lat2 IS NULL OR lon2 IS NULL THEN RETURN NULL; END IF;
    dlat := radians(lat2 - lat1);
    dlon := radians(lon2 - lon1);
    lat1 := radians(lat1);
    lat2 := radians(lat2);
    a := sin(dlat/2)^2 + cos(lat1) * cos(lat2) * sin(dlon/2)^2;
    c := 2 * asin(sqrt(a));
    RETURN ROUND((R * c)::numeric, 2);
END
$func$ LANGUAGE plpgsql;

-- 8. OTIMIZAÇÃO (Visões Materializadas)

CREATE MATERIALIZED VIEW mv_analytics_vendas_categoria AS
SELECT
    p.product_category_name AS categoria,
    COUNT(oi.order_item_id) AS total_unidades_vendidas,
    SUM(oi.price) AS receita_total
FROM
    order_item oi
JOIN
    "order" o ON oi.order_id = o.order_id
JOIN
    product p ON oi.product_id = p.product_id
WHERE
    o.order_status = 'delivered'
GROUP BY
    p.product_category_name;

CREATE INDEX idx_mv_vendas_qtd ON mv_analytics_vendas_categoria(total_unidades_vendidas DESC);

-- 9. AUDITORIA (Logs e Triggers)

CREATE TABLE audit_log (
    log_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    operation_type VARCHAR(10) NOT NULL,
    changed_by TEXT DEFAULT current_user,
    changed_at TIMESTAMP DEFAULT NOW(),
    old_data JSONB,
    new_data JSONB
);

CREATE INDEX idx_audit_table ON audit_log(table_name);
CREATE INDEX idx_audit_date ON audit_log(changed_at);

CREATE OR REPLACE FUNCTION fn_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO audit_log (table_name, operation_type, new_data) VALUES (TG_TABLE_NAME, 'INSERT', row_to_json(NEW));
        RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        IF NEW IS DISTINCT FROM OLD THEN
            INSERT INTO audit_log (table_name, operation_type, old_data, new_data) VALUES (TG_TABLE_NAME, 'UPDATE', row_to_json(OLD), row_to_json(NEW));
        END IF;
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO audit_log (table_name, operation_type, old_data) VALUES (TG_TABLE_NAME, 'DELETE', row_to_json(OLD));
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_product AFTER INSERT OR UPDATE OR DELETE ON product FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_payment AFTER INSERT OR UPDATE OR DELETE ON order_payment FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
CREATE TRIGGER trg_audit_order AFTER INSERT OR UPDATE OR DELETE ON "order" FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();
