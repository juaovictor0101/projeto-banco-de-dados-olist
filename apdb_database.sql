-- 1. Tabelas de Dimensão e Referência (Sem dependências FK externas)

DROP TABLE IF EXISTS geo_location CASCADE;
CREATE TABLE geo_location (
    geolocation_zip_code_prefix VARCHAR(5) NOT NULL,
    geolocation_lat DECIMAL(9, 6),
    geolocation_lng DECIMAL(9, 6),
    geolocation_city VARCHAR(90),
    geolocation_state VARCHAR(2)
);

DROP TABLE IF EXISTS product CASCADE;
CREATE TABLE product (
    product_id VARCHAR(32) NOT NULL,
    product_category_name VARCHAR(50),
    product_name_lenght INTEGER, 
    product_description_lenght INTEGER, 
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    PRIMARY KEY (product_id)
);

DROP TABLE IF EXISTS seller CASCADE;
CREATE TABLE seller (
    seller_id VARCHAR(32) NOT NULL,
    seller_zip_code_prefix VARCHAR(5),
    seller_city VARCHAR(50),
    seller_state VARCHAR(2),
    PRIMARY KEY (seller_id)
);

-- 2. Tabelas Principais

DROP TABLE IF EXISTS customer CASCADE;
CREATE TABLE customer (
    customer_id VARCHAR(32) NOT NULL,
    customer_unique_id VARCHAR(32),
    customer_zip_code_prefix VARCHAR(5),
    customer_city VARCHAR(32),
    customer_state VARCHAR(2),
    PRIMARY KEY (customer_id),
    UNIQUE (customer_unique_id)
);

DROP TABLE IF EXISTS "order" CASCADE;
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

-- 3. Tabelas de Ligação/Fato

DROP TABLE IF EXISTS order_item CASCADE;
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

DROP TABLE IF EXISTS order_payment CASCADE;
CREATE TABLE order_payment (
    order_id VARCHAR(32) NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type VARCHAR(20),
    payment_installments INTEGER,
    payment_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, payment_sequential)
);

DROP TABLE IF EXISTS order_review CASCADE;
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

-- 1. RESTRIÇÕES DE VALOR (CHECK)

ALTER TABLE order_review
    ADD CONSTRAINT chk_review_score CHECK (review_score BETWEEN 1 AND 5);

ALTER TABLE order_item
    ADD CONSTRAINT chk_item_price_positive CHECK (price >= 0 AND freight_value >= 0);

ALTER TABLE order_payment
    ADD CONSTRAINT chk_payment_value_positive CHECK (payment_value >= 0);

ALTER TABLE "order"
    ADD CONSTRAINT chk_order_status CHECK (order_status IN (
        'delivered', 'shipped', 'canceled', 'invoiced',
        'processing', 'unavailable', 'created', 'approved'
));

-- 2. Chaves Estrangeiras (FKs)

-- Tabela "order" -> customer
ALTER TABLE "order"
    ADD CONSTRAINT fk_order_customer FOREIGN KEY (customer_id)
        REFERENCES customer (customer_id) ON UPDATE CASCADE ON DELETE RESTRICT;

-- Tabela order_review -> "order"
ALTER TABLE order_review
    ADD CONSTRAINT fk_order_review_order FOREIGN KEY (order_id)
        REFERENCES "order" (order_id) ON UPDATE CASCADE ON DELETE CASCADE;

-- Tabela order_payment -> "order"
ALTER TABLE order_payment
    ADD CONSTRAINT fk_order_payment_order FOREIGN KEY (order_id)
        REFERENCES "order" (order_id) ON UPDATE CASCADE ON DELETE CASCADE;

-- Tabela order_item -> "order"
ALTER TABLE order_item
    ADD CONSTRAINT fk_order_item_order FOREIGN KEY (order_id)
        REFERENCES "order" (order_id) ON UPDATE CASCADE ON DELETE CASCADE;

-- Tabela order_item -> product
ALTER TABLE order_item
    ADD CONSTRAINT fk_order_item_product FOREIGN KEY (product_id)
        REFERENCES product (product_id) ON UPDATE CASCADE ON DELETE RESTRICT;

-- Tabela order_item -> seller
ALTER TABLE order_item
    ADD CONSTRAINT fk_order_item_seller FOREIGN KEY (seller_id)
        REFERENCES seller (seller_id) ON UPDATE CASCADE ON DELETE RESTRICT;

-- 3. Índices de performance (Substituindo UNIQUE e otimizando consultas)

CREATE INDEX idx_customer_unique_id ON customer(customer_unique_id);

CREATE INDEX idx_order_purchase_timestamp ON "order"(order_purchase_timestamp);

CREATE INDEX idx_product_category_name ON product(product_category_name);

CREATE INDEX idx_customer_state ON customer(customer_state);


DROP USER IF EXISTS bi_analyst;
CREATE USER bi_analyst WITH PASSWORD 'bi_123456';

GRANT CONNECT ON DATABASE apdb_database TO bi_analyst;

-- 3. Conceder permissão de USO do esquema 'public'

GRANT USAGE ON SCHEMA public TO bi_analyst;

-- 4. Conceder SELECT em TODAS as tabelas existentes
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bi_analyst;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bi_analyst;


--TESTE DE SEGURANÇA

-- 1. Trocar a identidade para o usuário de BI
SET ROLE bi_analyst;

-- 2. Teste de LEITURA (Deve Funcionar)
SELECT count(*) AS total_pedidos_visualizacao FROM "order";

-- 3. Teste de ESCRITA/DELEÇÃO (Deve falhar com "permission denied")
-- Tentativa de apagar um pedido. O banco deve bloquear.
DELETE FROM "order" WHERE order_id = 'algum_id_inexistente';

RESET ROLE;

-- Total de Vendas por Vendedor
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(oi.order_id) AS qtd_itens_vendidos,
    SUM(oi.price) AS total_faturado
FROM
    seller s
JOIN
    order_item oi ON s.seller_id = oi.seller_id
JOIN
    "order" o ON oi.order_id = o.order_id
WHERE
    o.order_status = 'delivered' --apenas vendas concretizadas
GROUP BY
    s.seller_id,
    s.seller_city,
    s.seller_state
ORDER BY
    total_faturado DESC;


--Top 10 Clientes (Valor Gasto)
SELECT
    c.customer_unique_id,
    COUNT(DISTINCT o.order_id) AS total_pedidos,
    SUM(op.payment_value) AS valor_total_gasto
FROM
    customer c
JOIN
    "order" o ON c.customer_id = o.customer_id
JOIN
    order_payment op ON o.order_id = op.order_id
WHERE
    o.order_status = 'delivered'
    AND o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31' 
GROUP BY
    c.customer_unique_id
ORDER BY
    valor_total_gasto DESC
LIMIT 10;

--Média de Avaliações por Seller
SELECT
    s.seller_id,
    s.seller_city,
    COUNT(r.review_id) AS total_avaliacoes,
    ROUND(AVG(r.review_score), 2) AS media_nota
FROM
    seller s
JOIN
    order_item oi ON s.seller_id = oi.seller_id
JOIN
    order_review r ON oi.order_id = r.order_id
GROUP BY
    s.seller_id,
    s.seller_city
HAVING
    COUNT(r.review_id) > 10 -- Mostra apenas vendedores com mais de 10 avaliações para evitar distorções
ORDER BY
    media_nota DESC,
    total_avaliacoes DESC;

-- Relatório de Pedidos por Período
SELECT
    o.order_id,
    TO_CHAR(o.order_purchase_timestamp, 'DD/MM/YYYY HH24:MI') AS data_compra, -- Formata data para ptBR ler
    o.order_status,
    c.customer_unique_id AS id_cliente,
    c.customer_state AS estado_cliente,
    COALESCE(SUM(op.payment_value), 0) AS total_pago -- Soma pagamentos e trata nulos como 0
FROM
    "order" o
JOIN
    customer c ON o.customer_id = c.customer_id
LEFT JOIN
    order_payment op ON o.order_id = op.order_id
WHERE
    o.order_purchase_timestamp BETWEEN '2017-11-01 00:00:00' AND '2017-11-30 23:59:59'
GROUP BY
    o.order_id,
    o.order_purchase_timestamp,
    o.order_status,
    c.customer_unique_id,
    c.customer_state
ORDER BY
    o.order_purchase_timestamp DESC;

--Top 5 Produtos Mais Vendidos (Volume)
SELECT
    p.product_category_name AS categoria,
    p.product_id,
    COUNT(oi.order_item_id) AS total_unidades_vendidas,
    SUM(oi.price) AS receita_total_gerada
FROM
    order_item oi
JOIN
    "order" o ON oi.order_id = o.order_id
JOIN
    product p ON oi.product_id = p.product_id
WHERE
    o.order_status = 'delivered'
    AND o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY
    p.product_category_name,
    p.product_id
ORDER BY
    total_unidades_vendidas DESC
LIMIT 5;

--Top 10 Maiores Atrasos
SELECT
    o.order_id,
    c.customer_state AS estado_destino,
    TO_CHAR(o.order_estimated_delivery_date, 'DD/MM/YYYY') AS data_prometida,
    TO_CHAR(o.order_delivered_customer_date, 'DD/MM/YYYY') AS data_real_entrega,
    (o.order_delivered_customer_date - o.order_estimated_delivery_date) AS tempo_atraso
FROM
    "order" o
JOIN
    customer c ON o.customer_id = c.customer_id
WHERE
    o.order_status = 'delivered'
    AND o.order_delivered_customer_date > o.order_estimated_delivery_date -- Garante que é atraso
    AND o.order_purchase_timestamp BETWEEN '2016-01-01' AND '2018-12-31' -- Período amplo para pegar os piores casos históricos em anos
ORDER BY
    tempo_atraso DESC
LIMIT 10;

--5.7 Clientes com Maior Valor (LTV Global)
SELECT
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_pedidos,
    SUM(op.payment_value) AS valor_total_gasto
FROM
    customer c
JOIN
    "order" o ON c.customer_id = o.customer_id
JOIN
    order_payment op ON o.order_id = op.order_id
WHERE
    o.order_status = 'delivered' -- apenas vendas efetivadas
GROUP BY
    c.customer_unique_id,
    c.customer_city,
    c.customer_state
ORDER BY
    valor_total_gasto DESC
LIMIT 10;

--Tempo Médio de Entrega (Logística)
SELECT
    c.customer_state,
    COUNT(o.order_id) AS total_pedidos_entregues,
    -- Tempo de Trânsito: Quanto tempo a transportadora demorou (Envio -> Entrega)
    AVG(o.order_delivered_customer_date - o.order_delivered_carrier_date) AS tempo_medio_transporte,
    -- Lead Time Total: Quanto tempo o cliente esperou no total (Compra -> Entrega)
    AVG(o.order_delivered_customer_date - o.order_purchase_timestamp) AS tempo_medio_total_cliente
FROM
    "order" o
JOIN
    customer c ON o.customer_id = c.customer_id
WHERE
    o.order_status = 'delivered'
GROUP BY
    c.customer_state
ORDER BY
    tempo_medio_transporte DESC;

--

CREATE OR REPLACE FUNCTION calcular_distancia_km(lat1 float, lon1 float, lat2 float, lon2 float)
RETURNS float AS $func$
DECLARE
    R integer := 6371; -- Raio da Terra em km
    dlat float;
    dlon float;
    a float;
    c float;
BEGIN
    -- Se algum ponto for nulo, retorna nulo 
    IF lat1 IS NULL OR lon1 IS NULL OR lat2 IS NULL OR lon2 IS NULL THEN
        RETURN NULL;
    END IF;

    --graus para radianos (Postgres trig functions usam radianos)
    dlat := radians(lat2 - lat1);
    dlon := radians(lon2 - lon1);
    lat1 := radians(lat1);
    lat2 := radians(lat2);

    -- Fórmula de Haversine detalhada
    a := sin(dlat/2)^2 + cos(lat1) * cos(lat2) * sin(dlon/2)^2;
    c := 2 * asin(sqrt(a));

    -- Retorna a distância em Km arredondada para 2 casas
    RETURN ROUND((R * c)::numeric, 2);
END
$func$ LANGUAGE plpgsql;

WITH cliente_alvo AS (
    SELECT
        c.customer_id,
        AVG(g.geolocation_lat) as lat_cliente,
        AVG(g.geolocation_lng) as lng_cliente
    FROM
        customer c
    JOIN
        geo_location g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
    -- peguei um cliente específico (mas esse ID pode ser trocado por qualquer um da tabela customer)
    WHERE
        c.customer_unique_id = '8d50f5eadf50201ccdcedfb9e2ac8455'
    GROUP BY
        c.customer_id
),
vendedores_localizados AS (
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        AVG(g.geolocation_lat) as lat_seller,
        AVG(g.geolocation_lng) as lng_seller
    FROM
        seller s
    JOIN
        geo_location g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
    GROUP BY
        s.seller_id, s.seller_city, s.seller_state
)
SELECT
    v.seller_id,
    v.seller_city,
    v.seller_state,
    calcular_distancia_km(c.lat_cliente, c.lng_cliente, v.lat_seller, v.lng_seller) AS distancia_km
FROM
    vendedores_localizados v,
    cliente_alvo c
WHERE
    calcular_distancia_km(c.lat_cliente, c.lng_cliente, v.lat_seller, v.lng_seller) <= 50 
ORDER BY
    distancia_km ASC;


-- MEDIÇÃO INICIAL
EXPLAIN ANALYZE
SELECT
    p.product_category_name AS categoria,
    COUNT(oi.order_item_id) AS total_unidades_vendidas
FROM
    order_item oi
JOIN
    "order" o ON oi.order_id = o.order_id
JOIN
    product p ON oi.product_id = p.product_id
WHERE
    o.order_status = 'delivered'
    AND o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY
    p.product_category_name
ORDER BY
    total_unidades_vendidas DESC
LIMIT 5;

-- Criando um índice que cobre as duas colunas usadas no filtro WHERE
CREATE INDEX idx_order_status_date
ON "order" (order_status, order_purchase_timestamp);

-- MEDIÇÃO PÓS-OTIMIZAÇÃO
EXPLAIN ANALYZE
SELECT
    p.product_category_name AS categoria,
    COUNT(oi.order_item_id) AS total_unidades_vendidas
FROM
    order_item oi
JOIN
    "order" o ON oi.order_id = o.order_id
JOIN
    product p ON oi.product_id = p.product_id
WHERE
    o.order_status = 'delivered'
    AND o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY
    p.product_category_name
ORDER BY
    total_unidades_vendidas DESC
LIMIT 5;


--Materialized View

CREATE MATERIALIZED VIEW mv_vendas_por_categoria AS
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

CREATE INDEX idx_mv_categoria ON mv_vendas_por_categoria(total_unidades_vendidas DESC);

EXPLAIN ANALYZE
SELECT *
FROM mv_vendas_por_categoria
ORDER BY total_unidades_vendidas DESC
LIMIT 5;

--Tabela de Auditoria
CREATE TABLE audit_log (
    log_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,         
    operation_type VARCHAR(10) NOT NULL, 
    changed_by TEXT DEFAULT current_user, 
    changed_at TIMESTAMP DEFAULT NOW(),   
    old_data JSONB, 
    new_data JSONB  
);

-- Índice para buscar auditoria por tabela ou data 
CREATE INDEX idx_audit_table ON audit_log(table_name);
CREATE INDEX idx_audit_date ON audit_log(changed_at);

CREATE OR REPLACE FUNCTION fn_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO audit_log (table_name, operation_type, new_data)
        VALUES (TG_TABLE_NAME, 'INSERT', row_to_json(NEW));
        RETURN NEW;

    ELSIF (TG_OP = 'UPDATE') THEN
        -- Só loga se houver mudança real de dados
        IF NEW IS DISTINCT FROM OLD THEN
            INSERT INTO audit_log (table_name, operation_type, old_data, new_data)
            VALUES (TG_TABLE_NAME, 'UPDATE', row_to_json(OLD), row_to_json(NEW));
        END IF;
        RETURN NEW;

    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO audit_log (table_name, operation_type, old_data)
        VALUES (TG_TABLE_NAME, 'DELETE', row_to_json(OLD));
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Auditoria em Produtos (Para ver se alguém muda preço ou categoria)
CREATE TRIGGER trg_audit_product
AFTER INSERT OR UPDATE OR DELETE ON product
FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();

-- Auditoria em Pagamentos (Segurança Financeira)
CREATE TRIGGER trg_audit_payment
AFTER INSERT OR UPDATE OR DELETE ON order_payment
FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();

-- Auditoria em Pedidos (Para ver quem cancela pedidos)
CREATE TRIGGER trg_audit_order
AFTER INSERT OR UPDATE OR DELETE ON "order"
FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger();

--Testando a Auditoria

-- 1. alterar o preço de um produto qualquer (Simulando um erro)
UPDATE product
SET product_weight_g = 500
WHERE product_id = (SELECT product_id FROM product LIMIT 1);

-- 2. verificar o log de auditoria
SELECT
    changed_at,
    changed_by,
    table_name,
    operation_type,
    old_data->>'product_weight_g' as peso_antigo,
    new_data->>'product_weight_g' as peso_novo
FROM audit_log
WHERE table_name = 'product'
ORDER BY changed_at DESC;
