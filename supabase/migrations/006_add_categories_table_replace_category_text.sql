-- =============================================================================
-- DealHunter — Migration: add_categories_table_replace_category_text
-- Versão: 20260309210131
--
-- Cria tabela de lookup para categorias do Mercado Livre,
-- substitui a coluna category (TEXT) por category_id (FK UUID),
-- e recria as views dependentes.
--
-- Nota: Seeds são gerenciados por src/database/seeds.py em runtime.
-- =============================================================================

-- Tabela de lookup para categorias do Mercado Livre
CREATE TABLE IF NOT EXISTS categories (
    id          UUID    DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        TEXT    NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Seed: todas as categorias do ML + "Outros"
INSERT INTO categories (name) VALUES
    ('Acessórios para Veículos'),
    ('Agro'),
    ('Alimentos e Bebidas'),
    ('Pet Shop'),
    ('Antiguidades e Coleções'),
    ('Arte, Papelaria e Armarinho'),
    ('Bebês'),
    ('Beleza e Cuidado Pessoal'),
    ('Brinquedos e Hobbies'),
    ('Calçados, Roupas e Bolsas'),
    ('Câmeras e Acessórios'),
    ('Carros, Motos e Outros'),
    ('Casa, Móveis e Decoração'),
    ('Celulares e Telefones'),
    ('Construção'),
    ('Eletrodomésticos'),
    ('Eletrônicos, Áudio e Vídeo'),
    ('Esportes e Fitness'),
    ('Ferramentas'),
    ('Festas e Lembrancinhas'),
    ('Games'),
    ('Imóveis'),
    ('Indústria e Comércio'),
    ('Informática'),
    ('Ingressos'),
    ('Instrumentos Musicais'),
    ('Joias e Relógios'),
    ('Livros, Revistas e Comics'),
    ('Música, Filmes e Seriados'),
    ('Saúde'),
    ('Serviços'),
    ('Outros')
ON CONFLICT (name) DO NOTHING;

-- Drop views que dependem de category primeiro
DROP VIEW IF EXISTS vw_approved_unsent;
DROP VIEW IF EXISTS vw_top_deals;

-- Remove a coluna category TEXT e adiciona category_id UUID
ALTER TABLE products DROP COLUMN IF EXISTS category;
ALTER TABLE products ADD COLUMN category_id UUID REFERENCES categories(id);
CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
DROP INDEX IF EXISTS idx_products_category;

-- RLS
ALTER TABLE categories ENABLE ROW LEVEL SECURITY;

CREATE POLICY "categories_public_read"
    ON categories FOR SELECT USING (true);

CREATE POLICY "categories_service_write"
    ON categories FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- Recria views com JOIN na tabela categories
CREATE OR REPLACE VIEW vw_approved_unsent AS
SELECT
    p.id            AS product_id,
    p.ml_id,
    p.title,
    p.current_price,
    p.original_price,
    p.discount_percent,
    p.free_shipping,
    p.thumbnail_url,
    p.product_url,
    c.name          AS category,
    so.id           AS scored_offer_id,
    so.final_score,
    so.ai_description,
    so.scored_at
FROM scored_offers so
JOIN products p ON p.id = so.product_id
LEFT JOIN categories c ON c.id = p.category_id
WHERE so.status = 'approved'
  AND so.final_score >= 60
  AND NOT EXISTS (
      SELECT 1 FROM sent_offers se
      WHERE se.scored_offer_id = so.id
        AND se.sent_at >= NOW() - INTERVAL '24 hours'
  )
ORDER BY so.final_score DESC;

CREATE OR REPLACE VIEW vw_top_deals AS
SELECT
    p.ml_id,
    p.title,
    p.current_price,
    p.original_price,
    p.discount_percent,
    p.free_shipping,
    c.name          AS category,
    so.final_score,
    p.product_url
FROM products p
JOIN scored_offers so ON so.product_id = p.id
LEFT JOIN categories c ON c.id = p.category_id
WHERE p.last_seen_at >= NOW() - INTERVAL '6 hours'
  AND so.status = 'approved'
ORDER BY so.final_score DESC, p.discount_percent DESC
LIMIT 20;
