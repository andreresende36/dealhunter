-- =============================================================================
-- DealHunter — Migration: add_badges_table_and_badge_id
-- Versão: 20260309204836
--
-- Cria tabela de lookup para badges de oferta do Mercado Livre
-- e adiciona FK badge_id na tabela products.
--
-- Nota: Seeds são gerenciados por src/database/seeds.py em runtime.
-- =============================================================================

-- Tabela de lookup para badges de oferta do Mercado Livre
CREATE TABLE IF NOT EXISTS badges (
    id          UUID    DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        TEXT    NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Seed: badges conhecidos
INSERT INTO badges (name) VALUES
    ('Oferta do dia'),
    ('Oferta relâmpago'),
    ('Mais vendido'),
    ('Oferta imperdível')
ON CONFLICT (name) DO NOTHING;

-- Adiciona coluna badge_id na tabela products
ALTER TABLE products ADD COLUMN IF NOT EXISTS badge_id UUID REFERENCES badges(id);

CREATE INDEX IF NOT EXISTS idx_products_badge_id ON products(badge_id);

-- RLS
ALTER TABLE badges ENABLE ROW LEVEL SECURITY;

CREATE POLICY "badges_public_read"
    ON badges FOR SELECT USING (true);

CREATE POLICY "badges_service_write"
    ON badges FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');
