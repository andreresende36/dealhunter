-- =============================================================================
-- Migration 024: P02 — Normalizar products.brand → tabela brands
--
-- Cria tabela brands, migra dados, adiciona FK, dropa coluna antiga.
-- =============================================================================

-- Criar tabela brands
CREATE TABLE IF NOT EXISTS brands (
    id          UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trigger_brands_updated_at ON brands;
CREATE TRIGGER trigger_brands_updated_at
    BEFORE UPDATE ON brands
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- RLS
ALTER TABLE brands ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'brands_public_read' AND tablename = 'brands') THEN
        CREATE POLICY "brands_public_read" ON brands FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'brands_service_write' AND tablename = 'brands') THEN
        CREATE POLICY "brands_service_write" ON brands FOR ALL
            USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
    END IF;
END$$;

-- Migrar dados existentes
INSERT INTO brands (name)
SELECT DISTINCT TRIM(brand) FROM products WHERE brand IS NOT NULL AND TRIM(brand) <> ''
ON CONFLICT (name) DO NOTHING;

-- Adicionar FK
ALTER TABLE products ADD COLUMN IF NOT EXISTS brand_id UUID REFERENCES brands(id);
CREATE INDEX IF NOT EXISTS idx_products_brand_id ON products(brand_id);

-- Popular FK a partir da coluna texto
UPDATE products p
SET brand_id = b.id
FROM brands b
WHERE TRIM(p.brand) = b.name AND p.brand IS NOT NULL AND TRIM(p.brand) <> '';

-- Dropar coluna antiga
ALTER TABLE products DROP COLUMN IF EXISTS brand;

-- Atualizar views que referenciam p.brand
CREATE OR REPLACE VIEW vw_approved_unsent AS
SELECT
    p.id            AS product_id,
    p.ml_id,
    p.title,
    p.current_price,
    p.original_price,
    p.pix_price,
    p.discount_percent,
    p.discount_type,
    p.free_shipping,
    p.full_shipping,
    br.name         AS brand,
    p.thumbnail_url,
    p.product_url,
    p.rating_stars,
    p.rating_count,
    p.installments_without_interest,
    p.installment_count,
    p.installment_value,
    c.name          AS category,
    b.name          AS badge,
    so.id           AS scored_offer_id,
    so.final_score,
    so.scored_at,
    so.queue_priority,
    so.score_override,
    so.admin_notes
FROM scored_offers so
JOIN products p ON p.id = so.product_id
LEFT JOIN categories c ON c.id = p.category_id
LEFT JOIN badges b ON b.id = p.badge_id
LEFT JOIN brands br ON br.id = p.brand_id
WHERE so.status = 'approved'
  AND COALESCE(so.score_override, so.final_score) >= 60
  AND NOT EXISTS (
      SELECT 1 FROM sent_offers se
      WHERE se.scored_offer_id = so.id
        AND se.sent_at >= NOW() - INTERVAL '24 hours'
  )
ORDER BY so.queue_priority DESC, COALESCE(so.score_override, so.final_score) DESC;

CREATE OR REPLACE VIEW vw_top_deals AS
SELECT
    p.id            AS product_id,
    p.ml_id,
    p.title,
    p.current_price,
    p.original_price,
    p.pix_price,
    p.discount_percent,
    p.discount_type,
    p.free_shipping,
    p.full_shipping,
    br.name         AS brand,
    p.thumbnail_url,
    p.product_url,
    p.installment_count,
    p.installment_value,
    c.name          AS category,
    b.name          AS badge,
    so.final_score
FROM products p
JOIN scored_offers so ON so.product_id = p.id
LEFT JOIN categories c ON c.id = p.category_id
LEFT JOIN badges b ON b.id = p.badge_id
LEFT JOIN brands br ON br.id = p.brand_id
WHERE p.last_seen_at >= NOW() - INTERVAL '6 hours'
  AND so.status = 'approved'
ORDER BY so.final_score DESC, p.discount_percent DESC
LIMIT 20;
