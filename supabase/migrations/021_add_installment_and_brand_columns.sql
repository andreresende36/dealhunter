-- =============================================================================
-- Migration 021: Adiciona colunas de parcelamento, marca, FULL e variações
--
-- Contexto: Reestruturação da extração de dados dos cards ML para melhorar
-- precisão de preços, descontos e parcelamento nas mensagens.
-- =============================================================================

-- 1. Novas colunas em products
ALTER TABLE products ADD COLUMN IF NOT EXISTS brand TEXT DEFAULT '';
ALTER TABLE products ADD COLUMN IF NOT EXISTS full_shipping BOOLEAN DEFAULT FALSE;
ALTER TABLE products ADD COLUMN IF NOT EXISTS variations TEXT DEFAULT '';
ALTER TABLE products ADD COLUMN IF NOT EXISTS installment_count SMALLINT;
ALTER TABLE products ADD COLUMN IF NOT EXISTS installment_value DECIMAL(10,2);
ALTER TABLE products ADD COLUMN IF NOT EXISTS discount_type TEXT DEFAULT '';

-- 2. Adiciona pix_price ao price_history (tracking de preço Pix)
ALTER TABLE price_history ADD COLUMN IF NOT EXISTS pix_price DECIMAL(10,2);

-- 3. Recria vw_approved_unsent com novos campos
DROP VIEW IF EXISTS vw_approved_unsent;
CREATE VIEW vw_approved_unsent AS
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
    p.brand,
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
WHERE so.status = 'approved'
  AND COALESCE(so.score_override, so.final_score) >= 60
  AND NOT EXISTS (
      SELECT 1 FROM sent_offers se
      WHERE se.scored_offer_id = so.id
        AND se.sent_at >= NOW() - INTERVAL '24 hours'
  )
ORDER BY so.queue_priority DESC, COALESCE(so.score_override, so.final_score) DESC;

-- 4. Recria vw_top_deals com novos campos
DROP VIEW IF EXISTS vw_top_deals;
CREATE VIEW vw_top_deals AS
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
    p.brand,
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
WHERE p.last_seen_at >= NOW() - INTERVAL '6 hours'
  AND so.status = 'approved'
ORDER BY so.final_score DESC, p.discount_percent DESC
LIMIT 20;
