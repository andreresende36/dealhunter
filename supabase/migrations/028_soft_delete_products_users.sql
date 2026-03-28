-- =============================================================================
-- Migration 028: P16 — Soft delete em products e users
-- =============================================================================

ALTER TABLE products ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE users ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_products_deleted_at ON products(deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_deleted_at ON users(deleted_at) WHERE deleted_at IS NOT NULL;

-- Atualizar views para filtrar soft-deleted
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
  AND p.deleted_at IS NULL
  AND COALESCE(so.score_override, so.final_score) >= 60
  AND NOT EXISTS (
      SELECT 1 FROM sent_offers se
      WHERE se.scored_offer_id = so.id
        AND se.sent_at >= NOW() - INTERVAL '24 hours'
  )
ORDER BY so.queue_priority DESC, COALESCE(so.score_override, so.final_score) DESC;

CREATE OR REPLACE VIEW vw_last_24h_summary AS
SELECT
    (SELECT COUNT(*) FROM products WHERE last_seen_at >= NOW() - INTERVAL '24 hours' AND deleted_at IS NULL) AS products_scraped,
    (SELECT COUNT(*) FROM scored_offers WHERE scored_at >= NOW() - INTERVAL '24 hours') AS offers_scored,
    (SELECT COUNT(*) FROM scored_offers WHERE scored_at >= NOW() - INTERVAL '24 hours' AND status = 'approved') AS offers_approved,
    (SELECT COUNT(*) FROM sent_offers WHERE sent_at >= NOW() - INTERVAL '24 hours') AS offers_sent,
    (SELECT ROUND(AVG(final_score), 1) FROM scored_offers WHERE scored_at >= NOW() - INTERVAL '24 hours') AS avg_score,
    (SELECT MAX(discount_percent) FROM products WHERE last_seen_at >= NOW() - INTERVAL '24 hours' AND deleted_at IS NULL) AS max_discount_pct;

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
  AND p.deleted_at IS NULL
  AND so.status = 'approved'
ORDER BY so.final_score DESC, p.discount_percent DESC
LIMIT 20;
