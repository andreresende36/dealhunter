-- =============================================================================
-- DealHunter — Migration 002: Remove enrichment pipeline columns
--
-- Removes columns that were used by the deep scrape worker (discontinued):
--   - seller_reputation, sold_quantity (enrichment data)
--   - enrichment_status, enrichment_attempts, enrichment_error, enriched_at
--
-- Also drops related indexes.
-- =============================================================================

-- Drop enrichment indexes first
DROP INDEX IF EXISTS idx_products_enrichment_status;
DROP INDEX IF EXISTS idx_products_enrichment_pending;

-- Drop enrichment columns
ALTER TABLE products DROP COLUMN IF EXISTS seller_reputation;
ALTER TABLE products DROP COLUMN IF EXISTS sold_quantity;
ALTER TABLE products DROP COLUMN IF EXISTS enrichment_status;
ALTER TABLE products DROP COLUMN IF EXISTS enrichment_attempts;
ALTER TABLE products DROP COLUMN IF EXISTS enrichment_error;
ALTER TABLE products DROP COLUMN IF EXISTS enriched_at;
