-- =============================================================================
-- DealHunter — Migration: drop_enrichment_columns
-- Versão: 20260306022612
--
-- Remove colunas do deep scrape worker (descontinuado):
--   - seller_reputation, sold_quantity (enrichment data)
--   - enrichment_status, enrichment_attempts, enrichment_error, enriched_at
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
