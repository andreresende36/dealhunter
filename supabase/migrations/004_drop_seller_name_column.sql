-- =============================================================================
-- DealHunter — Migration: drop_seller_name_column
-- Versão: 20260306141049
--
-- Remove a coluna seller_name que não é mais coletada pelo scraper.
-- =============================================================================

ALTER TABLE products DROP COLUMN seller_name;
