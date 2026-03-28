-- =============================================================================
-- Migration 032: P13 — Remover sent_offers.clicks
-- =============================================================================

ALTER TABLE sent_offers DROP COLUMN IF EXISTS clicks;
