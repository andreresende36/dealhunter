-- =============================================================================
-- Migration 031: P07 — Adicionar user_id em sent_offers
-- =============================================================================

ALTER TABLE sent_offers ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id);
CREATE INDEX IF NOT EXISTS idx_sent_offers_user_id ON sent_offers(user_id);
