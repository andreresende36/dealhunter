-- Migration 033: P01 — UNIQUE constraint on users.affiliate_tag
-- O índice idx_users_affiliate_tag (não-único) se torna redundante após a UNIQUE.

ALTER TABLE users
    ADD CONSTRAINT uq_users_affiliate_tag UNIQUE (affiliate_tag);

-- Dropar índice não-único agora redundante (a UNIQUE criou seu próprio índice)
DROP INDEX IF EXISTS idx_users_affiliate_tag;
