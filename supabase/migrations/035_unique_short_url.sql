-- Migration 035: P14 — UNIQUE constraint on affiliate_links.short_url

ALTER TABLE affiliate_links
    ADD CONSTRAINT uq_affiliate_links_short_url UNIQUE (short_url);
