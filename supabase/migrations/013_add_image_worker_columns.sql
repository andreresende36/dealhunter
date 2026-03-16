-- Migration: Add image worker columns to products
-- Adds enhanced_image_url and image_status for the async image enhancement worker.

ALTER TABLE products ADD COLUMN enhanced_image_url TEXT;
ALTER TABLE products ADD COLUMN image_status TEXT DEFAULT 'pending';

CREATE INDEX idx_products_image_status ON products(image_status);

-- Backfill: existing products start as 'pending' (default handles this)
