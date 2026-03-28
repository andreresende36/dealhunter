-- =============================================================================
-- Migration 026: P04 — products.variations de TEXT para JSONB
-- =============================================================================

ALTER TABLE products ALTER COLUMN variations TYPE JSONB USING
    CASE
        WHEN variations IS NULL THEN NULL
        WHEN variations = '' THEN NULL
        ELSE jsonb_build_object('raw', variations)
    END;
