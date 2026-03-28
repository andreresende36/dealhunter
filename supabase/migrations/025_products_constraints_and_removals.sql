-- =============================================================================
-- Migration 025: P03 — Constraints e remoções em products
--
-- P03a: CHECK em discount_type
-- P03b: Remover image_status e enhanced_image_url
-- P03c: Padronizar gender
-- =============================================================================

-- P03a — CHECK em discount_type
-- Primeiro limpar valores inválidos
UPDATE products SET discount_type = NULL
WHERE discount_type IS NOT NULL AND discount_type NOT IN ('standard', 'pix');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'chk_products_discount_type' AND table_name = 'products'
    ) THEN
        ALTER TABLE products ADD CONSTRAINT chk_products_discount_type
            CHECK (discount_type IN ('standard', 'pix'));
    END IF;
END$$;

-- P03b — Remover image_status e enhanced_image_url
DROP INDEX IF EXISTS idx_products_image_status;
ALTER TABLE products DROP COLUMN IF EXISTS image_status;
ALTER TABLE products DROP COLUMN IF EXISTS enhanced_image_url;

-- P03c — Padronizar gender
UPDATE products SET gender = 'gender_neutral'
WHERE gender NOT IN ('male', 'female', 'unisex', 'gender_neutral') OR gender IS NULL;

ALTER TABLE products ALTER COLUMN gender SET DEFAULT 'gender_neutral';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'chk_products_gender' AND table_name = 'products'
    ) THEN
        ALTER TABLE products ADD CONSTRAINT chk_products_gender
            CHECK (gender IN ('male', 'female', 'unisex', 'gender_neutral'));
    END IF;
END$$;
