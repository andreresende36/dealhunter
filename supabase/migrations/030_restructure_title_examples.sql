-- =============================================================================
-- Migration 030: P05+P06 — Reestruturar title_examples
--
-- P05: Normalizar category → category_id FK
-- P06: Remover product_title e price (deriváveis via scored_offer_id → products)
-- =============================================================================

-- P05 — Adicionar FK para categories
ALTER TABLE title_examples ADD COLUMN IF NOT EXISTS category_id UUID REFERENCES categories(id);

-- Popular category_id a partir do texto
UPDATE title_examples te
SET category_id = c.id
FROM categories c
WHERE LOWER(TRIM(te.category)) = LOWER(TRIM(c.name))
  AND te.category IS NOT NULL;

-- Dropar coluna texto
ALTER TABLE title_examples DROP COLUMN IF EXISTS category;

-- P06 — Remover product_title e price
ALTER TABLE title_examples DROP COLUMN IF EXISTS product_title;
ALTER TABLE title_examples DROP COLUMN IF EXISTS price;
