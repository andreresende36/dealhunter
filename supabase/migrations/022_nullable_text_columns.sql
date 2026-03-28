-- =============================================================================
-- Migration 022: Torna nullable as colunas de texto sem valor atribuível
--
-- Contexto: brand, variations e discount_type usavam '' (string vazia) como
-- sentinela para "valor não encontrado". NULL é semanticamente correto para
-- indicar ausência de valor.
-- =============================================================================

-- 1. Remove o DEFAULT '' das colunas (passa a ter DEFAULT NULL)
ALTER TABLE products ALTER COLUMN brand       SET DEFAULT NULL;
ALTER TABLE products ALTER COLUMN variations  SET DEFAULT NULL;
ALTER TABLE products ALTER COLUMN discount_type SET DEFAULT NULL;

-- 2. Backfill: converte string vazia existente para NULL
UPDATE products SET brand         = NULL WHERE brand = '';
UPDATE products SET variations    = NULL WHERE variations = '';
UPDATE products SET discount_type = NULL WHERE discount_type = '';
