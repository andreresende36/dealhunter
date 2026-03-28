-- Migration 036: P19 — Remover índice duplicado em products.ml_id
-- A constraint UNIQUE em ml_id já cria seu próprio índice automaticamente.
-- O idx_products_ml_id (não-único) é redundante.

DROP INDEX IF EXISTS idx_products_ml_id;
