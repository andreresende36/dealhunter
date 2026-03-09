-- =============================================================================
-- DealHunter — Migration: add_enrichment_columns_to_products
-- Versão: 20260303223240
--
-- Adiciona colunas de enriquecimento para o deep scrape worker.
-- (Descontinuadas na migration 20260306022612)
-- =============================================================================

-- Adicionar colunas de enriquecimento à tabela products
ALTER TABLE products
  ADD COLUMN IF NOT EXISTS enrichment_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (enrichment_status IN (
      'pending',
      'in_progress',
      'enriched',
      'scored',
      'published',
      'failed',
      'skipped'
    ));

ALTER TABLE products
  ADD COLUMN IF NOT EXISTS enrichment_attempts INTEGER NOT NULL DEFAULT 0;

ALTER TABLE products
  ADD COLUMN IF NOT EXISTS enrichment_error TEXT DEFAULT '';

ALTER TABLE products
  ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMPTZ;

-- Índice para queries do worker (produtos pendentes)
CREATE INDEX IF NOT EXISTS idx_products_enrichment_status
  ON products(enrichment_status);

-- Índice para claim do worker (in_progress por tempo)
CREATE INDEX IF NOT EXISTS idx_products_enrichment_pending
  ON products(enrichment_status, last_seen_at DESC)
  WHERE enrichment_status IN ('pending', 'failed');
