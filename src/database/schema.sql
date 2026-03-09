-- =============================================================================
-- DealHunter — Schema PostgreSQL (Supabase)
-- Versão: 2.0 (Semana 1, Dias 3-5)
--
-- Executar via: Supabase Dashboard → SQL Editor → New Query
-- Cole este arquivo inteiro e clique em "Run"
--
-- Tabelas:
--   1. products        — catálogo de produtos coletados pelo scraper
--   2. price_history   — histórico de preços para detectar pricejacking
--   3. scored_offers   — resultado da análise (score engine + IA)
--   4. sent_offers     — controle de envios para evitar duplicatas
--   5. system_logs     — logs operacionais e monitoramento
-- =============================================================================

-- Extensões necessárias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "pg_trgm";     -- busca textual fuzzy (opcional)

-- =============================================================================
-- 1. badges
-- Tabela de lookup para os badges de oferta do Mercado Livre.
-- Cada badge possui um nome único (ex: "Oferta do dia", "Mais vendido").
-- =============================================================================
CREATE TABLE IF NOT EXISTS badges (
    id          UUID    DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        TEXT    NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Seed de badges é gerenciado por src/database/seeds.py (fonte única de verdade)
-- Inseridos automaticamente na inicialização do sistema.

-- =============================================================================
-- 1b. categories
-- Tabela de lookup para as categorias do Mercado Livre.
-- =============================================================================
CREATE TABLE IF NOT EXISTS categories (
    id          UUID    DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        TEXT    NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Seed de categorias é gerenciado por src/database/seeds.py (fonte única de verdade)
-- Inseridos automaticamente na inicialização do sistema.

-- =============================================================================
-- 2. products
-- Catálogo de todos os produtos encontrados pelo scraper.
-- ml_id é o identificador único no Mercado Livre.
-- id (UUID) é o PK interno usado como FK nas demais tabelas.
-- =============================================================================
CREATE TABLE IF NOT EXISTS products (
    id                  UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    ml_id               TEXT        NOT NULL UNIQUE,
    title               TEXT        NOT NULL,
    current_price       DECIMAL(10,2) NOT NULL,
    original_price      DECIMAL(10,2),
    discount_percent    INTEGER     DEFAULT 0,
    rating_stars        DECIMAL(3,1) DEFAULT 0,
    rating_count        INTEGER     DEFAULT 0,
    free_shipping       BOOLEAN     DEFAULT FALSE,
    thumbnail_url       TEXT        DEFAULT '',
    product_url         TEXT        NOT NULL DEFAULT '',
    category_id         UUID        REFERENCES categories(id),
    badge_id            UUID        REFERENCES badges(id),
    first_seen_at       TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Índices de products
CREATE INDEX IF NOT EXISTS idx_products_ml_id
    ON products(ml_id);

CREATE INDEX IF NOT EXISTS idx_products_discount
    ON products(discount_percent DESC);

CREATE INDEX IF NOT EXISTS idx_products_last_seen
    ON products(last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_products_category_id
    ON products(category_id);

CREATE INDEX IF NOT EXISTS idx_products_price
    ON products(current_price);

CREATE INDEX IF NOT EXISTS idx_products_badge_id
    ON products(badge_id);

-- Trigger: preserva first_seen_at e atualiza last_seen_at automaticamente nos UPDATEs
CREATE OR REPLACE FUNCTION fn_products_on_update()
RETURNS TRIGGER AS $$
BEGIN
    NEW.first_seen_at = OLD.first_seen_at;   -- nunca sobrescreve a data de primeira coleta
    NEW.last_seen_at  = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_products_on_update
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION fn_products_on_update();

-- =============================================================================
-- 3. price_history
-- Registra cada variação de preço detectada pelo scraper.
-- Usado pelo FakeDiscountDetector para calcular o preço médio histórico.
-- =============================================================================
CREATE TABLE IF NOT EXISTS price_history (
    id              UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    product_id      UUID        NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    price           DECIMAL(10,2) NOT NULL,
    original_price  DECIMAL(10,2),
    recorded_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Índices de price_history
CREATE INDEX IF NOT EXISTS idx_price_history_product_id
    ON price_history(product_id);

CREATE INDEX IF NOT EXISTS idx_price_history_recorded_at
    ON price_history(recorded_at DESC);

-- Índice composto para queries de histórico por produto + período
CREATE INDEX IF NOT EXISTS idx_price_history_product_recorded
    ON price_history(product_id, recorded_at DESC);

-- =============================================================================
-- 4. scored_offers
-- Resultado da análise de cada oferta pelo Score Engine e/ou IA.
-- Uma oferta pode ser analisada múltiplas vezes (ex: score atualizado pela IA).
-- =============================================================================
CREATE TABLE IF NOT EXISTS scored_offers (
    id              UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    product_id      UUID        NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    rule_score      INTEGER     NOT NULL CHECK (rule_score BETWEEN 0 AND 100),
    ai_score        INTEGER     CHECK (ai_score BETWEEN 0 AND 100),  -- NULL = não analisado pela IA
    final_score     INTEGER     NOT NULL CHECK (final_score BETWEEN 0 AND 100),
    ai_description  TEXT,                                             -- Texto gerado pelo Claude
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'approved', 'rejected')),
    scored_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Índices de scored_offers
CREATE INDEX IF NOT EXISTS idx_scored_offers_product_id
    ON scored_offers(product_id);

CREATE INDEX IF NOT EXISTS idx_scored_offers_scored_at
    ON scored_offers(scored_at DESC);

CREATE INDEX IF NOT EXISTS idx_scored_offers_status
    ON scored_offers(status);

CREATE INDEX IF NOT EXISTS idx_scored_offers_final_score
    ON scored_offers(final_score DESC);

-- =============================================================================
-- 5. sent_offers
-- Registra cada envio para WhatsApp ou Telegram.
-- Permite detectar duplicatas e rastrear cliques via Shlink.
-- Uma scored_offer pode ser enviada para múltiplos canais (1 linha por canal).
-- =============================================================================
CREATE TABLE IF NOT EXISTS sent_offers (
    id                  UUID    DEFAULT uuid_generate_v4() PRIMARY KEY,
    scored_offer_id     UUID    NOT NULL REFERENCES scored_offers(id) ON DELETE CASCADE,
    channel             TEXT    NOT NULL CHECK (channel IN ('telegram', 'whatsapp')),
    shlink_short_url    TEXT    NOT NULL DEFAULT '',
    sent_at             TIMESTAMPTZ DEFAULT NOW(),
    clicks              INTEGER DEFAULT 0
);

-- Índices de sent_offers
CREATE INDEX IF NOT EXISTS idx_sent_offers_scored_offer_id
    ON sent_offers(scored_offer_id);

CREATE INDEX IF NOT EXISTS idx_sent_offers_sent_at
    ON sent_offers(sent_at DESC);

CREATE INDEX IF NOT EXISTS idx_sent_offers_channel
    ON sent_offers(channel);

-- =============================================================================
-- 6. system_logs
-- Logs operacionais e métricas de cada execução do pipeline.
-- Consultado pelo módulo de monitoring para relatórios e alertas.
-- =============================================================================
CREATE TABLE IF NOT EXISTS system_logs (
    id          UUID    DEFAULT uuid_generate_v4() PRIMARY KEY,
    event_type  TEXT    NOT NULL,  -- "scrape_success"|"scrape_error"|"score_run"|"send_ok"|"send_error"
    details     JSONB   DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Índices de system_logs
CREATE INDEX IF NOT EXISTS idx_system_logs_event_type
    ON system_logs(event_type);

CREATE INDEX IF NOT EXISTS idx_system_logs_created_at
    ON system_logs(created_at DESC);

-- Índice GIN para queries no campo JSONB (ex: buscar por ml_id nos detalhes)
CREATE INDEX IF NOT EXISTS idx_system_logs_details_gin
    ON system_logs USING gin(details);

-- =============================================================================
-- Row Level Security (RLS)
-- service_role (chave server-side) tem acesso total via bypass.
-- anon key tem acesso somente de leitura via policies explícitas.
-- =============================================================================

ALTER TABLE badges       ENABLE ROW LEVEL SECURITY;
ALTER TABLE categories   ENABLE ROW LEVEL SECURITY;
ALTER TABLE products      ENABLE ROW LEVEL SECURITY;
ALTER TABLE price_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE scored_offers ENABLE ROW LEVEL SECURITY;
ALTER TABLE sent_offers   ENABLE ROW LEVEL SECURITY;
ALTER TABLE system_logs   ENABLE ROW LEVEL SECURITY;

-- service_role bypassa RLS automaticamente — não precisa de policy.
-- As policies abaixo são para o anon key (leitura pública dos dados de oferta).

CREATE POLICY "badges_public_read"
    ON badges FOR SELECT USING (true);

CREATE POLICY "badges_service_write"
    ON badges FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "categories_public_read"
    ON categories FOR SELECT USING (true);

CREATE POLICY "categories_service_write"
    ON categories FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "products_public_read"
    ON products FOR SELECT USING (true);

CREATE POLICY "products_service_write"
    ON products FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "price_history_public_read"
    ON price_history FOR SELECT USING (true);

CREATE POLICY "price_history_service_write"
    ON price_history FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "scored_offers_public_read"
    ON scored_offers FOR SELECT USING (true);

CREATE POLICY "scored_offers_service_write"
    ON scored_offers FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "sent_offers_public_read"
    ON sent_offers FOR SELECT USING (true);

CREATE POLICY "sent_offers_service_write"
    ON sent_offers FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE POLICY "system_logs_service_only"
    ON system_logs FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- =============================================================================
-- Views
-- =============================================================================

-- Ofertas aprovadas prontas para envio (score >= 60, ainda não enviadas hoje)
CREATE OR REPLACE VIEW vw_approved_unsent AS
SELECT
    p.id            AS product_id,
    p.ml_id,
    p.title,
    p.current_price,
    p.original_price,
    p.discount_percent,
    p.free_shipping,
    p.thumbnail_url,
    p.product_url,
    c.name          AS category,
    so.id           AS scored_offer_id,
    so.final_score,
    so.ai_description,
    so.scored_at
FROM scored_offers so
JOIN products p ON p.id = so.product_id
LEFT JOIN categories c ON c.id = p.category_id
WHERE so.status = 'approved'
  AND so.final_score >= 60
  AND NOT EXISTS (
      SELECT 1 FROM sent_offers se
      WHERE se.scored_offer_id = so.id
        AND se.sent_at >= NOW() - INTERVAL '24 hours'
  )
ORDER BY so.final_score DESC;

-- Resumo das últimas 24 horas
CREATE OR REPLACE VIEW vw_last_24h_summary AS
SELECT
    (SELECT COUNT(*) FROM products      WHERE last_seen_at >= NOW() - INTERVAL '24 hours')  AS products_scraped,
    (SELECT COUNT(*) FROM scored_offers WHERE scored_at   >= NOW() - INTERVAL '24 hours')  AS offers_scored,
    (SELECT COUNT(*) FROM scored_offers WHERE scored_at   >= NOW() - INTERVAL '24 hours'
                                         AND status = 'approved')                           AS offers_approved,
    (SELECT COUNT(*) FROM sent_offers   WHERE sent_at     >= NOW() - INTERVAL '24 hours')  AS offers_sent,
    (SELECT ROUND(AVG(final_score),1)
       FROM scored_offers WHERE scored_at >= NOW() - INTERVAL '24 hours')                  AS avg_score,
    (SELECT MAX(discount_percent)
       FROM products WHERE last_seen_at  >= NOW() - INTERVAL '24 hours')                   AS max_discount_pct;

-- Top produtos por desconto (últimas 6h)
CREATE OR REPLACE VIEW vw_top_deals AS
SELECT
    p.ml_id,
    p.title,
    p.current_price,
    p.original_price,
    p.discount_percent,
    p.free_shipping,
    c.name          AS category,
    so.final_score,
    p.product_url
FROM products p
JOIN scored_offers so ON so.product_id = p.id
LEFT JOIN categories c ON c.id = p.category_id
WHERE p.last_seen_at >= NOW() - INTERVAL '6 hours'
  AND so.status = 'approved'
ORDER BY so.final_score DESC, p.discount_percent DESC
LIMIT 20;
