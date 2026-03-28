-- =============================================================================
-- Crivo — Schema PostgreSQL (Supabase)
-- Snapshot do schema final após todas as migrations (001–040).
-- Este arquivo é documentação de referência e criação do zero.
-- Para alterações incrementais use supabase/migrations/.
-- =============================================================================

-- Extensões necessárias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "pg_trgm";     -- busca textual fuzzy (opcional)

-- =============================================================================
-- Função reutilizável: auto-update updated_at
-- =============================================================================
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 1. badges
-- =============================================================================
CREATE TABLE IF NOT EXISTS badges (
    id          UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER trigger_badges_updated_at
    BEFORE UPDATE ON badges
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- =============================================================================
-- 2. categories
-- =============================================================================
CREATE TABLE IF NOT EXISTS categories (
    id          UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER trigger_categories_updated_at
    BEFORE UPDATE ON categories
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- =============================================================================
-- 3. marketplaces
-- =============================================================================
CREATE TABLE IF NOT EXISTS marketplaces (
    id          UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER trigger_marketplaces_updated_at
    BEFORE UPDATE ON marketplaces
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- =============================================================================
-- 4. brands
-- =============================================================================
CREATE TABLE IF NOT EXISTS brands (
    id          UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER trigger_brands_updated_at
    BEFORE UPDATE ON brands
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- =============================================================================
-- 5. products
-- =============================================================================
CREATE TABLE IF NOT EXISTS products (
    id                            UUID          DEFAULT uuid_generate_v4() PRIMARY KEY,
    ml_id                         TEXT          NOT NULL UNIQUE,
    title                         TEXT          NOT NULL,
    current_price                 DECIMAL(10,2) NOT NULL,
    original_price                DECIMAL(10,2),
    pix_price                     DECIMAL(10,2),
    discount_percent              DECIMAL(4,1)  DEFAULT 0,
    rating_stars                  DECIMAL(3,1)  DEFAULT 0,
    rating_count                  INTEGER       DEFAULT 0,
    free_shipping                 BOOLEAN       DEFAULT FALSE,
    full_shipping                 BOOLEAN       DEFAULT FALSE,
    installments_without_interest BOOLEAN       DEFAULT FALSE,
    installment_count             SMALLINT,
    installment_value             DECIMAL(10,2),
    brand_id                      UUID          REFERENCES brands(id),
    variations                    JSONB,
    discount_type                 TEXT          CONSTRAINT chk_products_discount_type
                                                CHECK (discount_type IN ('standard', 'pix')),
    gender                        TEXT          DEFAULT 'gender_neutral'
                                                CONSTRAINT chk_products_gender
                                                CHECK (gender IN ('male', 'female', 'unisex', 'gender_neutral')),
    thumbnail_url                 TEXT          DEFAULT '',
    product_url                   TEXT          NOT NULL DEFAULT ''
                                                CONSTRAINT chk_products_product_url CHECK (product_url <> ''),
    category_id                   UUID          REFERENCES categories(id),
    badge_id                      UUID          REFERENCES badges(id),
    marketplace_id                UUID          REFERENCES marketplaces(id),
    first_seen_at                 TIMESTAMPTZ   DEFAULT NOW(),
    last_seen_at                  TIMESTAMPTZ   DEFAULT NOW(),
    created_at                    TIMESTAMPTZ   DEFAULT NOW(),
    deleted_at                    TIMESTAMPTZ
);

-- idx_products_ml_id removido (P19): o UNIQUE em ml_id já cria seu próprio índice
CREATE INDEX IF NOT EXISTS idx_products_discount        ON products(discount_percent DESC);
CREATE INDEX IF NOT EXISTS idx_products_last_seen       ON products(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_products_category_id     ON products(category_id);
CREATE INDEX IF NOT EXISTS idx_products_badge_id        ON products(badge_id);
CREATE INDEX IF NOT EXISTS idx_products_marketplace_id  ON products(marketplace_id);
CREATE INDEX IF NOT EXISTS idx_products_brand_id        ON products(brand_id);
CREATE INDEX IF NOT EXISTS idx_products_price           ON products(current_price);
CREATE INDEX IF NOT EXISTS idx_products_deleted_at      ON products(deleted_at) WHERE deleted_at IS NOT NULL;

CREATE OR REPLACE FUNCTION fn_products_on_update()
RETURNS TRIGGER AS $$
BEGIN
    NEW.first_seen_at = OLD.first_seen_at;
    NEW.last_seen_at  = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_products_on_update
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION fn_products_on_update();

-- =============================================================================
-- 6. price_history (particionada por mês — P17)
-- =============================================================================
CREATE TABLE IF NOT EXISTS price_history (
    id              UUID          DEFAULT uuid_generate_v4(),
    product_id      UUID          NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    price           DECIMAL(10,2) NOT NULL,
    original_price  DECIMAL(10,2),
    pix_price       DECIMAL(10,2),
    recorded_at     TIMESTAMPTZ   DEFAULT NOW(),
    PRIMARY KEY (id, recorded_at)
) PARTITION BY RANGE (recorded_at);

-- Partições mensais (criar novas mensalmente via fn_create_price_history_partition)
CREATE TABLE IF NOT EXISTS price_history_y2026m03 PARTITION OF price_history FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS price_history_y2026m04 PARTITION OF price_history FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS price_history_y2026m05 PARTITION OF price_history FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS price_history_y2026m06 PARTITION OF price_history FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS price_history_default  PARTITION OF price_history DEFAULT;

CREATE INDEX IF NOT EXISTS idx_price_history_product_id       ON price_history(product_id);
CREATE INDEX IF NOT EXISTS idx_price_history_recorded_at      ON price_history(recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_price_history_product_recorded ON price_history(product_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_price_history_price            ON price_history(price);

-- Função para criação automática de partição futura (chamar via pg_cron ou FastAPI)
CREATE OR REPLACE FUNCTION fn_create_price_history_partition()
RETURNS VOID AS $$
DECLARE
    partition_date DATE := DATE_TRUNC('month', NOW() + INTERVAL '2 months');
    partition_name TEXT := 'price_history_y' || TO_CHAR(partition_date, 'YYYY') || 'm' || TO_CHAR(partition_date, 'MM');
    start_date TEXT := TO_CHAR(partition_date, 'YYYY-MM-DD');
    end_date   TEXT := TO_CHAR(partition_date + INTERVAL '1 month', 'YYYY-MM-DD');
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE FORMAT(
            'CREATE TABLE %I PARTITION OF price_history FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- 7. scored_offers
-- =============================================================================
CREATE TABLE IF NOT EXISTS scored_offers (
    id              UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    product_id      UUID        NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    rule_score      INTEGER     NOT NULL CHECK (rule_score BETWEEN 0 AND 100),
    final_score     INTEGER     NOT NULL CHECK (final_score BETWEEN 0 AND 100),
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'approved', 'rejected')),
    scored_at       TIMESTAMPTZ DEFAULT NOW(),
    queue_priority  INTEGER     DEFAULT 0,
    score_override  INTEGER,
    admin_notes     TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    -- NOTA (P15): UNIQUE em product_id é intencional — o sistema faz upsert de score.
    -- Histórico de scores não é mantido por decisão de negócio.
    CONSTRAINT scored_offers_product_id_unique UNIQUE (product_id)
);

CREATE TRIGGER trigger_scored_offers_updated_at
    BEFORE UPDATE ON scored_offers
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

CREATE INDEX IF NOT EXISTS idx_scored_offers_product_id     ON scored_offers(product_id);
CREATE INDEX IF NOT EXISTS idx_scored_offers_scored_at      ON scored_offers(scored_at DESC);
CREATE INDEX IF NOT EXISTS idx_scored_offers_status         ON scored_offers(status);
CREATE INDEX IF NOT EXISTS idx_scored_offers_final_score    ON scored_offers(final_score DESC);
CREATE INDEX IF NOT EXISTS idx_scored_offers_queue_priority ON scored_offers(queue_priority DESC);

-- =============================================================================
-- 8. sent_offers
-- =============================================================================
CREATE TABLE IF NOT EXISTS sent_offers (
    id                  UUID    DEFAULT uuid_generate_v4() PRIMARY KEY,
    scored_offer_id     UUID    NOT NULL REFERENCES scored_offers(id) ON DELETE CASCADE,
    user_id             UUID    REFERENCES users(id),
    channel             TEXT    NOT NULL CHECK (channel IN ('telegram', 'whatsapp')),
    sent_at             TIMESTAMPTZ DEFAULT NOW(),
    triggered_by        TEXT    DEFAULT 'auto'
);

CREATE INDEX IF NOT EXISTS idx_sent_offers_scored_offer_id ON sent_offers(scored_offer_id);
CREATE INDEX IF NOT EXISTS idx_sent_offers_sent_at         ON sent_offers(sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_sent_offers_channel         ON sent_offers(channel);
CREATE INDEX IF NOT EXISTS idx_sent_offers_user_id         ON sent_offers(user_id);

-- =============================================================================
-- 9. system_logs
-- =============================================================================
CREATE TABLE IF NOT EXISTS system_logs (
    id          UUID    DEFAULT uuid_generate_v4() PRIMARY KEY,
    event_type  TEXT    NOT NULL,
    details     JSONB   DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_system_logs_event_type  ON system_logs(event_type);
CREATE INDEX IF NOT EXISTS idx_system_logs_created_at  ON system_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_logs_details_gin ON system_logs USING gin(details);

-- =============================================================================
-- 10. users
-- =============================================================================
CREATE TABLE IF NOT EXISTS users (
    id              UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    name            TEXT        NOT NULL,
    affiliate_tag   TEXT        NOT NULL,
    email           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    deleted_at      TIMESTAMPTZ,
    CONSTRAINT uq_users_affiliate_tag UNIQUE (affiliate_tag)  -- P01
);

CREATE TRIGGER trigger_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- idx_users_affiliate_tag não criado (P01): o UNIQUE acima cria seu próprio índice
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email  ON users(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_deleted_at    ON users(deleted_at) WHERE deleted_at IS NOT NULL;

-- =============================================================================
-- 11. user_secrets
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_secrets (
    id          UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id     UUID        NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    ml_cookies  JSONB,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER trigger_user_secrets_updated_at
    BEFORE UPDATE ON user_secrets
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- =============================================================================
-- 12. affiliate_links
-- =============================================================================
CREATE TABLE IF NOT EXISTS affiliate_links (
    id              UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    product_id      UUID        NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    user_id         UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    short_url       TEXT        NOT NULL,
    long_url        TEXT,
    ml_link_id      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (product_id, user_id),
    CONSTRAINT uq_affiliate_links_short_url UNIQUE (short_url)  -- P14
);

CREATE TRIGGER trigger_affiliate_links_updated_at
    BEFORE UPDATE ON affiliate_links
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

CREATE INDEX IF NOT EXISTS idx_affiliate_links_product_id ON affiliate_links(product_id);
CREATE INDEX IF NOT EXISTS idx_affiliate_links_user_id    ON affiliate_links(user_id);

-- =============================================================================
-- 13. title_examples
-- =============================================================================
CREATE TABLE IF NOT EXISTS title_examples (
    id              UUID          DEFAULT uuid_generate_v4() PRIMARY KEY,
    scored_offer_id UUID          REFERENCES scored_offers(id) ON DELETE SET NULL,
    category_id     UUID          REFERENCES categories(id),
    generated_title TEXT          NOT NULL,
    final_title     TEXT          NOT NULL,
    action          TEXT          NOT NULL CHECK (action IN ('approved', 'edited', 'timeout')),
    created_at      TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_title_examples_action     ON title_examples(action);
CREATE INDEX IF NOT EXISTS idx_title_examples_created_at ON title_examples(created_at DESC);

-- =============================================================================
-- 14. admin_settings
-- =============================================================================
CREATE TABLE IF NOT EXISTS admin_settings (
    key         TEXT PRIMARY KEY,
    value       JSONB NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TRIGGER trigger_admin_settings_updated_at
    BEFORE UPDATE ON admin_settings
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

ALTER TABLE admin_settings ENABLE ROW LEVEL SECURITY;

CREATE POLICY "admin_settings_public_read"
    ON admin_settings FOR SELECT USING (true);

CREATE POLICY "admin_settings_service_write"
    ON admin_settings FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- =============================================================================
-- Row Level Security (RLS)
-- =============================================================================
ALTER TABLE badges          ENABLE ROW LEVEL SECURITY;
ALTER TABLE categories      ENABLE ROW LEVEL SECURITY;
ALTER TABLE marketplaces    ENABLE ROW LEVEL SECURITY;
ALTER TABLE brands          ENABLE ROW LEVEL SECURITY;
ALTER TABLE products        ENABLE ROW LEVEL SECURITY;
ALTER TABLE price_history   ENABLE ROW LEVEL SECURITY;
ALTER TABLE scored_offers   ENABLE ROW LEVEL SECURITY;
ALTER TABLE sent_offers     ENABLE ROW LEVEL SECURITY;
ALTER TABLE system_logs     ENABLE ROW LEVEL SECURITY;
ALTER TABLE users           ENABLE ROW LEVEL SECURITY;
ALTER TABLE affiliate_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE title_examples  ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_secrets    ENABLE ROW LEVEL SECURITY;

-- Lookup tables: SELECT público
CREATE POLICY "badges_public_read"             ON badges          FOR SELECT USING (true);
CREATE POLICY "badges_service_write"           ON badges          FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "categories_public_read"         ON categories      FOR SELECT USING (true);
CREATE POLICY "categories_service_write"       ON categories      FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "marketplaces_public_read"       ON marketplaces    FOR SELECT USING (true);
CREATE POLICY "marketplaces_service_write"     ON marketplaces    FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "brands_public_read"             ON brands          FOR SELECT USING (true);
CREATE POLICY "brands_service_write"           ON brands          FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "products_public_read"           ON products        FOR SELECT USING (true);
CREATE POLICY "products_service_write"         ON products        FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');

-- Tabelas restritas: somente service_role (ou owner)
CREATE POLICY "price_history_service_read"     ON price_history   FOR SELECT USING (auth.role() = 'service_role');
CREATE POLICY "price_history_service_write"    ON price_history   FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "scored_offers_service_read"     ON scored_offers   FOR SELECT USING (auth.role() = 'service_role');
CREATE POLICY "scored_offers_service_write"    ON scored_offers   FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "sent_offers_service_read"       ON sent_offers     FOR SELECT USING (auth.role() = 'service_role');
CREATE POLICY "sent_offers_service_write"      ON sent_offers     FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "system_logs_service_only"       ON system_logs     FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "users_authenticated_read"       ON users           FOR SELECT USING (auth.role() = 'service_role' OR auth.uid() = id);
CREATE POLICY "users_service_write"            ON users           FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "affiliate_links_authenticated_read" ON affiliate_links FOR SELECT USING (auth.role() = 'service_role' OR auth.uid() = user_id);
CREATE POLICY "affiliate_links_service_write"  ON affiliate_links FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "title_examples_service_read"    ON title_examples  FOR SELECT USING (auth.role() = 'service_role');
CREATE POLICY "title_examples_service_write"   ON title_examples  FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');
CREATE POLICY "user_secrets_service_only"      ON user_secrets    FOR ALL    USING (auth.role() = 'service_role') WITH CHECK (auth.role() = 'service_role');

-- =============================================================================
-- Views
-- =============================================================================

-- Ofertas aprovadas ainda não enviadas nas últimas 24h (fila de envio)
CREATE OR REPLACE VIEW vw_approved_unsent AS
SELECT
    p.id            AS product_id,
    p.ml_id,
    p.title,
    p.current_price,
    p.original_price,
    p.pix_price,
    p.discount_percent,
    p.discount_type,
    p.free_shipping,
    p.full_shipping,
    br.name         AS brand,
    p.thumbnail_url,
    p.product_url,
    p.rating_stars,
    p.rating_count,
    p.installments_without_interest,
    p.installment_count,
    p.installment_value,
    c.name          AS category,
    b.name          AS badge,
    so.id           AS scored_offer_id,
    so.final_score,
    so.scored_at,
    so.queue_priority,
    so.score_override,
    so.admin_notes
FROM scored_offers so
JOIN products p ON p.id = so.product_id
LEFT JOIN categories c ON c.id = p.category_id
LEFT JOIN badges b ON b.id = p.badge_id
LEFT JOIN brands br ON br.id = p.brand_id
WHERE so.status = 'approved'
  AND p.deleted_at IS NULL
  AND COALESCE(so.score_override, so.final_score) >= 60
  AND NOT EXISTS (
      SELECT 1 FROM sent_offers se
      WHERE se.scored_offer_id = so.id
        AND se.sent_at >= NOW() - INTERVAL '24 hours'
  )
ORDER BY so.queue_priority DESC, COALESCE(so.score_override, so.final_score) DESC;

-- Resumo das últimas 24h (KPIs do dashboard) — Materialized View (P20)
-- Atualizada a cada 15 min via fn_refresh_mv_summary() (pg_cron ou FastAPI)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_last_24h_summary AS
SELECT
    (SELECT COUNT(*) FROM products      WHERE last_seen_at >= NOW() - INTERVAL '24 hours' AND deleted_at IS NULL) AS products_scraped,
    (SELECT COUNT(*) FROM scored_offers WHERE scored_at    >= NOW() - INTERVAL '24 hours') AS offers_scored,
    (SELECT COUNT(*) FROM scored_offers WHERE scored_at    >= NOW() - INTERVAL '24 hours' AND status = 'approved') AS offers_approved,
    (SELECT COUNT(*) FROM sent_offers   WHERE sent_at      >= NOW() - INTERVAL '24 hours') AS offers_sent,
    (SELECT ROUND(AVG(final_score), 1)  FROM scored_offers WHERE scored_at >= NOW() - INTERVAL '24 hours') AS avg_score,
    (SELECT MAX(discount_percent)       FROM products      WHERE last_seen_at >= NOW() - INTERVAL '24 hours' AND deleted_at IS NULL) AS max_discount_pct;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_last_24h_summary ON mv_last_24h_summary ((1));

CREATE OR REPLACE FUNCTION fn_refresh_mv_summary()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_last_24h_summary;
END;
$$ LANGUAGE plpgsql;

-- Top 20 deals ativos nas últimas 6h
CREATE OR REPLACE VIEW vw_top_deals AS
SELECT
    p.id            AS product_id,
    p.ml_id,
    p.title,
    p.current_price,
    p.original_price,
    p.pix_price,
    p.discount_percent,
    p.discount_type,
    p.free_shipping,
    p.full_shipping,
    br.name         AS brand,
    p.thumbnail_url,
    p.product_url,
    p.installment_count,
    p.installment_value,
    c.name          AS category,
    b.name          AS badge,
    so.final_score
FROM products p
JOIN scored_offers so ON so.product_id = p.id
LEFT JOIN categories c ON c.id = p.category_id
LEFT JOIN badges b ON b.id = p.badge_id
LEFT JOIN brands br ON br.id = p.brand_id
WHERE p.last_seen_at >= NOW() - INTERVAL '6 hours'
  AND p.deleted_at IS NULL
  AND so.status = 'approved'
ORDER BY so.final_score DESC, p.discount_percent DESC
LIMIT 20;

-- =============================================================================
-- 15. scored_offer_transitions (P09)
-- Audit trail automático de mudanças de status via trigger.
-- =============================================================================
CREATE TABLE IF NOT EXISTS scored_offer_transitions (
    id                UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    scored_offer_id   UUID        NOT NULL REFERENCES scored_offers(id) ON DELETE CASCADE,
    from_status       TEXT,
    to_status         TEXT        NOT NULL CHECK (to_status IN ('pending', 'approved', 'rejected')),
    changed_by        TEXT,
    notes             TEXT,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sot_scored_offer_id ON scored_offer_transitions(scored_offer_id);
CREATE INDEX IF NOT EXISTS idx_sot_created_at      ON scored_offer_transitions(created_at DESC);

ALTER TABLE scored_offer_transitions ENABLE ROW LEVEL SECURITY;
CREATE POLICY "sot_service_only" ON scored_offer_transitions FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

CREATE OR REPLACE FUNCTION fn_scored_offer_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO scored_offer_transitions (scored_offer_id, from_status, to_status)
        VALUES (NEW.id, OLD.status, NEW.status);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_scored_offer_status_change
    AFTER UPDATE OF status ON scored_offers
    FOR EACH ROW EXECUTE FUNCTION fn_scored_offer_status_change();

-- =============================================================================
-- Automações (P18)
-- =============================================================================

-- Limpeza de logs com mais de 90 dias (chamar via pg_cron ou FastAPI diariamente)
CREATE OR REPLACE FUNCTION fn_cleanup_old_system_logs()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM system_logs WHERE created_at < NOW() - INTERVAL '90 days';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
