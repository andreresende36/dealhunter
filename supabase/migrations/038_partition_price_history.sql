-- Migration 038: P17 — Particionamento mensal de price_history
-- Volume estimado: ~10M registros/mês. Particionamento por RANGE(recorded_at).

-- 1. Criar tabela particionada
CREATE TABLE price_history_partitioned (
    id              UUID          DEFAULT uuid_generate_v4(),
    product_id      UUID          NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    price           DECIMAL(10,2) NOT NULL,
    original_price  DECIMAL(10,2),
    pix_price       DECIMAL(10,2),
    recorded_at     TIMESTAMPTZ   DEFAULT NOW(),
    PRIMARY KEY (id, recorded_at)
) PARTITION BY RANGE (recorded_at);

-- 2. Criar partições: mês atual (2026-03) + 3 futuros + default
CREATE TABLE IF NOT EXISTS price_history_y2026m03 PARTITION OF price_history_partitioned
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS price_history_y2026m04 PARTITION OF price_history_partitioned
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS price_history_y2026m05 PARTITION OF price_history_partitioned
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS price_history_y2026m06 PARTITION OF price_history_partitioned
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS price_history_default PARTITION OF price_history_partitioned DEFAULT;

-- 3. Migrar dados existentes
INSERT INTO price_history_partitioned (id, product_id, price, original_price, pix_price, recorded_at)
SELECT id, product_id, price, original_price, pix_price, recorded_at
  FROM price_history;

-- 4. Swap de nomes
ALTER TABLE price_history RENAME TO price_history_old;
ALTER TABLE price_history_partitioned RENAME TO price_history;

-- 5. Recriar índices na tabela renomeada
CREATE INDEX IF NOT EXISTS idx_price_history_product_id       ON price_history(product_id);
CREATE INDEX IF NOT EXISTS idx_price_history_recorded_at      ON price_history(recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_price_history_product_recorded ON price_history(product_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_price_history_price            ON price_history(price);

-- 6. RLS (as policies não são transferidas no rename)
ALTER TABLE price_history ENABLE ROW LEVEL SECURITY;

CREATE POLICY "price_history_service_read" ON price_history FOR SELECT
    USING (auth.role() = 'service_role');
CREATE POLICY "price_history_service_write" ON price_history FOR ALL
    USING (auth.role() = 'service_role')
    WITH CHECK (auth.role() = 'service_role');

-- 7. Função para criação automática de partição futura
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

-- 8. Agendar criação mensal de partição via pg_cron (se disponível)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.schedule(
            'create-price-history-partition',
            '0 0 1 * *',
            $$SELECT fn_create_price_history_partition()$$
        );
    END IF;
END;
$$;

-- NOTA: Após confirmar que a migração está correta e todos os dados migraram,
-- dropar a tabela antiga:
--   DROP TABLE IF EXISTS price_history_old;
-- Não é feito automaticamente para permitir rollback de dados se necessário.
