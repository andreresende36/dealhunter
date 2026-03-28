-- Migration 040: P20 — Materializar vw_last_24h_summary → mv_last_24h_summary

DROP VIEW IF EXISTS vw_last_24h_summary;

CREATE MATERIALIZED VIEW mv_last_24h_summary AS
SELECT
    (SELECT COUNT(*) FROM products      WHERE last_seen_at >= NOW() - INTERVAL '24 hours' AND deleted_at IS NULL) AS products_scraped,
    (SELECT COUNT(*) FROM scored_offers WHERE scored_at    >= NOW() - INTERVAL '24 hours') AS offers_scored,
    (SELECT COUNT(*) FROM scored_offers WHERE scored_at    >= NOW() - INTERVAL '24 hours' AND status = 'approved') AS offers_approved,
    (SELECT COUNT(*) FROM sent_offers   WHERE sent_at      >= NOW() - INTERVAL '24 hours') AS offers_sent,
    (SELECT ROUND(AVG(final_score), 1)  FROM scored_offers WHERE scored_at >= NOW() - INTERVAL '24 hours') AS avg_score,
    (SELECT MAX(discount_percent)       FROM products      WHERE last_seen_at >= NOW() - INTERVAL '24 hours' AND deleted_at IS NULL) AS max_discount_pct;

-- Índice único necessário para REFRESH CONCURRENTLY
CREATE UNIQUE INDEX ON mv_last_24h_summary ((1));

-- Função para refresh
CREATE OR REPLACE FUNCTION fn_refresh_mv_summary()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_last_24h_summary;
END;
$$ LANGUAGE plpgsql;

-- Agendar refresh a cada 15 minutos via pg_cron (se disponível)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.schedule(
            'refresh-mv-summary',
            '*/15 * * * *',
            $$SELECT fn_refresh_mv_summary()$$
        );
    END IF;
END;
$$;

-- Se pg_cron não estiver disponível, chamar fn_refresh_mv_summary() via FastAPI
-- ao final de cada ciclo de scraping para refresh imediato.
