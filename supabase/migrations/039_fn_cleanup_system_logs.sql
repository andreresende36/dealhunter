-- Migration 039: P18 — Retenção de 90 dias em system_logs

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

-- Agendar limpeza diária às 03:00 via pg_cron (se disponível)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
        PERFORM cron.schedule(
            'cleanup-system-logs',
            '0 3 * * *',
            $$SELECT fn_cleanup_old_system_logs()$$
        );
    END IF;
END;
$$;

-- Se pg_cron não estiver disponível, chamar via task agendada no FastAPI:
--   SELECT fn_cleanup_old_system_logs()
