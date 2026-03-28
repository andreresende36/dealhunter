-- =============================================================================
-- Migration 027: P10 — Remover password_hash, mover ml_cookies para user_secrets
-- =============================================================================

-- Criar tabela segura
CREATE TABLE IF NOT EXISTS user_secrets (
    id          UUID        DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id     UUID        NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    ml_cookies  JSONB,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trigger_user_secrets_updated_at ON user_secrets;
CREATE TRIGGER trigger_user_secrets_updated_at
    BEFORE UPDATE ON user_secrets
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- RLS restritivo — somente service_role
ALTER TABLE user_secrets ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname = 'user_secrets_service_only' AND tablename = 'user_secrets') THEN
        CREATE POLICY "user_secrets_service_only" ON user_secrets
            FOR ALL USING (auth.role() = 'service_role')
            WITH CHECK (auth.role() = 'service_role');
    END IF;
END$$;

-- Migrar dados existentes
INSERT INTO user_secrets (user_id, ml_cookies)
SELECT id, ml_cookies FROM users WHERE ml_cookies IS NOT NULL
ON CONFLICT (user_id) DO NOTHING;

-- Limpar tabela users
ALTER TABLE users DROP COLUMN IF EXISTS ml_cookies;
ALTER TABLE users DROP COLUMN IF EXISTS password_hash;
