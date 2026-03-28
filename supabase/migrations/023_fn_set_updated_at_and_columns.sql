-- =============================================================================
-- Migration 023: P08 — Função fn_set_updated_at() + updated_at em 7 tabelas
--
-- Cria função reutilizável para auto-update de updated_at via trigger.
-- Adiciona coluna updated_at e trigger nas tabelas que não a possuem.
-- admin_settings já tem updated_at, só precisa do trigger.
-- =============================================================================

-- Função reutilizável
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- users
ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
DROP TRIGGER IF EXISTS trigger_users_updated_at ON users;
CREATE TRIGGER trigger_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- scored_offers
ALTER TABLE scored_offers ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
DROP TRIGGER IF EXISTS trigger_scored_offers_updated_at ON scored_offers;
CREATE TRIGGER trigger_scored_offers_updated_at
    BEFORE UPDATE ON scored_offers
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- affiliate_links
ALTER TABLE affiliate_links ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
DROP TRIGGER IF EXISTS trigger_affiliate_links_updated_at ON affiliate_links;
CREATE TRIGGER trigger_affiliate_links_updated_at
    BEFORE UPDATE ON affiliate_links
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- badges
ALTER TABLE badges ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
DROP TRIGGER IF EXISTS trigger_badges_updated_at ON badges;
CREATE TRIGGER trigger_badges_updated_at
    BEFORE UPDATE ON badges
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- categories
ALTER TABLE categories ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
DROP TRIGGER IF EXISTS trigger_categories_updated_at ON categories;
CREATE TRIGGER trigger_categories_updated_at
    BEFORE UPDATE ON categories
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- marketplaces
ALTER TABLE marketplaces ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
DROP TRIGGER IF EXISTS trigger_marketplaces_updated_at ON marketplaces;
CREATE TRIGGER trigger_marketplaces_updated_at
    BEFORE UPDATE ON marketplaces
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- admin_settings (já tem updated_at, só precisa do trigger)
DROP TRIGGER IF EXISTS trigger_admin_settings_updated_at ON admin_settings;
CREATE TRIGGER trigger_admin_settings_updated_at
    BEFORE UPDATE ON admin_settings
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
