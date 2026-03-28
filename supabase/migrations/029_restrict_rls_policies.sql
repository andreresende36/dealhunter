-- =============================================================================
-- Migration 029: P11 — Restringir RLS
--
-- Restringe SELECT em tabelas sensíveis.
-- Mantém SELECT público em lookup tables: badges, categories, marketplaces,
-- brands, products, admin_settings.
-- =============================================================================

-- users: só o próprio usuário ou service_role
DROP POLICY IF EXISTS "users_public_read" ON users;
CREATE POLICY "users_authenticated_read" ON users FOR SELECT
    USING (auth.role() = 'service_role' OR auth.uid() = id);

-- affiliate_links: só o próprio dono ou service_role
DROP POLICY IF EXISTS "affiliate_links_public_read" ON affiliate_links;
CREATE POLICY "affiliate_links_authenticated_read" ON affiliate_links FOR SELECT
    USING (auth.role() = 'service_role' OR auth.uid() = user_id);

-- sent_offers: somente service_role
DROP POLICY IF EXISTS "sent_offers_public_read" ON sent_offers;
CREATE POLICY "sent_offers_service_read" ON sent_offers FOR SELECT
    USING (auth.role() = 'service_role');

-- scored_offers: somente service_role
DROP POLICY IF EXISTS "scored_offers_public_read" ON scored_offers;
CREATE POLICY "scored_offers_service_read" ON scored_offers FOR SELECT
    USING (auth.role() = 'service_role');

-- title_examples: somente service_role
DROP POLICY IF EXISTS "title_examples_public_read" ON title_examples;
CREATE POLICY "title_examples_service_read" ON title_examples FOR SELECT
    USING (auth.role() = 'service_role');

-- price_history: somente service_role
DROP POLICY IF EXISTS "price_history_public_read" ON price_history;
CREATE POLICY "price_history_service_read" ON price_history FOR SELECT
    USING (auth.role() = 'service_role');
