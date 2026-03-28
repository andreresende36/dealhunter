-- Migration 037: P09 — Tabela de transições de status de scored_offers + trigger automático

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

-- Trigger para capturar automaticamente toda mudança de status
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

DROP TRIGGER IF EXISTS trigger_scored_offer_status_change ON scored_offers;
CREATE TRIGGER trigger_scored_offer_status_change
    AFTER UPDATE OF status ON scored_offers
    FOR EACH ROW EXECUTE FUNCTION fn_scored_offer_status_change();
