ALTER TABLE products ADD COLUMN IF NOT EXISTS installments_without_interest BOOLEAN DEFAULT FALSE;
