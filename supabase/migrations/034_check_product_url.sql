-- Migration 034: P12 — CHECK constraint on products.product_url
-- Limpa registros com product_url vazio usando thumbnail_url como fallback.

UPDATE products
   SET product_url = thumbnail_url
 WHERE product_url = ''
   AND thumbnail_url IS NOT NULL
   AND thumbnail_url <> '';

-- Remover registros sem qualquer URL válida (não deveriam existir, mas garante constraint)
-- (não deleta, apenas documenta: product_url NOT NULL DEFAULT '' já evita NULL)

ALTER TABLE products
    ADD CONSTRAINT chk_products_product_url CHECK (product_url <> '');
