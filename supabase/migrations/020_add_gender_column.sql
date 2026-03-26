-- Migration 020: Adiciona coluna gender à tabela products
-- Armazena o gênero alvo do produto: Masculino, Feminino, Unissex ou Sem gênero.
-- Classificado por keyword rápida ou IA (gemini-2.5-flash) apenas para categorias
-- relevantes (Calçados, Esportes, Joias, Beleza, Brinquedos).

ALTER TABLE products
ADD COLUMN IF NOT EXISTS gender TEXT DEFAULT 'Sem gênero';
