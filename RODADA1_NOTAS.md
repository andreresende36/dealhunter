# Rodada 1 — Notas de Revisão

## Referências no código que precisam de revisão manual

1. **`src/utils/brands.py`** — Módulo de extração de marca por texto (`extract_brand()`) continua sendo usado por `title_generator.py`, `message_formatter.py` e `score_engine.py`. Esses módulos extraem a marca do título do produto em runtime (para exibição/scoring), o que é independente da nova tabela `brands`. Verificar se há duplicação de lógica ou se faz sentido unificar.

2. **`src/distributor/sender.py:114`** — TODO sobre reativação do image enhancement. O fluxo de seleção e upload de imagem foi desativado; o sistema usa apenas `thumbnail_url` do Mercado Livre.

3. **`StorageManager.get_or_create_user()`** — Mantém `password` e `ml_cookies` na assinatura para backward compatibility com `pipeline.py`, mas os valores são silenciosamente ignorados. Limpar a assinatura e os call sites quando possível.

4. **`src/scraper/ml_scraper.py`** — Verifica se o scraper popula `discount_type` com valores válidos conforme o CHECK constraint adicionado (`'percentage'`, `'amount'`, `'coupon'`). Se o scraper retornar outros valores, o INSERT falhará.

5. **`src/scraper/ml_classifier.py`** — A função `classify_gender_with_ai()` depende de um prompt externo (`prompts/gender_user.txt`) que foi atualizado para retornar valores em inglês. Confirmar que o modelo de IA (OpenRouter/Haiku) respeita os novos valores consistentemente.

## Frontend e RLS (P11)

A migração `029_restrict_rls_policies.sql` removeu as policies `public_read` de 6 tabelas sensíveis e criou policies restritas (`service_role_only` ou `authenticated_only`). Impacto no frontend admin:

- **Tabelas afetadas**: `users`, `affiliate_links`, `sent_offers`, `scored_offers`, `title_examples`, `price_history`
- **O frontend Next.js vai quebrar** se estiver acessando essas tabelas diretamente via `supabase-js` com a `anon` key, pois o acesso anônimo foi removido.
- **Solução necessária**: Criar endpoints API (Edge Functions ou API routes) que usem a `service_role` key no backend para intermediar o acesso. Alternativamente, configurar autenticação no admin e usar policies `authenticated_only`.
- **Tabelas não afetadas pelo RLS restritivo**: `products`, `brands`, `badges`, `categories`, `marketplaces`, `admin_settings` — estas mantêm acesso público de leitura.

## Registros órfãos em `title_examples`

A migração `030_restructure_title_examples.sql` removeu as colunas `product_title`, `category` e `price`. Registros existentes que tinham `scored_offer_id = NULL` perdem a referência ao produto original (não é possível derivar `product_title` via JOIN).

- **Ação recomendada**: Executar query para identificar registros órfãos:
  ```sql
  SELECT id, generated_title, final_title
  FROM title_examples
  WHERE scored_offer_id IS NULL;
  ```
- Se existirem, decidir se devem ser deletados ou se `scored_offer_id` pode ser preenchido retroativamente.

## Módulos de image enhancement preservados (P03b)

Os seguintes módulos foram **preservados no filesystem** conforme instruído, mas não são mais referenciados pelo código ativo:

- `src/image/image_storage.py` — Upload de imagens para Supabase Storage
- `src/image/lifestyle_generator.py` — Geração de imagens lifestyle via IA
- `src/utils/password.py` — Utilitário de hash de senha (não mais importado)

As colunas `image_status` e `enhanced_image_url` foram removidas do schema. Para reativar o image enhancement no futuro, será necessário:
1. Adicionar as colunas de volta (nova migração)
2. Reconectar os métodos no `supabase_client.py` e `sqlite_fallback.py`
3. Reintegrar o fluxo no `sender.py`
