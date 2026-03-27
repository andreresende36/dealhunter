"""
Crivo — Supabase Client
Interface assíncrona com o PostgreSQL via Supabase.

Tabelas gerenciadas:
  products, price_history, scored_offers, sent_offers, system_logs

Uso como context manager (recomendado):
    async with SupabaseClient() as db:
        product_id = await db.upsert_product(scraped_product)
        await db.add_price_history(product_id, price=299.90, original_price=599.90)

Uso direto:
    db = SupabaseClient()
    await db.connect()
    ...
    await db.close()
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import structlog
from supabase import AsyncClient, acreate_client

from src.config import settings
from src.scraper.base_scraper import ScrapedProduct
from .exceptions import SupabaseError

logger = structlog.get_logger(__name__)

_FIELDS_ID_NAME = "id, name"


class SupabaseClient:
    """
    Cliente assíncrono para o Supabase (PostgreSQL).

    Métodos levantam SupabaseError em caso de falha para que o
    StorageManager possa decidir o que fazer (fallback, retry, etc.).
    """

    def __init__(self) -> None:
        self.cfg = settings.supabase
        self._client: Optional[AsyncClient] = None

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def __aenter__(self) -> SupabaseClient:
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def connect(self) -> None:
        """Abre a conexão com o Supabase usando a service_role key."""
        self._client = await acreate_client(
            self.cfg.url,
            self.cfg.service_role_key,  # service_role bypassa RLS
        )
        logger.info("supabase_connected", url=self.cfg.url)

    def close(self) -> None:
        """Libera a conexão."""
        self._client = None
        logger.info("supabase_closed")

    @property
    def _db(self) -> AsyncClient:
        """Retorna o cliente conectado ou levanta RuntimeError."""
        if self._client is None:
            raise RuntimeError(
                "SupabaseClient não conectado. "
                "Chame connect() ou use como context manager."
            )
        return self._client

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def ping(self) -> bool:
        """
        Verifica se o Supabase está acessível e respondendo.
        Retorna True se OK, False em qualquer falha.
        """
        try:
            # Query mínima: busca 0 linhas de products
            await self._db.table("products").select("id").limit(0).execute()
            logger.debug("supabase_ping_ok")
            return True
        except Exception as exc:
            logger.warning("supabase_ping_failed", error=str(exc))
            return False

    # ------------------------------------------------------------------
    # badges
    # ------------------------------------------------------------------

    async def get_or_create_badge(self, name: str) -> Optional[str]:
        """Retorna o ID do badge pelo nome. Cria se não existir."""
        if not name:
            return None
        try:
            result = (
                await self._db.table("badges")
                .select("id")
                .eq("name", name)
                .limit(1)
                .execute()
            )
            if result.data:
                return result.data[0]["id"]
            # Não existe, cria
            result = await self._db.table("badges").insert({"name": name}).execute()
            if result.data:
                badge_id: str = result.data[0]["id"]
                logger.debug("supabase_badge_created", name=name, badge_id=badge_id)
                return badge_id
            return None
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_or_create_badge") from exc

    # ------------------------------------------------------------------
    # categories
    # ------------------------------------------------------------------

    async def get_or_create_category(self, name: str) -> Optional[str]:
        """Retorna o ID da categoria pelo nome. Cria se não existir."""
        if not name:
            return None
        try:
            result = (
                await self._db.table("categories")
                .select("id")
                .eq("name", name)
                .limit(1)
                .execute()
            )
            if result.data:
                return result.data[0]["id"]
            result = await self._db.table("categories").insert({"name": name}).execute()
            if result.data:
                cat_id: str = result.data[0]["id"]
                logger.debug("supabase_category_created", name=name, category_id=cat_id)
                return cat_id
            return None
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_or_create_category") from exc

    async def get_all_badges(self) -> dict[str, str]:
        """Retorna todos os badges como {nome: uuid}."""
        try:
            result = await self._db.table("badges").select(_FIELDS_ID_NAME).execute()
            return {row["name"]: row["id"] for row in (result.data or [])}
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_all_badges") from exc

    async def get_all_categories(self) -> dict[str, str]:
        """Retorna todas as categorias como {nome: uuid}."""
        try:
            result = await self._db.table("categories").select(_FIELDS_ID_NAME).execute()
            return {row["name"]: row["id"] for row in (result.data or [])}
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_all_categories") from exc

    async def get_or_create_marketplace(self, name: str) -> Optional[str]:
        """Retorna o ID do marketplace pelo nome. Cria se não existir."""
        if not name:
            return None
        try:
            result = (
                await self._db.table("marketplaces")
                .select("id")
                .eq("name", name)
                .limit(1)
                .execute()
            )
            if result.data:
                return result.data[0]["id"]
            result = await self._db.table("marketplaces").insert({"name": name}).execute()
            if result.data:
                mp_id: str = result.data[0]["id"]
                logger.debug("supabase_marketplace_created", name=name, marketplace_id=mp_id)
                return mp_id
            return None
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_or_create_marketplace") from exc

    async def get_all_marketplaces(self) -> dict[str, str]:
        """Retorna todos os marketplaces como {nome: uuid}."""
        try:
            result = await self._db.table("marketplaces").select(_FIELDS_ID_NAME).execute()
            return {row["name"]: row["id"] for row in (result.data or [])}
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_all_marketplaces") from exc

    # ------------------------------------------------------------------
    # products
    # ------------------------------------------------------------------

    async def upsert_product(
        self,
        product: ScrapedProduct,
        badge_id: str | None = None,
        category_id: str | None = None,
        marketplace_id: str | None = None,
    ) -> Optional[str]:
        """
        Insere ou atualiza um produto pelo ml_id.

        - Na inserção: define first_seen_at = last_seen_at = NOW()
        - Na atualização: preserva first_seen_at via trigger no banco,
          atualiza apenas os campos de preço e disponibilidade.

        Retorna o UUID (id) do produto no banco, ou None em caso de erro.
        """
        data = self._product_to_row(
            product,
            badge_id=badge_id,
            category_id=category_id,
            marketplace_id=marketplace_id,
        )
        try:
            result = (
                await self._db.table("products")
                .upsert(data, on_conflict="ml_id")
                .execute()
            )
            if not result.data:
                raise SupabaseError(
                    "upsert retornou sem dados",
                    operation="upsert_product",
                    ml_id=product.ml_id,
                )

            product_id: str = result.data[0]["id"]
            logger.debug("product_upserted", ml_id=product.ml_id, product_id=product_id)
            return product_id

        except SupabaseError:
            raise
        except Exception as exc:
            raise SupabaseError(
                str(exc), operation="upsert_product", ml_id=product.ml_id
            ) from exc

    async def check_duplicate(self, ml_id: str) -> bool:
        """
        Verifica se um produto já existe no banco.
        Retorna True se existe, False se não existe.
        Levanta SupabaseError em caso de falha de comunicação.
        """
        try:
            result = (
                await self._db.table("products")
                .select("id")
                .eq("ml_id", ml_id)
                .limit(1)
                .execute()
            )
            return len(result.data) > 0
        except Exception as exc:
            raise SupabaseError(
                str(exc), operation="check_duplicate", ml_id=ml_id
            ) from exc

    async def check_duplicates_batch(self, ml_ids: list[str]) -> set[str]:
        """
        Verifica quais ml_ids já existem no banco em UMA única query.
        Retorna set dos ml_ids que já existem.
        """
        if not ml_ids:
            return set()
        try:
            result = (
                await self._db.table("products")
                .select("ml_id")
                .in_("ml_id", ml_ids)
                .execute()
            )
            return {row["ml_id"] for row in result.data}
        except Exception as exc:
            raise SupabaseError(str(exc), operation="check_duplicates_batch") from exc

    async def upsert_products_batch(
        self,
        products: list["ScrapedProduct"],
        badge_ids: dict[str, str | None] | None = None,
        category_ids: dict[str, str | None] | None = None,
        marketplace_ids: dict[str, str | None] | None = None,
    ) -> dict[str, str]:
        """
        Upsert de múltiplos produtos em UMA única chamada.
        Retorna dict mapeando ml_id → UUID do produto.
        Deduplicates by ml_id (keeps last occurrence).
        """
        if not products:
            return {}
        badges_map = badge_ids or {}
        cats_map = category_ids or {}
        mps_map = marketplace_ids or {}
        # Deduplica por ml_id (Supabase rejeita ON CONFLICT com dupes na mesma batch)
        seen: dict[str, "ScrapedProduct"] = {}
        for p in products:
            seen[p.ml_id] = p
        unique_products = list(seen.values())
        rows = [
            self._product_to_row(
                p,
                badge_id=badges_map.get(p.ml_id),
                category_id=cats_map.get(p.ml_id),
                marketplace_id=mps_map.get(p.ml_id),
            )
            for p in unique_products
        ]
        try:
            result = (
                await self._db.table("products")
                .upsert(rows, on_conflict="ml_id")
                .execute()
            )
            if not result.data:
                raise SupabaseError(
                    "batch upsert retornou sem dados",
                    operation="upsert_products_batch",
                )
            return {row["ml_id"]: row["id"] for row in result.data}
        except SupabaseError:
            raise
        except Exception as exc:
            raise SupabaseError(str(exc), operation="upsert_products_batch") from exc

    async def add_price_history_batch(self, entries: list[dict]) -> None:
        """
        Insere múltiplas entradas de histórico de preço em UMA única chamada.
        entries: lista de dicts com keys: product_id, price, original_price.
        """
        if not entries:
            return
        now = datetime.now(tz=timezone.utc).isoformat()
        rows = [
            {
                "product_id": e["product_id"],
                "price": e["price"],
                "original_price": e["original_price"],
                "pix_price": e.get("pix_price"),
                "recorded_at": now,
            }
            for e in entries
        ]
        try:
            await self._db.table("price_history").insert(rows).execute()
            logger.debug("price_history_batch_added", count=len(rows))
        except Exception as exc:
            raise SupabaseError(str(exc), operation="add_price_history_batch") from exc

    async def get_product_id(self, ml_id: str) -> Optional[str]:
        """Retorna o UUID interno de um produto pelo ml_id."""
        try:
            result = (
                await self._db.table("products")
                .select("id")
                .eq("ml_id", ml_id)
                .limit(1)
                .execute()
            )
            return result.data[0]["id"] if result.data else None
        except Exception as exc:
            raise SupabaseError(
                str(exc), operation="get_product_id", ml_id=ml_id
            ) from exc

    # ------------------------------------------------------------------
    # price_history
    # ------------------------------------------------------------------

    async def add_price_history(
        self,
        product_id: str,
        price: float,
        original_price: Optional[float] = None,
        pix_price: Optional[float] = None,
    ) -> bool:
        """
        Registra o preço atual de um produto no histórico.
        Deve ser chamado a cada vez que o scraper coleta o produto.

        Args:
            product_id: UUID interno do produto (retornado por upsert_product)
            price: Preço atual em BRL
            original_price: Preço original antes do desconto (pode ser None)
            pix_price: Preço com desconto Pix/boleto (pode ser None)
        """
        data = {
            "product_id": product_id,
            "price": price,
            "original_price": original_price,
            "pix_price": pix_price,
            "recorded_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            await self._db.table("price_history").insert(data).execute()
            logger.debug("price_history_added", product_id=product_id, price=price)
            return True
        except Exception as exc:
            raise SupabaseError(str(exc), operation="add_price_history") from exc

    async def get_price_history(self, product_id: str, days: int = 30) -> list[dict]:
        """
        Retorna o histórico de preços dos últimos N dias para um produto.

        Útil para o FakeDiscountDetector calcular o preço médio histórico.

        Returns:
            Lista de dicts com campos: price, original_price, recorded_at
            Ordenada do mais antigo para o mais recente.
        """
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(days=days)).isoformat()
        try:
            result = (
                await self._db.table("price_history")
                .select("price, original_price, recorded_at")
                .eq("product_id", product_id)
                .gte("recorded_at", cutoff)
                .order("recorded_at", desc=False)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_price_history") from exc

    # ------------------------------------------------------------------
    # scored_offers
    # ------------------------------------------------------------------

    async def save_scored_offer(
        self,
        product_id: str,
        rule_score: int,
        final_score: int,
        status: str,
    ) -> Optional[str]:
        """
        Salva o resultado da análise de uma oferta.

        Args:
            product_id: UUID interno do produto
            rule_score: Pontuação calculada pelo Score Engine (0-100)
            final_score: Pontuação final
            status: "approved" | "rejected" | "pending"

        Returns:
            UUID do scored_offer criado, ou None em caso de erro.
        """
        data = {
            "product_id": product_id,
            "rule_score": rule_score,
            "final_score": final_score,
            "status": status,
            "scored_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            result = await self._db.table("scored_offers").insert(data).execute()
            if not result.data:
                return None
            scored_offer_id: str = result.data[0]["id"]
            logger.info(
                "scored_offer_saved",
                product_id=product_id,
                final_score=final_score,
                status=status,
            )
            return scored_offer_id
        except Exception as exc:
            raise SupabaseError(str(exc), operation="save_scored_offer") from exc

    async def save_scored_offers_batch(self, entries: list[dict]) -> list[str]:
        """
        Insere múltiplas scored_offers em UMA única chamada.

        entries: lista de dicts com keys:
            product_id, rule_score, final_score, status.

        Returns:
            Lista de UUIDs gerados pelo banco.
        """
        if not entries:
            return []
        now = datetime.now(tz=timezone.utc).isoformat()
        # Deduplica por product_id — último registro prevalece
        deduped: dict[str, dict] = {}
        for e in entries:
            deduped[e["product_id"]] = {
                "product_id": e["product_id"],
                "rule_score": e["rule_score"],
                "final_score": e["final_score"],
                "status": e["status"],
                "scored_at": now,
            }
        rows = list(deduped.values())
        try:
            result = (
                await self._db.table("scored_offers")
                .upsert(rows, on_conflict="product_id")
                .execute()
            )
            if not result.data:
                raise SupabaseError(
                    "batch insert retornou sem dados",
                    operation="save_scored_offers_batch",
                )
            ids = [row["id"] for row in result.data]
            logger.debug("scored_offers_batch_saved", count=len(ids))
            return ids
        except SupabaseError:
            raise
        except Exception as exc:
            raise SupabaseError(str(exc), operation="save_scored_offers_batch") from exc

    async def get_pending_scored_offers(self, limit: int = 50) -> list[dict]:
        """
        Retorna ofertas aprovadas ainda não enviadas.
        Usa a view vw_approved_unsent para eficiência.
        """
        try:
            result = (
                await self._db.table("vw_approved_unsent")
                .select("*")
                .limit(limit)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            raise SupabaseError(
                str(exc), operation="get_pending_scored_offers"
            ) from exc

    async def revert_to_pending(self, scored_offer_id: str) -> bool:
        """Marca oferta como pendente (fallback para timeouts)."""
        try:
            await (
                self._db.table("scored_offers")
                .update({"status": "pending"})
                .eq("id", scored_offer_id)
                .execute()
            )
            return True
        except Exception as exc:
            raise SupabaseError(str(exc), operation="revert_to_pending") from exc

    # ------------------------------------------------------------------
    # sent_offers
    # ------------------------------------------------------------------

    async def has_recent_sends(self, hours: int = 24) -> bool:
        """Verifica rapidamente se há ALGUM envio nas últimas N horas (1 query)."""
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).isoformat()
        try:
            result = (
                await self._db.table("sent_offers")
                .select("id")
                .gte("sent_at", cutoff)
                .limit(1)
                .execute()
            )
            return len(result.data) > 0
        except Exception as exc:
            raise SupabaseError(str(exc), operation="has_recent_sends") from exc

    async def get_recently_sent_ids(self, hours: int = 24) -> set[str]:
        """
        Retorna o conjunto de ml_ids enviados nas últimas N horas (1 query batch).
        Substitui N chamadas a was_recently_sent() por uma única query.
        """
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).isoformat()
        try:
            result = (
                await self._db.table("sent_offers")
                .select("scored_offers!inner(products!inner(ml_id))")
                .gte("sent_at", cutoff)
                .execute()
            )
            return {
                row["scored_offers"]["products"]["ml_id"]
                for row in result.data
                if row.get("scored_offers", {}).get("products", {}).get("ml_id")
            }
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_recently_sent_ids") from exc

    async def mark_as_sent(
        self,
        scored_offer_id: str,
        channel: str,
    ) -> bool:
        """
        Registra o envio de uma oferta para um canal específico.

        Args:
            scored_offer_id: UUID do scored_offer enviado
            channel: "telegram" ou "whatsapp"

        Returns:
            True se registrado com sucesso, False em caso de erro.
        """
        data = {
            "scored_offer_id": scored_offer_id,
            "channel": channel,
            "sent_at": datetime.now(tz=timezone.utc).isoformat(),
            "clicks": 0,
        }
        try:
            await self._db.table("sent_offers").insert(data).execute()
            logger.debug(
                "offer_marked_sent",
                scored_offer_id=scored_offer_id,
                channel=channel,
            )
            return True
        except Exception as exc:
            raise SupabaseError(str(exc), operation="mark_as_sent") from exc

    async def was_recently_sent(self, ml_id: str, hours: int = 24) -> bool:
        """
        Verifica se um produto (pelo ml_id) foi enviado nas últimas N horas.
        Faz join entre sent_offers → scored_offers → products.
        """
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).isoformat()
        try:
            # Busca pelo product_id nas scored_offers enviadas
            product_id = await self.get_product_id(ml_id)
            if not product_id:
                return False

            result = (
                await self._db.table("sent_offers")
                .select("id, scored_offers!inner(product_id)")
                .eq("scored_offers.product_id", product_id)
                .gte("sent_at", cutoff)
                .limit(1)
                .execute()
            )
            return len(result.data) > 0
        except Exception as exc:
            raise SupabaseError(
                str(exc), operation="was_recently_sent", ml_id=ml_id
            ) from exc

    async def update_click_count(
        self, scored_offer_id: str, channel: str, clicks: int
    ) -> bool:
        """Atualiza o contador de cliques de um envio."""
        try:
            await (
                self._db.table("sent_offers")
                .update({"clicks": clicks})
                .eq("scored_offer_id", scored_offer_id)
                .eq("channel", channel)
                .execute()
            )
            return True
        except Exception as exc:
            raise SupabaseError(str(exc), operation="update_click_count") from exc

    # ------------------------------------------------------------------
    # system_logs
    # ------------------------------------------------------------------

    async def log_event(self, event_type: str, details: Optional[dict] = None) -> bool:
        """
        Registra um evento operacional no banco para monitoramento.

        event_type sugeridos:
          "scrape_start"   — início de uma rodada de scraping
          "scrape_success" — scraping concluído com sucesso
          "scrape_error"   — erro durante o scraping
          "score_run"      — rodada do score engine concluída
          "send_ok"        — oferta enviada com sucesso
          "send_error"     — falha no envio
          "health_check"   — resultado do health check

        Args:
            event_type: Tipo do evento (string curta, snake_case)
            details: Dict com dados adicionais (serializado como JSONB)
        """
        data = {
            "event_type": event_type,
            "details": details or {},
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            await self._db.table("system_logs").insert(data).execute()
            logger.debug("event_logged", event_type=event_type)
            return True
        except Exception as exc:
            # Falha de log é menos crítica, mas ainda reportamos via exceção
            raise SupabaseError(str(exc), operation="log_event") from exc

    async def get_recent_logs(
        self, event_type: Optional[str] = None, limit: int = 100
    ) -> list[dict]:
        """Retorna os logs mais recentes, opcionalmente filtrado por event_type."""
        try:
            query = (
                self._db.table("system_logs")
                .select("event_type, details, created_at")
                .order("created_at", desc=True)
                .limit(limit)
            )
            if event_type:
                query = query.eq("event_type", event_type)
            result = await query.execute()
            return result.data or []
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_recent_logs") from exc

    # ------------------------------------------------------------------
    # users
    # ------------------------------------------------------------------

    async def get_or_create_user(
        self,
        name: str,
        affiliate_tag: str,
        email: str | None = None,
        password_hash: str | None = None,
        ml_cookies: dict | None = None,
    ) -> Optional[str]:
        """Retorna o ID do user pela tag. Cria se nao existir."""
        try:
            result = (
                await self._db.table("users")
                .select("id")
                .eq("affiliate_tag", affiliate_tag)
                .limit(1)
                .execute()
            )
            if result.data:
                return result.data[0]["id"]
            data: dict = {
                "name": name,
                "affiliate_tag": affiliate_tag,
                "ml_cookies": ml_cookies,
            }
            if email:
                data["email"] = email
            if password_hash:
                data["password_hash"] = password_hash
            result = await self._db.table("users").insert(data).execute()
            if result.data:
                user_id: str = result.data[0]["id"]
                logger.info("user_created", name=name, tag=affiliate_tag)
                return user_id
            return None
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_or_create_user") from exc

    async def get_user_by_tag(self, affiliate_tag: str) -> Optional[dict]:
        """Retorna o user completo pela tag."""
        try:
            result = (
                await self._db.table("users")
                .select("*")
                .eq("affiliate_tag", affiliate_tag)
                .limit(1)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_user_by_tag") from exc

    # ------------------------------------------------------------------
    # affiliate_links
    # ------------------------------------------------------------------

    async def get_affiliate_link(
        self, product_id: str, user_id: str
    ) -> Optional[dict]:
        """Retorna o affiliate link para um produto+user, ou None."""
        try:
            result = (
                await self._db.table("affiliate_links")
                .select("*")
                .eq("product_id", product_id)
                .eq("user_id", user_id)
                .limit(1)
                .execute()
            )
            return result.data[0] if result.data else None
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_affiliate_link") from exc

    async def save_affiliate_link(
        self,
        product_id: str,
        user_id: str,
        short_url: str,
        long_url: str = "",
        ml_link_id: str = "",
    ) -> Optional[str]:
        """Salva um affiliate link (upsert por product_id+user_id)."""
        data = {
            "product_id": product_id,
            "user_id": user_id,
            "short_url": short_url,
            "long_url": long_url,
            "ml_link_id": ml_link_id,
        }
        try:
            result = (
                await self._db.table("affiliate_links")
                .upsert(data, on_conflict="product_id,user_id")
                .execute()
            )
            if result.data:
                return result.data[0]["id"]
            return None
        except Exception as exc:
            raise SupabaseError(str(exc), operation="save_affiliate_link") from exc

    async def get_missing_affiliate_links(
        self, user_id: str, product_ids: list[str]
    ) -> list[str]:
        """Retorna product_ids que ainda nao tem affiliate link para este user."""
        if not product_ids:
            return []
        try:
            result = (
                await self._db.table("affiliate_links")
                .select("product_id")
                .eq("user_id", user_id)
                .in_("product_id", product_ids)
                .execute()
            )
            existing = {row["product_id"] for row in (result.data or [])}
            return [pid for pid in product_ids if pid not in existing]
        except Exception as exc:
            raise SupabaseError(
                str(exc), operation="get_missing_affiliate_links"
            ) from exc

    async def save_affiliate_links_batch(
        self, links: list[dict]
    ) -> list[str]:
        """Salva multiplos affiliate links em uma chamada."""
        if not links:
            return []
        try:
            result = (
                await self._db.table("affiliate_links")
                .upsert(links, on_conflict="product_id,user_id")
                .execute()
            )
            if not result.data:
                return []
            return [row["id"] for row in result.data]
        except Exception as exc:
            raise SupabaseError(
                str(exc), operation="save_affiliate_links_batch"
            ) from exc

    # ------------------------------------------------------------------
    # Image Worker
    # ------------------------------------------------------------------

    async def get_pending_images(self, batch_size: int = 5) -> list[dict]:
        """Retorna produtos que precisam de processamento de imagem."""
        try:
            result = (
                await self._db.table("products")
                .select("id, ml_id, title, thumbnail_url")
                .eq("image_status", "pending")
                .order("last_seen_at", desc=True)
                .limit(batch_size)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_pending_images") from exc

    async def discard_offer(self, scored_offer_id: str, reason: str) -> bool:
        """Marca oferta como rejeitada (reprovada pelo validador)."""
        try:
            await (
                self._db.table("scored_offers")
                .update({"status": "rejected"})
                .eq("id", scored_offer_id)
                .execute()
            )
            await self.log_event(
                "offer_discarded",
                {"scored_offer_id": scored_offer_id, "reason": reason},
            )
            return True
        except Exception as exc:
            raise SupabaseError(str(exc), operation="discard_offer") from exc

    async def update_image_status(
        self,
        product_id: str,
        status: str,
        enhanced_url: str | None = None,
    ) -> bool:
        """Atualiza o status de processamento de imagem de um produto."""
        data: dict = {"image_status": status}
        if enhanced_url:
            data["enhanced_image_url"] = enhanced_url
        try:
            await (
                self._db.table("products")
                .update(data)
                .eq("id", product_id)
                .execute()
            )
            return True
        except Exception as exc:
            raise SupabaseError(str(exc), operation="update_image_status") from exc

    async def get_enhanced_image_url(self, product_id: str) -> str | None:
        """Retorna a URL da imagem aprimorada, se existir."""
        try:
            result = (
                await self._db.table("products")
                .select("enhanced_image_url")
                .eq("id", product_id)
                .eq("image_status", "enhanced")
                .limit(1)
                .execute()
            )
            if result.data and result.data[0].get("enhanced_image_url"):
                return result.data[0]["enhanced_image_url"]
            return None
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_enhanced_image_url") from exc

    # ------------------------------------------------------------------
    # title_examples
    # ------------------------------------------------------------------

    async def save_title_example(self, data: dict) -> Optional[str]:
        """Salva um exemplo de título aprovado/editado."""
        row = {
            "scored_offer_id": data.get("scored_offer_id"),
            "product_title": data["product_title"],
            "category": data.get("category"),
            "price": data.get("price"),
            "generated_title": data["generated_title"],
            "final_title": data["final_title"],
            "action": data["action"],
        }
        try:
            result = await self._db.table("title_examples").insert(row).execute()
            if result.data:
                example_id: str = result.data[0]["id"]
                logger.debug("supabase_title_example_saved", example_id=example_id)
                return example_id
            return None
        except Exception as exc:
            raise SupabaseError(str(exc), operation="save_title_example") from exc

    async def get_recent_title_examples(self, limit: int = 10) -> list[dict]:
        """Retorna exemplos recentes de títulos aprovados/editados."""
        try:
            result = (
                await self._db.table("title_examples")
                .select("product_title, final_title, action")
                .in_("action", ["approved", "edited"])
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
            )
            return result.data or []
        except Exception as exc:
            raise SupabaseError(str(exc), operation="get_recent_title_examples") from exc

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    @staticmethod
    def _product_to_row(
        product: ScrapedProduct,
        badge_id: str | None = None,
        category_id: str | None = None,
        marketplace_id: str | None = None,
    ) -> dict:
        """
        Mapeia os campos do ScrapedProduct para as colunas da tabela products.
        Campos do ScrapedProduct → Colunas do banco:
          url           → product_url
          price         → current_price
          discount_pct  → discount_percent  (float → int)
          rating        → rating_stars
          review_count  → rating_count
          free_shipping → free_shipping
          installments_without_interest → installments_without_interest
          image_url     → thumbnail_url
          category      → category_id  (resolvido externamente)
        """
        now = datetime.now(tz=timezone.utc).isoformat()
        row = {
            "ml_id": product.ml_id,
            "title": product.title,
            "current_price": product.price,
            "original_price": product.original_price,
            "pix_price": product.pix_price,
            "discount_percent": round(product.discount_pct, 1),
            "rating_stars": product.rating,
            "rating_count": product.review_count,
            "free_shipping": product.free_shipping,
            "full_shipping": product.full_shipping,
            "installments_without_interest": product.installments_without_interest,
            "installment_count": product.installment_count,
            "installment_value": product.installment_value,
            "brand": product.brand,
            "variations": product.variations,
            "discount_type": product.discount_type,
            "gender": product.gender,
            "thumbnail_url": product.image_url,
            "product_url": product.url,
            "category_id": category_id,
            "badge_id": badge_id,
            "marketplace_id": marketplace_id,
            # Supabase retorna first_seen_at inalterado no update via trigger
            "first_seen_at": now,
            "last_seen_at": now,
        }
        return row
