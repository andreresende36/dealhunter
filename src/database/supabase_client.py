"""
DealHunter — Supabase Client
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

    async def close(self) -> None:
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
    # products
    # ------------------------------------------------------------------

    async def upsert_product(self, product: ScrapedProduct) -> Optional[str]:
        """
        Insere ou atualiza um produto pelo ml_id.

        - Na inserção: define first_seen_at = last_seen_at = NOW()
        - Na atualização: preserva first_seen_at via trigger no banco,
          atualiza apenas os campos de preço e disponibilidade.

        Retorna o UUID (id) do produto no banco, ou None em caso de erro.
        """
        data = self._product_to_row(product)
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
    ) -> bool:
        """
        Registra o preço atual de um produto no histórico.
        Deve ser chamado a cada vez que o scraper coleta o produto.

        Args:
            product_id: UUID interno do produto (retornado por upsert_product)
            price: Preço atual em BRL
            original_price: Preço original antes do desconto (pode ser None)
        """
        data = {
            "product_id": product_id,
            "price": price,
            "original_price": original_price,
            "recorded_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            await self._db.table("price_history").insert(data).execute()
            logger.debug("price_history_added", product_id=product_id, price=price)
            return True
        except Exception as exc:
            raise SupabaseError(
                str(exc), operation="add_price_history"
            ) from exc

    async def get_price_history(
        self, product_id: str, days: int = 30
    ) -> list[dict]:
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
            raise SupabaseError(
                str(exc), operation="get_price_history"
            ) from exc

    # ------------------------------------------------------------------
    # scored_offers
    # ------------------------------------------------------------------

    async def save_scored_offer(
        self,
        product_id: str,
        rule_score: int,
        final_score: int,
        status: str,
        ai_score: Optional[int] = None,
        ai_description: Optional[str] = None,
    ) -> Optional[str]:
        """
        Salva o resultado da análise de uma oferta.

        Args:
            product_id: UUID interno do produto
            rule_score: Pontuação calculada pelo Score Engine (0-100)
            final_score: Pontuação final (igual a rule_score se sem IA)
            status: "approved" | "rejected" | "pending"
            ai_score: Pontuação da IA (0-100), None se não analisado
            ai_description: Descrição gerada pelo Claude, None se sem IA

        Returns:
            UUID do scored_offer criado, ou None em caso de erro.
        """
        data = {
            "product_id": product_id,
            "rule_score": rule_score,
            "ai_score": ai_score,
            "final_score": final_score,
            "ai_description": ai_description,
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
            raise SupabaseError(
                str(exc), operation="save_scored_offer"
            ) from exc

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

    # ------------------------------------------------------------------
    # sent_offers
    # ------------------------------------------------------------------

    async def mark_as_sent(
        self,
        scored_offer_id: str,
        channel: str,
        shlink_short_url: str = "",
    ) -> bool:
        """
        Registra o envio de uma oferta para um canal específico.

        Args:
            scored_offer_id: UUID do scored_offer enviado
            channel: "telegram" ou "whatsapp"
            shlink_short_url: URL encurtada gerada pelo Shlink

        Returns:
            True se registrado com sucesso, False em caso de erro.
        """
        data = {
            "scored_offer_id": scored_offer_id,
            "channel": channel,
            "shlink_short_url": shlink_short_url,
            "sent_at": datetime.now(tz=timezone.utc).isoformat(),
            "clicks": 0,
        }
        try:
            await self._db.table("sent_offers").insert(data).execute()
            logger.info(
                "offer_marked_sent",
                scored_offer_id=scored_offer_id,
                channel=channel,
            )
            return True
        except Exception as exc:
            raise SupabaseError(
                str(exc), operation="mark_as_sent"
            ) from exc

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

    async def update_click_count(self, scored_offer_id: str, channel: str, clicks: int) -> bool:
        """Atualiza o contador de cliques de um envio (chamado pelo Shlink webhook)."""
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
            raise SupabaseError(
                str(exc), operation="update_click_count"
            ) from exc

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
            raise SupabaseError(
                str(exc), operation="log_event"
            ) from exc

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
            raise SupabaseError(
                str(exc), operation="get_recent_logs"
            ) from exc

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    @staticmethod
    def _product_to_row(product: ScrapedProduct) -> dict:
        """
        Mapeia os campos do ScrapedProduct para as colunas da tabela products.
        Campos do ScrapedProduct → Colunas do banco:
          url           → product_url
          price         → current_price
          discount_pct  → discount_percent  (float → int)
          seller        → seller_name
          rating        → rating_stars
          review_count  → rating_count
          image_url     → thumbnail_url
        """
        now = datetime.now(tz=timezone.utc).isoformat()
        return {
            "ml_id":            product.ml_id,
            "title":            product.title,
            "current_price":    product.price,
            "original_price":   product.original_price,
            "discount_percent": int(product.discount_pct),
            "seller_name":      product.seller,
            "rating_stars":     product.rating,
            "rating_count":     product.review_count,
            "free_shipping":    product.free_shipping,
            "thumbnail_url":    product.image_url,
            "product_url":      product.url,
            "category":         product.category,
            # Supabase retorna first_seen_at inalterado no update via trigger
            "first_seen_at":    now,
            "last_seen_at":     now,
        }
