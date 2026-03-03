"""
DealHunter — Storage Manager
Abstração que unifica Supabase e SQLite Fallback em uma interface única.

Estratégia:
  - SQLite é SEMPRE inicializado (cache local e buffer de sync)
  - Supabase é tentado na inicialização; se falhar, SQLite assume o papel principal
  - Se Supabase estiver disponível, todas as escritas vão para os dois bancos
  - Se Supabase ficar indisponível durante a sessão, continua com SQLite
  - Ao reconectar, sync_pending() envia os dados acumulados no SQLite

Uso:
    async with StorageManager() as storage:
        product_id = await storage.upsert_product(product)
        await storage.add_price_history(product_id, price=299.90)
        await storage.log_event("scrape_success", {"count": 42})

    # Para forçar SQLite (desenvolvimento / testes):
    async with StorageManager(force_sqlite=True) as storage:
        ...
"""

from __future__ import annotations

import structlog

from src.config import settings
from src.scraper.base_scraper import ScrapedProduct
from .supabase_client import SupabaseClient
from .sqlite_fallback import SQLiteFallback
from .exceptions import SupabaseError

logger = structlog.get_logger(__name__)


class StorageManager:
    """
    Gerenciador de armazenamento com failover automático Supabase → SQLite.

    Todas as escritas são espelhadas no SQLite local para garantir
    que nenhum dado seja perdido em caso de falha de rede.
    O Supabase é o banco canônico; o SQLite é o buffer de segurança.
    """

    def __init__(self, force_sqlite: bool = False) -> None:
        """
        Args:
            force_sqlite: Se True, usa apenas SQLite (útil em desenvolvimento
                          e testes sem credenciais do Supabase configuradas).
                          Também ativado automaticamente se APP_ENV != production.
        """
        self._force_sqlite = force_sqlite or not settings.is_production
        self._supabase = SupabaseClient()
        self._sqlite = SQLiteFallback()
        self._using_supabase = False

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def __aenter__(self) -> StorageManager:
        await self._connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self._disconnect()

    async def _connect(self) -> None:
        """Inicializa SQLite sempre; tenta Supabase se não for forçado SQLite."""
        await self._sqlite.initialize()

        if self._force_sqlite:
            logger.info("storage_backend", backend="sqlite", reason="forced")
            return

        try:
            await self._supabase.connect()
            ok = await self._supabase.ping()
            if ok:
                self._using_supabase = True
                logger.info("storage_backend", backend="supabase")
            else:
                await self._supabase.close()
                logger.warning(
                    "storage_backend",
                    backend="sqlite",
                    reason="supabase_ping_failed",
                )
        except Exception as exc:
            logger.warning(
                "storage_backend",
                backend="sqlite",
                reason="supabase_connect_error",
                error=str(exc),
            )

    async def _disconnect(self) -> None:
        """Fecha todas as conexões abertas."""
        await self._sqlite.close()
        if self._using_supabase:
            await self._supabase.close()

    # ------------------------------------------------------------------
    # Propriedades de estado
    # ------------------------------------------------------------------

    @property
    def backend(self) -> str:
        """Retorna o nome do backend ativo: 'supabase' ou 'sqlite'."""
        return "supabase" if self._using_supabase else "sqlite"

    @property
    def is_healthy(self) -> bool:
        """True se há pelo menos um backend disponível (sempre True após connect)."""
        return True  # SQLite local nunca falha após initialize()

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def ping(self) -> dict[str, bool]:
        """
        Verifica disponibilidade de cada backend.

        Returns:
            {"supabase": bool, "sqlite": bool}
        """
        sqlite_ok = await self._sqlite.ping()
        supabase_ok = await self._supabase.ping() if self._using_supabase else False
        return {"supabase": supabase_ok, "sqlite": sqlite_ok}

    # ------------------------------------------------------------------
    # products
    # ------------------------------------------------------------------

    async def upsert_product(self, product: ScrapedProduct) -> str:
        """
        Insere ou atualiza um produto em ambos os bancos.

        O SQLite é gravado primeiro (buffer seguro).
        O Supabase é gravado em seguida se disponível.

        Returns:
            UUID do produto (gerado pelo SQLite se Supabase indisponível).

        Raises:
            SQLiteError: se o SQLite local falhar (crítico — dado perdido).
        """
        # SQLite sempre primeiro — garante que o dado não se perde
        local_id = await self._sqlite.upsert_product(product)

        if self._using_supabase:
            try:
                remote_id = await self._supabase.upsert_product(product)
                return remote_id
            except SupabaseError as exc:
                logger.warning(
                    "supabase_write_failed_using_local_id",
                    ml_id=product.ml_id,
                    error=str(exc),
                )

        return local_id

    async def check_duplicate(self, ml_id: str) -> bool:
        """Verifica se um produto já existe (consulta o backend ativo, fallback no outro)."""
        if self._using_supabase:
            try:
                return await self._supabase.check_duplicate(ml_id)
            except SupabaseError:
                pass
        return await self._sqlite.check_duplicate(ml_id)

    async def get_product_id(self, ml_id: str) -> str | None:
        """Retorna o UUID interno de um produto pelo ml_id."""
        if self._using_supabase:
            try:
                return await self._supabase.get_product_id(ml_id)
            except SupabaseError:
                pass
        return await self._sqlite.get_product_id(ml_id)

    # ------------------------------------------------------------------
    # price_history
    # ------------------------------------------------------------------

    async def add_price_history(
        self,
        product_id: str,
        price: float,
        original_price: float | None = None,
    ) -> bool:
        """Registra o preço atual em ambos os bancos."""
        local_ok = await self._sqlite.add_price_history(
            product_id, price, original_price
        )
        if self._using_supabase:
            try:
                return await self._supabase.add_price_history(
                    product_id, price, original_price
                )
            except SupabaseError as exc:
                logger.warning("supabase_price_history_failed", error=str(exc))
        return local_ok

    async def get_price_history(self, product_id: str, days: int = 30) -> list[dict]:
        """Retorna histórico de preços dos últimos N dias."""
        if self._using_supabase:
            try:
                return await self._supabase.get_price_history(product_id, days)
            except SupabaseError:
                pass
        return await self._sqlite.get_price_history(product_id, days)

    # ------------------------------------------------------------------
    # scored_offers
    # ------------------------------------------------------------------

    async def save_scored_offer(
        self,
        product_id: str,
        rule_score: int,
        final_score: int,
        status: str,
        ai_score: int | None = None,
        ai_description: str | None = None,
    ) -> str:
        """
        Salva o resultado da análise em ambos os bancos.

        Returns:
            UUID do scored_offer.

        Raises:
            SQLiteError: se o SQLite local falhar.
        """
        local_id = await self._sqlite.save_scored_offer(
            product_id,
            rule_score,
            final_score,
            status,
            ai_score,
            ai_description,
        )
        if self._using_supabase:
            try:
                remote_id = await self._supabase.save_scored_offer(
                    product_id,
                    rule_score,
                    final_score,
                    status,
                    ai_score,
                    ai_description,
                )
                return remote_id
            except SupabaseError as exc:
                logger.warning("supabase_scored_offer_failed", error=str(exc))

        return local_id

    # ------------------------------------------------------------------
    # sent_offers
    # ------------------------------------------------------------------

    async def mark_as_sent(
        self,
        scored_offer_id: str,
        channel: str,
        shlink_short_url: str = "",
    ) -> bool:
        """Registra o envio em ambos os bancos."""
        local_ok = await self._sqlite.mark_as_sent(
            scored_offer_id, channel, shlink_short_url
        )
        if self._using_supabase:
            try:
                return await self._supabase.mark_as_sent(
                    scored_offer_id, channel, shlink_short_url
                )
            except SupabaseError as exc:
                logger.warning("supabase_mark_sent_failed", error=str(exc))
        return local_ok

    async def was_recently_sent(self, ml_id: str, hours: int = 24) -> bool:
        """Verifica se o produto foi enviado nas últimas N horas."""
        if self._using_supabase:
            try:
                return await self._supabase.was_recently_sent(ml_id, hours)
            except SupabaseError:
                pass
        return await self._sqlite.was_recently_sent(ml_id, hours)

    # ------------------------------------------------------------------
    # system_logs
    # ------------------------------------------------------------------

    async def log_event(self, event_type: str, details: dict | None = None) -> bool:
        """Registra um evento operacional em ambos os bancos."""
        local_ok = await self._sqlite.log_event(event_type, details)
        if self._using_supabase:
            try:
                return await self._supabase.log_event(event_type, details)
            except SupabaseError as exc:
                logger.warning("supabase_log_event_failed", error=str(exc))
        return local_ok

    async def get_recent_logs(
        self, event_type: str | None = None, limit: int = 100
    ) -> list[dict]:
        """Retorna os logs mais recentes."""
        if self._using_supabase:
            try:
                return await self._supabase.get_recent_logs(event_type, limit)
            except SupabaseError:
                pass
        return await self._sqlite.get_recent_logs(event_type, limit)

    # ------------------------------------------------------------------
    # Sincronização
    # ------------------------------------------------------------------

    async def sync_pending(self) -> dict:
        """
        Sincroniza registros pendentes do SQLite com o Supabase.

        Deve ser chamado quando o Supabase voltar após um período offline,
        ou periodicamente via APScheduler para garantir consistência.

        Returns:
            Dict com stats de sincronização por tabela, ou {} se Supabase
            não estiver disponível.
        """
        if not self._using_supabase:
            logger.warning("sync_pending_skipped", reason="supabase_not_connected")
            return {}

        counts = await self._sqlite.get_unsynced_count()
        total_pending = sum(v for v in counts.values() if v > 0)

        if total_pending == 0:
            logger.debug("sync_pending_nothing_to_sync")
            return {"total_pending": 0}

        logger.info("sync_pending_start", pending=counts)
        stats = await self._sqlite.sync_to_supabase(self._supabase)

        total_errors = sum(v["errors"] for v in stats.values())
        if total_errors > 0:
            logger.error(
                "sync_completed_with_errors",
                stats=stats,
                total_errors=total_errors,
            )

        return stats
