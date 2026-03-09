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
from .exceptions import SQLiteError, SupabaseError
from .seeds import BADGES, CATEGORIES

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
        # Caches persistentes de lookup: {nome_canonico: uuid}
        self._badge_cache: dict[str, str] = {}
        self._category_cache: dict[str, str] = {}
        # Lookup de normalização: {nome_lower: nome_canonico}
        self._badge_canonical: dict[str, str] = {b.lower(): b for b in BADGES}
        self._category_canonical: dict[str, str] = {c.lower(): c for c in CATEGORIES}

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
            await self._preload_caches()
            return

        try:
            await self._supabase.connect()
            ok = await self._supabase.ping()
            if ok:
                self._using_supabase = True
                logger.info("storage_backend", backend="supabase")
                # Seed de lookup tables no Supabase
                await self._seed_supabase()
                # Preload caches de lookup
                await self._preload_caches()
                # Auto-sync: envia pendentes do SQLite ao Supabase
                await self._auto_sync()
            else:
                await self._supabase.close()
                logger.warning(
                    "storage_backend",
                    backend="sqlite",
                    reason="supabase_ping_failed",
                )
                await self._preload_caches()
        except Exception as exc:
            logger.warning(
                "storage_backend",
                backend="sqlite",
                reason="supabase_connect_error",
                error=str(exc),
            )
            await self._preload_caches()

    async def _seed_supabase(self) -> None:
        """Insere badges e categories canônicos no Supabase (idempotente)."""
        try:
            for name in BADGES:
                await self._supabase.get_or_create_badge(name)
            for name in CATEGORIES:
                await self._supabase.get_or_create_category(name)
            logger.debug(
                "supabase_seeds_applied",
                badges=len(BADGES),
                categories=len(CATEGORIES),
            )
        except SupabaseError as exc:
            logger.warning("supabase_seed_failed", error=str(exc))

    async def _preload_caches(self) -> None:
        """Carrega badges e categories em memória para evitar queries repetidas."""
        try:
            if self._using_supabase:
                self._badge_cache = await self._supabase.get_all_badges()
                self._category_cache = await self._supabase.get_all_categories()
            else:
                self._badge_cache = await self._sqlite.get_all_badges()
                self._category_cache = await self._sqlite.get_all_categories()
            logger.debug(
                "caches_preloaded",
                badges=len(self._badge_cache),
                categories=len(self._category_cache),
            )
        except Exception as exc:
            logger.warning("cache_preload_failed", error=str(exc))

    async def _auto_sync(self) -> None:
        """Sincroniza pendentes do SQLite ao reconectar com Supabase."""
        try:
            counts = await self._sqlite.get_unsynced_count()
            total = sum(v for v in counts.values() if v > 0)
            if total > 0:
                logger.info("auto_sync_start", pending=counts)
                stats = await self._sqlite.sync_to_supabase(self._supabase)
                total_synced = sum(v["synced"] for v in stats.values())
                total_errors = sum(v["errors"] for v in stats.values())
                logger.info(
                    "auto_sync_done",
                    synced=total_synced,
                    errors=total_errors,
                )
        except Exception as exc:
            logger.warning("auto_sync_failed", error=str(exc))

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

        Resolve badge_id e category_id antes de gravar (simétrico com batch).

        Returns:
            UUID do produto (gerado pelo SQLite se Supabase indisponível).

        Raises:
            SQLiteError: se o SQLite local falhar (crítico — dado perdido).
        """
        # Resolve FKs de lookup (usa cache em memória)
        badge_id = await self.resolve_badge_id(product.badge)
        category_id = await self.resolve_category_id(product.category)

        if self._using_supabase:
            try:
                # Supabase primeiro: gera o UUID canônico
                remote_id = await self._supabase.upsert_product(
                    product, badge_id=badge_id, category_id=category_id
                )
                # SQLite usa o mesmo UUID para manter FKs consistentes
                await self._sqlite.upsert_product(
                    product,
                    product_id=remote_id,
                    badge_id=badge_id,
                    category_id=category_id,
                )
                return remote_id
            except SupabaseError as exc:
                logger.warning(
                    "supabase_write_failed_using_local_id",
                    ml_id=product.ml_id,
                    error=str(exc),
                )

        # Supabase indisponível: SQLite gera seu próprio UUID
        return await self._sqlite.upsert_product(
            product, badge_id=badge_id, category_id=category_id
        )

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
    # Normalização de nomes (case-insensitive → canônico)
    # ------------------------------------------------------------------

    def _normalize_badge(self, name: str) -> str:
        """Normaliza nome de badge para a forma canônica dos seeds.

        Ex: 'MAIS VENDIDO' → 'Mais vendido', 'oferta do dia' → 'Oferta do dia'.
        Se não encontrar match nos seeds, retorna o nome stripped como está.
        """
        return self._badge_canonical.get(name.strip().lower(), name.strip())

    def _normalize_category(self, name: str) -> str:
        """Normaliza nome de categoria para a forma canônica dos seeds.

        Ex: 'ELETRÔNICOS, ÁUDIO E VÍDEO' → 'Eletrônicos, Áudio e Vídeo'.
        Se não encontrar match nos seeds, retorna o nome stripped como está.
        """
        return self._category_canonical.get(name.strip().lower(), name.strip())

    # ------------------------------------------------------------------
    # badges
    # ------------------------------------------------------------------

    async def resolve_badge_id(self, name: str) -> str | None:
        """Resolve o nome de um badge para seu UUID, usando cache em memória."""
        if not name:
            return None
        # Normaliza para forma canônica (case-insensitive)
        name = self._normalize_badge(name)
        # Cache hit
        if name in self._badge_cache:
            return self._badge_cache[name]
        # Cache miss → consulta + cria se necessário
        badge_id: str | None = None
        if self._using_supabase:
            try:
                badge_id = await self._supabase.get_or_create_badge(name)
            except SupabaseError as exc:
                logger.warning("supabase_badge_resolve_failed", error=str(exc))
        if badge_id is None:
            badge_id = await self._sqlite.get_or_create_badge(name)
        if badge_id:
            self._badge_cache[name] = badge_id
        return badge_id

    # ------------------------------------------------------------------
    # categories
    # ------------------------------------------------------------------

    async def resolve_category_id(self, name: str) -> str | None:
        """Resolve o nome de uma categoria para seu UUID, usando cache em memória."""
        if not name:
            return None
        # Normaliza para forma canônica (case-insensitive)
        name = self._normalize_category(name)
        # Cache hit
        if name in self._category_cache:
            return self._category_cache[name]
        # Cache miss → consulta + cria se necessário
        cat_id: str | None = None
        if self._using_supabase:
            try:
                cat_id = await self._supabase.get_or_create_category(name)
            except SupabaseError as exc:
                logger.warning("supabase_category_resolve_failed", error=str(exc))
        if cat_id is None:
            cat_id = await self._sqlite.get_or_create_category(name)
        if cat_id:
            self._category_cache[name] = cat_id
        return cat_id

    # ------------------------------------------------------------------
    # Batch operations (performance)
    # ------------------------------------------------------------------

    async def check_duplicates_batch(self, ml_ids: list[str]) -> set[str]:
        """Verifica quais ml_ids já existem (1 query ao invés de N)."""
        if self._using_supabase:
            try:
                return await self._supabase.check_duplicates_batch(ml_ids)
            except SupabaseError:
                pass
        return await self._sqlite.check_duplicates_batch(ml_ids)

    async def upsert_products_batch(
        self, products: list[ScrapedProduct]
    ) -> dict[str, str]:
        """
        Upsert de múltiplos produtos (1 chamada ao invés de N).
        Retorna dict mapeando ml_id → UUID.
        """
        if not products:
            return {}

        # Resolve badge IDs em batch (cache por nome)
        badge_cache: dict[str, str | None] = {}
        badge_ids: dict[str, str | None] = {}
        for p in products:
            if p.badge:
                if p.badge not in badge_cache:
                    badge_cache[p.badge] = await self.resolve_badge_id(p.badge)
                badge_ids[p.ml_id] = badge_cache[p.badge]

        # Resolve category IDs em batch (cache por nome)
        cat_cache: dict[str, str | None] = {}
        category_ids: dict[str, str | None] = {}
        for p in products:
            if p.category:
                if p.category not in cat_cache:
                    cat_cache[p.category] = await self.resolve_category_id(p.category)
                category_ids[p.ml_id] = cat_cache[p.category]

        if self._using_supabase:
            try:
                remote_ids = await self._supabase.upsert_products_batch(
                    products,
                    badge_ids=badge_ids,
                    category_ids=category_ids,
                )
                # Espelha no SQLite com os mesmos UUIDs
                await self._sqlite.upsert_products_batch(
                    products,
                    product_ids=remote_ids,
                    badge_ids=badge_ids,
                    category_ids=category_ids,
                )
                return remote_ids
            except SupabaseError as exc:
                logger.warning(
                    "supabase_batch_upsert_failed",
                    error=str(exc),
                )

        # Fallback: SQLite gera UUIDs próprios
        return await self._sqlite.upsert_products_batch(
            products,
            badge_ids=badge_ids,
            category_ids=category_ids,
        )

    async def add_price_history_batch(self, entries: list[dict]) -> bool:
        """
        Insere múltiplas entradas de histórico de preço (1 chamada ao invés de N).
        entries: lista de dicts com keys: product_id, price, original_price.
        """
        if not entries:
            return True

        local_ok = False
        try:
            local_ok = await self._sqlite.add_price_history_batch(entries)
        except SQLiteError as exc:
            logger.warning("sqlite_price_history_batch_failed", error=str(exc))

        if self._using_supabase:
            try:
                return await self._supabase.add_price_history_batch(entries)
            except SupabaseError as exc:
                logger.warning("supabase_price_history_batch_failed", error=str(exc))
        return local_ok

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
        local_ok = False
        try:
            local_ok = await self._sqlite.add_price_history(
                product_id, price, original_price
            )
        except SQLiteError as exc:
            if "FOREIGN KEY" in str(exc):
                logger.warning(
                    "sqlite_fk_skip",
                    product_id=product_id,
                    table="price_history",
                )
            else:
                raise
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
        """
        local_id = None
        try:
            local_id = await self._sqlite.save_scored_offer(
                product_id,
                rule_score,
                final_score,
                status,
                ai_score,
                ai_description,
            )
        except SQLiteError as exc:
            if "FOREIGN KEY" in str(exc):
                logger.warning(
                    "sqlite_fk_skip",
                    product_id=product_id,
                    table="scored_offers",
                )
            else:
                raise

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

        return local_id or ""

    # ------------------------------------------------------------------
    # sent_offers
    # ------------------------------------------------------------------

    async def has_recent_sends(self, hours: int = 24) -> bool:
        """Verifica rapidamente se há algum envio recente (1 query)."""
        if self._using_supabase:
            try:
                return await self._supabase.has_recent_sends(hours)
            except SupabaseError:
                pass
        return await self._sqlite.has_recent_sends(hours)

    async def mark_as_sent(
        self,
        scored_offer_id: str,
        channel: str,
        shlink_short_url: str = "",
    ) -> bool:
        """Registra o envio em ambos os bancos."""
        local_ok = False
        try:
            local_ok = await self._sqlite.mark_as_sent(
                scored_offer_id, channel, shlink_short_url
            )
        except SQLiteError as exc:
            if "FOREIGN KEY" in str(exc):
                logger.warning(
                    "sqlite_fk_skip",
                    scored_offer_id=scored_offer_id,
                    table="sent_offers",
                )
            else:
                raise
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
