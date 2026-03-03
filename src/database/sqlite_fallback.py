"""
DealHunter — SQLite Fallback
Banco de dados local espelho do Supabase.

Funciona como:
  1. Cache local sempre ativo (mesmo quando Supabase está disponível)
  2. Backend principal quando Supabase está indisponível
  3. Buffer de sincronização para reconexão posterior

Tabelas espelhadas: products, price_history, scored_offers,
                    sent_offers, system_logs

Diferenças em relação ao Supabase:
  - UUIDs gerados em Python (uuid.uuid4()) e armazenados como TEXT
  - Sem RLS (banco local, acesso direto)
  - Coluna extra `synced` (0/1) para controle de sincronização
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import aiosqlite
import structlog

from src.config import settings
from src.scraper.base_scraper import ScrapedProduct
from .exceptions import SQLiteError

if TYPE_CHECKING:
    from .supabase_client import SupabaseClient

logger = structlog.get_logger(__name__)


class SQLiteFallback:
    """
    Banco SQLite local com a mesma interface pública do SupabaseClient.

    Uso como context manager (recomendado):
        async with SQLiteFallback() as db:
            product_id = await db.upsert_product(product)

    Uso direto:
        db = SQLiteFallback()
        await db.initialize()
        ...
        await db.close()
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or settings.sqlite.db_path
        self._conn: Optional[aiosqlite.Connection] = None

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    async def __aenter__(self) -> SQLiteFallback:
        await self.initialize()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def initialize(self) -> None:
        """Abre (ou cria) o banco SQLite e garante que o schema existe."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(str(self.db_path))
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        self._conn = conn
        await self._create_schema()
        logger.info("sqlite_initialized", path=str(self.db_path))

    async def close(self) -> None:
        """Fecha a conexão com o banco."""
        conn = self._conn
        if conn is not None:
            await conn.close()
            self._conn = None
            logger.info("sqlite_closed")

    @property
    def _db(self) -> aiosqlite.Connection:
        """Retorna a conexão ativa ou levanta RuntimeError."""
        if self._conn is None:
            raise RuntimeError(
                "SQLiteFallback não inicializado. "
                "Chame initialize() ou use como context manager."
            )
        return self._conn

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    async def _create_schema(self) -> None:
        """Cria todas as tabelas e índices se ainda não existirem."""
        schema = """
        CREATE TABLE IF NOT EXISTS products (
            id                TEXT PRIMARY KEY,
            ml_id             TEXT NOT NULL UNIQUE,
            title             TEXT NOT NULL,
            current_price     REAL NOT NULL,
            original_price    REAL,
            discount_percent  INTEGER DEFAULT 0,
            seller_name       TEXT DEFAULT '',
            seller_reputation TEXT DEFAULT '',
            sold_quantity     INTEGER DEFAULT 0,
            rating_stars      REAL DEFAULT 0,
            rating_count      INTEGER DEFAULT 0,
            free_shipping     INTEGER DEFAULT 0,
            thumbnail_url     TEXT DEFAULT '',
            product_url       TEXT DEFAULT '',
            category          TEXT DEFAULT '',
            first_seen_at     TEXT DEFAULT (datetime('now')),
            last_seen_at      TEXT DEFAULT (datetime('now')),
            created_at        TEXT DEFAULT (datetime('now')),
            enrichment_status TEXT DEFAULT 'pending',
            enrichment_attempts INTEGER DEFAULT 0,
            enrichment_error  TEXT DEFAULT '',
            enriched_at       TEXT DEFAULT NULL,
            synced            INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id             TEXT PRIMARY KEY,
            product_id     TEXT NOT NULL
                               REFERENCES products(id) ON DELETE CASCADE,
            price          REAL NOT NULL,
            original_price REAL,
            recorded_at    TEXT DEFAULT (datetime('now')),
            synced         INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS scored_offers (
            id             TEXT PRIMARY KEY,
            product_id     TEXT NOT NULL
                               REFERENCES products(id) ON DELETE CASCADE,
            rule_score     INTEGER NOT NULL,
            ai_score       INTEGER,
            final_score    INTEGER NOT NULL,
            ai_description TEXT,
            status         TEXT NOT NULL DEFAULT 'pending',
            scored_at      TEXT DEFAULT (datetime('now')),
            synced         INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS sent_offers (
            id              TEXT PRIMARY KEY,
            scored_offer_id TEXT NOT NULL
                                REFERENCES scored_offers(id) ON DELETE CASCADE,
            channel          TEXT NOT NULL,
            shlink_short_url TEXT DEFAULT '',
            sent_at          TEXT DEFAULT (datetime('now')),
            clicks           INTEGER DEFAULT 0,
            synced           INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS system_logs (
            id         TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            details    TEXT DEFAULT '{}',
            created_at TEXT DEFAULT (datetime('now')),
            synced     INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_p_ml_id ON products(ml_id);
        CREATE INDEX IF NOT EXISTS idx_p_discount
            ON products(discount_percent DESC);
        CREATE INDEX IF NOT EXISTS idx_p_last_seen
            ON products(last_seen_at DESC);
        CREATE INDEX IF NOT EXISTS idx_p_synced ON products(synced);
        CREATE INDEX IF NOT EXISTS idx_p_enrichment
            ON products(enrichment_status);

        CREATE INDEX IF NOT EXISTS idx_ph_product
            ON price_history(product_id);
        CREATE INDEX IF NOT EXISTS idx_ph_recorded
            ON price_history(recorded_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ph_synced ON price_history(synced);

        CREATE INDEX IF NOT EXISTS idx_so_product
            ON scored_offers(product_id);
        CREATE INDEX IF NOT EXISTS idx_so_status ON scored_offers(status);
        CREATE INDEX IF NOT EXISTS idx_so_score
            ON scored_offers(final_score DESC);
        CREATE INDEX IF NOT EXISTS idx_so_synced ON scored_offers(synced);

        CREATE INDEX IF NOT EXISTS idx_se_scored
            ON sent_offers(scored_offer_id);
        CREATE INDEX IF NOT EXISTS idx_se_sent_at
            ON sent_offers(sent_at DESC);
        CREATE INDEX IF NOT EXISTS idx_se_synced ON sent_offers(synced);

        CREATE INDEX IF NOT EXISTS idx_sl_type ON system_logs(event_type);
        CREATE INDEX IF NOT EXISTS idx_sl_created
            ON system_logs(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_sl_synced ON system_logs(synced);
        """
        await self._db.executescript(schema)
        await self._db.commit()

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    async def ping(self) -> bool:
        """Verifica se o banco SQLite está acessível."""
        try:
            await self._db.execute("SELECT 1")
            return True
        except Exception as exc:
            logger.warning("sqlite_ping_failed", error=str(exc))
            return False

    # ------------------------------------------------------------------
    # products
    # ------------------------------------------------------------------

    async def upsert_product(
        self, product: ScrapedProduct, product_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Insere ou atualiza um produto pelo ml_id.

        Na inserção: usa product_id fornecido (quando Supabase disponível)
                     ou gera um novo UUID.
        Na atualização: preserva id e first_seen_at originais.

        Args:
            product: Dados do produto scrapeado.
            product_id: UUID a usar na inserção. Se None, gera um novo.
                        Ignorado se o produto já existir no SQLite.

        Returns:
            UUID (str) do produto, ou None em caso de erro.
        """
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            cursor = await self._db.execute(
                "SELECT id, first_seen_at FROM products WHERE ml_id = ?",
                (product.ml_id,),
            )
            existing = await cursor.fetchone()

            if existing:
                product_id = existing["id"]
                first_seen = existing["first_seen_at"]
                await self._db.execute(
                    """
                    UPDATE products SET
                        title=?, current_price=?, original_price=?,
                        discount_percent=?, seller_name=?,
                        rating_stars=?, rating_count=?,
                        free_shipping=?, thumbnail_url=?,
                        product_url=?, category=?,
                        first_seen_at=?, last_seen_at=?, synced=0
                    WHERE ml_id=?
                    """,
                    (
                        product.title,
                        product.price,
                        product.original_price,
                        int(product.discount_pct),
                        product.seller,
                        product.rating,
                        product.review_count,
                        int(product.free_shipping),
                        product.image_url,
                        product.url,
                        product.category,
                        first_seen,
                        now,
                        product.ml_id,
                    ),
                )
            else:
                product_id = product_id or str(uuid.uuid4())
                await self._db.execute(
                    """
                    INSERT INTO products (
                        id, ml_id, title, current_price, original_price,
                        discount_percent, seller_name, seller_reputation,
                        sold_quantity, rating_stars, rating_count,
                        free_shipping, thumbnail_url, product_url, category,
                        first_seen_at, last_seen_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        product_id,
                        product.ml_id,
                        product.title,
                        product.price,
                        product.original_price,
                        int(product.discount_pct),
                        product.seller,
                        "",
                        0,
                        product.rating,
                        product.review_count,
                        int(product.free_shipping),
                        product.image_url,
                        product.url,
                        product.category,
                        now,
                        now,
                    ),
                )

            await self._db.commit()
            logger.debug(
                "sqlite_product_upserted",
                ml_id=product.ml_id,
                product_id=product_id,
            )
            return product_id

        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="upsert_product", ml_id=product.ml_id
            ) from exc

    async def check_duplicate(self, ml_id: str) -> bool:
        """Retorna True se o produto já existe no banco local."""
        try:
            cursor = await self._db.execute(
                "SELECT 1 FROM products WHERE ml_id = ?", (ml_id,)
            )
            return await cursor.fetchone() is not None
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="check_duplicate", ml_id=ml_id
            ) from exc

    async def get_product_id(self, ml_id: str) -> Optional[str]:
        """Retorna o UUID interno de um produto pelo ml_id."""
        try:
            cursor = await self._db.execute(
                "SELECT id FROM products WHERE ml_id = ?", (ml_id,)
            )
            row = await cursor.fetchone()
            return row["id"] if row else None
        except Exception as exc:
            raise SQLiteError(
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
        """Registra o preço atual no histórico local."""
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            await self._db.execute(
                """
                INSERT INTO price_history
                    (id, product_id, price, original_price, recorded_at)
                VALUES (?,?,?,?,?)
                """,
                (str(uuid.uuid4()), product_id, price, original_price, now),
            )
            await self._db.commit()
            return True
        except Exception as exc:
            raise SQLiteError(str(exc), operation="add_price_history") from exc

    async def get_price_history(self, product_id: str, days: int = 30) -> list[dict]:
        """Retorna histórico de preços dos últimos N dias."""
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(days=days)).isoformat()
        try:
            cursor = await self._db.execute(
                """
                SELECT price, original_price, recorded_at
                FROM price_history
                WHERE product_id=? AND recorded_at>=?
                ORDER BY recorded_at ASC
                """,
                (product_id, cutoff),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_price_history") from exc

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

        Returns:
            UUID do scored_offer criado, ou None em caso de erro.
        """
        row_id = str(uuid.uuid4())
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            await self._db.execute(
                """
                INSERT INTO scored_offers (
                    id, product_id, rule_score, ai_score, final_score,
                    ai_description, status, scored_at
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    row_id,
                    product_id,
                    rule_score,
                    ai_score,
                    final_score,
                    ai_description,
                    status,
                    now,
                ),
            )
            await self._db.commit()
            logger.debug(
                "sqlite_scored_offer_saved",
                product_id=product_id,
                final_score=final_score,
                status=status,
            )
            return row_id
        except Exception as exc:
            raise SQLiteError(str(exc), operation="save_scored_offer") from exc

    # ------------------------------------------------------------------
    # sent_offers
    # ------------------------------------------------------------------

    async def mark_as_sent(
        self,
        scored_offer_id: str,
        channel: str,
        shlink_short_url: str = "",
    ) -> bool:
        """Registra o envio de uma oferta para um canal."""
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            await self._db.execute(
                """
                INSERT INTO sent_offers
                    (id, scored_offer_id, channel, shlink_short_url, sent_at)
                VALUES (?,?,?,?,?)
                """,
                (
                    str(uuid.uuid4()),
                    scored_offer_id,
                    channel,
                    shlink_short_url,
                    now,
                ),
            )
            await self._db.commit()
            return True
        except Exception as exc:
            raise SQLiteError(str(exc), operation="mark_as_sent") from exc

    async def was_recently_sent(self, ml_id: str, hours: int = 24) -> bool:
        """
        Verifica se um produto (pelo ml_id) foi enviado nas últimas N horas.
        JOIN: sent_offers → scored_offers → products.
        """
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).isoformat()
        try:
            cursor = await self._db.execute(
                """
                SELECT 1
                FROM sent_offers se
                JOIN scored_offers so ON so.id = se.scored_offer_id
                JOIN products p       ON p.id  = so.product_id
                WHERE p.ml_id=? AND se.sent_at>=?
                LIMIT 1
                """,
                (ml_id, cutoff),
            )
            return await cursor.fetchone() is not None
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="was_recently_sent", ml_id=ml_id
            ) from exc

    # ------------------------------------------------------------------
    # Enrichment queue (deep scrape worker)
    # ------------------------------------------------------------------

    async def claim_for_enrichment(self, batch_size: int = 10) -> list[dict]:
        """Reclama um lote de produtos pendentes para enriquecimento."""
        try:
            cursor = await self._db.execute(
                """
                SELECT id, ml_id, product_url
                FROM products
                WHERE enrichment_status = 'pending'
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (batch_size,),
            )
            rows = await cursor.fetchall()
            if not rows:
                return []

            ids = [row["id"] for row in rows]
            placeholders = ",".join("?" for _ in ids)
            await self._db.execute(
                f"UPDATE products SET enrichment_status = 'in_progress'"  # noqa: S608
                f" WHERE id IN ({placeholders})"
                f" AND enrichment_status = 'pending'",
                ids,
            )
            await self._db.commit()
            logger.debug("sqlite_enrichment_claimed", count=len(ids))
            return [dict(row) for row in rows]
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="claim_for_enrichment"
            ) from exc

    async def set_enrichment_status(
        self, product_id: str, status: str, error: str = ""
    ) -> bool:
        """Atualiza o status de enriquecimento de um produto."""
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            if status == "failed":
                await self._db.execute(
                    """
                    UPDATE products
                    SET enrichment_status = ?, enrichment_error = ?, synced = 0
                    WHERE id = ?
                    """,
                    (status, error, product_id),
                )
            elif status == "enriched":
                await self._db.execute(
                    """
                    UPDATE products
                    SET enrichment_status = ?, enriched_at = ?, synced = 0
                    WHERE id = ?
                    """,
                    (status, now, product_id),
                )
            else:
                await self._db.execute(
                    """
                    UPDATE products
                    SET enrichment_status = ?, synced = 0
                    WHERE id = ?
                    """,
                    (status, product_id),
                )
            await self._db.commit()
            return True
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="set_enrichment_status"
            ) from exc

    async def increment_enrichment_attempts(self, product_id: str) -> bool:
        """Incrementa o contador de tentativas de enriquecimento."""
        try:
            await self._db.execute(
                """
                UPDATE products
                SET enrichment_attempts = enrichment_attempts + 1, synced = 0
                WHERE id = ?
                """,
                (product_id,),
            )
            await self._db.commit()
            return True
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="increment_enrichment_attempts"
            ) from exc

    async def update_enriched_data(self, product_id: str, data: dict) -> bool:
        """Atualiza um produto com dados coletados no deep scrape."""
        if not data:
            return True
        try:
            set_clauses = ", ".join(f"{k} = ?" for k in data)
            values = list(data.values()) + [product_id]
            await self._db.execute(
                f"UPDATE products SET {set_clauses}, synced = 0"  # noqa: S608
                f" WHERE id = ?",
                values,
            )
            await self._db.commit()
            logger.debug(
                "sqlite_enriched_data_updated", product_id=product_id
            )
            return True
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="update_enriched_data"
            ) from exc

    async def get_product_for_scoring(self, product_id: str) -> dict | None:
        """Retorna dados completos de um produto para scoring."""
        try:
            cursor = await self._db.execute(
                "SELECT * FROM products WHERE id = ?",
                (product_id,),
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="get_product_for_scoring"
            ) from exc

    async def get_products_needing_retry(
        self, max_attempts: int = 3, batch_size: int = 5
    ) -> list[dict]:
        """Retorna produtos com falha que ainda podem ser retentados."""
        try:
            cursor = await self._db.execute(
                """
                SELECT id, ml_id, product_url, enrichment_attempts
                FROM products
                WHERE enrichment_status = 'failed'
                  AND enrichment_attempts < ?
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (max_attempts, batch_size),
            )
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="get_products_needing_retry"
            ) from exc

    async def reset_stale_claims(self, stale_minutes: int = 30) -> int:
        """Reseta produtos in_progress por tempo demais (crash recovery)."""
        cutoff = (
            datetime.now(tz=timezone.utc) - timedelta(minutes=stale_minutes)
        ).isoformat()
        try:
            cursor = await self._db.execute(
                """
                UPDATE products
                SET enrichment_status = 'pending', synced = 0
                WHERE enrichment_status = 'in_progress'
                  AND last_seen_at < ?
                """,
                (cutoff,),
            )
            await self._db.commit()
            count = cursor.rowcount
            if count > 0:
                logger.info("sqlite_stale_claims_reset", count=count)
            return count
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="reset_stale_claims"
            ) from exc

    # ------------------------------------------------------------------
    # system_logs
    # ------------------------------------------------------------------

    async def log_event(self, event_type: str, details: Optional[dict] = None) -> bool:
        """Registra um evento operacional no banco local."""
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            await self._db.execute(
                "INSERT INTO system_logs (id, event_type, details, created_at)"
                " VALUES (?,?,?,?)",
                (
                    str(uuid.uuid4()),
                    event_type,
                    json.dumps(details or {}),
                    now,
                ),
            )
            await self._db.commit()
            return True
        except Exception as exc:
            raise SQLiteError(str(exc), operation="log_event") from exc

    async def get_recent_logs(
        self, event_type: Optional[str] = None, limit: int = 100
    ) -> list[dict]:
        """Retorna os logs mais recentes do banco local."""
        try:
            if event_type:
                cursor = await self._db.execute(
                    "SELECT event_type, details, created_at FROM system_logs"
                    " WHERE event_type=? ORDER BY created_at DESC LIMIT ?",
                    (event_type, limit),
                )
            else:
                cursor = await self._db.execute(
                    "SELECT event_type, details, created_at FROM system_logs"
                    " ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                )
            rows = await cursor.fetchall()
            result = []
            for row in rows:
                d = dict(row)
                d["details"] = json.loads(d.get("details") or "{}")
                result.append(d)
            return result
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_recent_logs") from exc

    # ------------------------------------------------------------------
    # sync_to_supabase
    # ------------------------------------------------------------------

    async def sync_to_supabase(self, client: SupabaseClient) -> dict:
        """
        Sincroniza registros locais (synced=0) com o Supabase.

        Ordem respeita dependências de FK:
          products → price_history → scored_offers → sent_offers → system_logs

        Returns:
            Dict {tabela: {synced: int, errors: int}}
        """
        logger.info("sqlite_sync_start")
        stats = {
            "products": await self._sync_table(client, "products", "ml_id"),
            "price_history": await self._sync_table(client, "price_history", "id"),
            "scored_offers": await self._sync_table(client, "scored_offers", "id"),
            "sent_offers": await self._sync_table(client, "sent_offers", "id"),
            "system_logs": await self._sync_logs_table(client),
        }
        total_synced = sum(v["synced"] for v in stats.values())
        total_errors = sum(v["errors"] for v in stats.values())
        logger.info(
            "sqlite_sync_done",
            total_synced=total_synced,
            total_errors=total_errors,
        )
        return stats

    async def _sync_table(
        self,
        client: SupabaseClient,
        table: str,
        conflict_col: str,
        limit: int = 500,
    ) -> dict:
        """Sincroniza uma tabela genérica (sem transformação de colunas)."""
        ok_ids: list[str] = []
        fail_ids: list[str] = []
        try:
            cursor = await self._db.execute(
                f"SELECT * FROM {table} WHERE synced=0 LIMIT ?",  # noqa: S608
                (limit,),
            )
            rows = await cursor.fetchall()
            for row in rows:
                row_id = str(row["id"])
                data = {k: row[k] for k in row.keys() if k != "synced"}
                try:
                    res = (
                        await client._db.table(table)
                        .upsert(data, on_conflict=conflict_col)
                        .execute()
                    )
                    if res.data:
                        ok_ids.append(row_id)
                    else:
                        fail_ids.append(row_id)
                except Exception:
                    fail_ids.append(row_id)

            for row_id in ok_ids:
                await self._db.execute(
                    f"UPDATE {table} SET synced=1 WHERE id=?",  # noqa: S608
                    (row_id,),
                )
            await self._db.commit()
        except Exception as exc:
            logger.error(f"sync_{table}_error", error=str(exc))
            fail_ids.append("outer_error")
        return {"synced": len(ok_ids), "errors": len(fail_ids)}

    async def _sync_logs_table(self, client: SupabaseClient) -> dict:
        """Sincroniza system_logs: deserializa details (TEXT→dict) antes."""
        ok_ids: list[str] = []
        fail_ids: list[str] = []
        try:
            cursor = await self._db.execute(
                "SELECT * FROM system_logs WHERE synced=0 LIMIT 500"
            )
            rows = await cursor.fetchall()
            for row in rows:
                row_id = str(row["id"])
                data = {k: row[k] for k in row.keys() if k != "synced"}
                data["details"] = json.loads(data.get("details") or "{}")
                try:
                    res = await client._db.table("system_logs").insert(data).execute()
                    if res.data:
                        ok_ids.append(row_id)
                    else:
                        fail_ids.append(row_id)
                except Exception:
                    fail_ids.append(row_id)

            for row_id in ok_ids:
                await self._db.execute(
                    "UPDATE system_logs SET synced=1 WHERE id=?",
                    (row_id,),
                )
            await self._db.commit()
        except Exception as exc:
            logger.error("sync_system_logs_error", error=str(exc))
            fail_ids.append("outer_error")
        return {"synced": len(ok_ids), "errors": len(fail_ids)}

    # ------------------------------------------------------------------
    # Métricas locais
    # ------------------------------------------------------------------

    async def get_unsynced_count(self) -> dict[str, int]:
        """Retorna quantos registros estão pendentes de sincronização."""
        tables = [
            "products",
            "price_history",
            "scored_offers",
            "sent_offers",
            "system_logs",
        ]
        counts: dict[str, int] = {}
        for table in tables:
            try:
                sql = f"SELECT COUNT(*) FROM {table} WHERE synced=0"  # noqa: S608
                cursor = await self._db.execute(sql)
                row = await cursor.fetchone()
                counts[table] = row[0] if row else 0
            except Exception:
                counts[table] = -1
        return counts
