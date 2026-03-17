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
from .seeds import BADGES, CATEGORIES, MARKETPLACES

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
        CREATE TABLE IF NOT EXISTS badges (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL UNIQUE,
            created_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS categories (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL UNIQUE,
            created_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS marketplaces (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL UNIQUE,
            created_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS products (
            id                TEXT PRIMARY KEY,
            ml_id             TEXT NOT NULL UNIQUE,
            title             TEXT NOT NULL,
            current_price     REAL NOT NULL,
            original_price    REAL,
            discount_percent  INTEGER DEFAULT 0,
            rating_stars      REAL DEFAULT 0,
            rating_count      INTEGER DEFAULT 0,
            free_shipping     INTEGER DEFAULT 0,
            installments_without_interest INTEGER DEFAULT 0,
            thumbnail_url     TEXT DEFAULT '',
            product_url       TEXT DEFAULT '',
            category_id       TEXT REFERENCES categories(id),
            badge_id          TEXT REFERENCES badges(id),
            marketplace_id    TEXT REFERENCES marketplaces(id),
            first_seen_at     TEXT DEFAULT (datetime('now')),
            last_seen_at      TEXT DEFAULT (datetime('now')),
            created_at        TEXT DEFAULT (datetime('now')),
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
            final_score    INTEGER NOT NULL,
            status         TEXT NOT NULL DEFAULT 'pending',
            scored_at      TEXT DEFAULT (datetime('now')),
            synced         INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS sent_offers (
            id              TEXT PRIMARY KEY,
            scored_offer_id TEXT NOT NULL
                                REFERENCES scored_offers(id) ON DELETE CASCADE,
            channel          TEXT NOT NULL,
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
        CREATE INDEX IF NOT EXISTS idx_p_category_id ON products(category_id);
        CREATE INDEX IF NOT EXISTS idx_p_badge_id ON products(badge_id);
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

        CREATE TABLE IF NOT EXISTS users (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            affiliate_tag   TEXT NOT NULL,
            email           TEXT,
            password_hash   TEXT,
            ml_cookies      TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            synced          INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_u_tag ON users(affiliate_tag);

        CREATE TABLE IF NOT EXISTS affiliate_links (
            id              TEXT PRIMARY KEY,
            product_id      TEXT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            short_url       TEXT NOT NULL,
            long_url        TEXT,
            ml_link_id      TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            synced          INTEGER DEFAULT 0,
            UNIQUE (product_id, user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_al_product ON affiliate_links(product_id);
        CREATE INDEX IF NOT EXISTS idx_al_user ON affiliate_links(user_id);
        CREATE INDEX IF NOT EXISTS idx_al_synced ON affiliate_links(synced);

        -- Views (SQLite usa datetime() em vez de NOW() - INTERVAL)
        CREATE VIEW IF NOT EXISTS vw_approved_unsent AS
        SELECT
            p.id            AS product_id,
            p.ml_id,
            p.title,
            p.current_price,
            p.original_price,
            p.discount_percent,
            p.free_shipping,
            p.thumbnail_url,
            p.product_url,
            c.name          AS category,
            so.id           AS scored_offer_id,
            so.final_score,
            so.scored_at
        FROM scored_offers so
        JOIN products p ON p.id = so.product_id
        LEFT JOIN categories c ON c.id = p.category_id
        WHERE so.status = 'approved'
          AND so.final_score >= 60
          AND NOT EXISTS (
              SELECT 1 FROM sent_offers se
              WHERE se.scored_offer_id = so.id
                AND se.sent_at >= datetime('now', '-24 hours')
          )
        ORDER BY so.final_score DESC;

        CREATE VIEW IF NOT EXISTS vw_last_24h_summary AS
        SELECT
            (SELECT COUNT(*) FROM products      WHERE last_seen_at >= datetime('now', '-24 hours')) AS products_scraped,
            (SELECT COUNT(*) FROM scored_offers WHERE scored_at   >= datetime('now', '-24 hours')) AS offers_scored,
            (SELECT COUNT(*) FROM scored_offers WHERE scored_at   >= datetime('now', '-24 hours')
                                                 AND status = 'approved')                          AS offers_approved,
            (SELECT COUNT(*) FROM sent_offers   WHERE sent_at     >= datetime('now', '-24 hours')) AS offers_sent,
            (SELECT ROUND(AVG(final_score), 1)
               FROM scored_offers WHERE scored_at >= datetime('now', '-24 hours'))                 AS avg_score,
            (SELECT MAX(discount_percent)
               FROM products WHERE last_seen_at  >= datetime('now', '-24 hours'))                  AS max_discount_pct;

        CREATE VIEW IF NOT EXISTS vw_top_deals AS
        SELECT
            p.ml_id,
            p.title,
            p.current_price,
            p.original_price,
            p.discount_percent,
            p.free_shipping,
            c.name          AS category,
            so.final_score,
            p.product_url
        FROM products p
        JOIN scored_offers so ON so.product_id = p.id
        LEFT JOIN categories c ON c.id = p.category_id
        WHERE p.last_seen_at >= datetime('now', '-6 hours')
          AND so.status = 'approved'
        ORDER BY so.final_score DESC, p.discount_percent DESC
        LIMIT 20;
        """
        await self._db.executescript(schema)
        await self._db.commit()

        # Migrações incrementais para bancos já existentes
        for col, ref in [
            ("badge_id", "TEXT REFERENCES badges(id)"),
            ("category_id", "TEXT REFERENCES categories(id)"),
            ("marketplace_id", "TEXT REFERENCES marketplaces(id)"),
            ("installments_without_interest", "INTEGER DEFAULT 0"),
        ]:
            try:
                await self._db.execute(f"ALTER TABLE products ADD COLUMN {col} {ref}")
                await self._db.commit()
            except Exception:
                pass  # Coluna já existe

        # Índice criado aqui (e não no executescript) pois depende da
        # migração incremental que adiciona marketplace_id em bancos já existentes
        try:
            await self._db.execute(
                "CREATE INDEX IF NOT EXISTS idx_p_marketplace_id ON products(marketplace_id)"
            )
            await self._db.commit()
        except Exception:
            pass

        # Migrações incrementais para a tabela users
        for col, definition in [
            ("email", "TEXT"),
            ("password_hash", "TEXT"),
        ]:
            try:
                await self._db.execute(f"ALTER TABLE users ADD COLUMN {col} {definition}")
                await self._db.commit()
            except Exception:
                pass  # Coluna já existe

        # Índice único em users.email (criado após migration que adiciona a coluna)
        try:
            await self._db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_u_email ON users(email) WHERE email IS NOT NULL"
            )
            await self._db.commit()
        except Exception:
            pass

        # Migrações incrementais — image worker
        for col, definition in [
            ("enhanced_image_url", "TEXT"),
            ("image_status", "TEXT DEFAULT 'pending'"),
        ]:
            try:
                await self._db.execute(f"ALTER TABLE products ADD COLUMN {col} {definition}")
                await self._db.commit()
            except Exception:
                pass  # Coluna já existe

        try:
            await self._db.execute(
                "CREATE INDEX IF NOT EXISTS idx_p_image_status ON products(image_status)"
            )
            await self._db.commit()
        except Exception:
            pass

        # Remoção de colunas obsoletas (Shlink removido)
        try:
            await self._db.execute("ALTER TABLE sent_offers DROP COLUMN shlink_short_url")
            await self._db.commit()
        except Exception:
            pass  # Coluna já removida ou nunca existiu

        # Seed de dados canônicos (idempotente)
        await self._seed_lookup_tables()

    async def _seed_lookup_tables(self) -> None:
        """Insere badges, categories e marketplaces canônicos definidos em seeds.py."""
        try:
            for name in BADGES:
                await self._db.execute(
                    "INSERT OR IGNORE INTO badges (id, name) VALUES (?, ?)",
                    (str(uuid.uuid4()), name),
                )
            for name in CATEGORIES:
                await self._db.execute(
                    "INSERT OR IGNORE INTO categories (id, name) VALUES (?, ?)",
                    (str(uuid.uuid4()), name),
                )
            for name in MARKETPLACES:
                await self._db.execute(
                    "INSERT OR IGNORE INTO marketplaces (id, name) VALUES (?, ?)",
                    (str(uuid.uuid4()), name),
                )
            await self._db.commit()
            logger.debug(
                "sqlite_seeds_applied",
                badges=len(BADGES),
                categories=len(CATEGORIES),
                marketplaces=len(MARKETPLACES),
            )
        except Exception as exc:
            logger.warning("sqlite_seed_failed", error=str(exc))

    async def sync_lookup_ids(
        self,
        remote_badges: dict[str, str],
        remote_categories: dict[str, str],
        remote_marketplaces: dict[str, str] | None = None,
    ) -> None:
        """Atualiza UUIDs locais de badges/categories/marketplaces para igualar os do Supabase.

        Isso garante que badge_id, category_id e marketplace_id resolvidos pelo
        Supabase funcionem como FK válida no SQLite (ambos usam o mesmo UUID).

        Também atualiza referências em products (CASCADE manual).

        Args:
            remote_badges: Mapeamento {nome: uuid_supabase} dos badges.
            remote_categories: Mapeamento {nome: uuid_supabase} das categorias.
            remote_marketplaces: Mapeamento {nome: uuid_supabase} dos marketplaces.
        """
        if remote_marketplaces is None:
            remote_marketplaces = {}
        try:
            # Sync badges
            for name, remote_id in remote_badges.items():
                cursor = await self._db.execute(
                    "SELECT id FROM badges WHERE name = ?", (name,)
                )
                row = await cursor.fetchone()
                if row and row["id"] != remote_id:
                    local_id = row["id"]
                    await self._db.execute(
                        "UPDATE products SET badge_id = ? WHERE badge_id = ?",
                        (remote_id, local_id),
                    )
                    await self._db.execute(
                        "UPDATE badges SET id = ? WHERE name = ?",
                        (remote_id, name),
                    )
                elif not row:
                    await self._db.execute(
                        "INSERT INTO badges (id, name) VALUES (?, ?)",
                        (remote_id, name),
                    )

            # Sync categories
            for name, remote_id in remote_categories.items():
                cursor = await self._db.execute(
                    "SELECT id FROM categories WHERE name = ?", (name,)
                )
                row = await cursor.fetchone()
                if row and row["id"] != remote_id:
                    local_id = row["id"]
                    await self._db.execute(
                        "UPDATE products SET category_id = ? WHERE category_id = ?",
                        (remote_id, local_id),
                    )
                    await self._db.execute(
                        "UPDATE categories SET id = ? WHERE name = ?",
                        (remote_id, name),
                    )
                elif not row:
                    await self._db.execute(
                        "INSERT INTO categories (id, name) VALUES (?, ?)",
                        (remote_id, name),
                    )

            # Sync marketplaces
            for name, remote_id in remote_marketplaces.items():
                cursor = await self._db.execute(
                    "SELECT id FROM marketplaces WHERE name = ?", (name,)
                )
                row = await cursor.fetchone()
                if row and row["id"] != remote_id:
                    local_id = row["id"]
                    await self._db.execute(
                        "UPDATE products SET marketplace_id = ? WHERE marketplace_id = ?",
                        (remote_id, local_id),
                    )
                    await self._db.execute(
                        "UPDATE marketplaces SET id = ? WHERE name = ?",
                        (remote_id, name),
                    )
                elif not row:
                    await self._db.execute(
                        "INSERT INTO marketplaces (id, name) VALUES (?, ?)",
                        (remote_id, name),
                    )

            await self._db.commit()
            logger.debug(
                "sqlite_lookup_ids_synced",
                badges=len(remote_badges),
                categories=len(remote_categories),
                marketplaces=len(remote_marketplaces),
            )
        except Exception as exc:
            logger.warning("sqlite_lookup_sync_failed", error=str(exc))

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
    # badges
    # ------------------------------------------------------------------

    async def get_all_badges(self) -> dict[str, str]:
        """Retorna todos os badges como {nome: uuid}."""
        try:
            cursor = await self._db.execute("SELECT id, name FROM badges")
            rows = await cursor.fetchall()
            return {row["name"]: row["id"] for row in rows}
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_all_badges") from exc

    async def get_or_create_badge(self, name: str) -> Optional[str]:
        """Retorna o ID do badge pelo nome. Cria se não existir."""
        if not name:
            return None
        try:
            cursor = await self._db.execute(
                "SELECT id FROM badges WHERE name = ?", (name,)
            )
            row = await cursor.fetchone()
            if row:
                return row["id"]
            badge_id = str(uuid.uuid4())
            await self._db.execute(
                "INSERT INTO badges (id, name) VALUES (?, ?)",
                (badge_id, name),
            )
            await self._db.commit()
            logger.debug("sqlite_badge_created", name=name, badge_id=badge_id)
            return badge_id
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_or_create_badge") from exc

    async def ensure_badge_id(self, name: str, badge_id: str) -> None:
        """Garante que o badge exista no SQLite com o UUID especificado (do Supabase).

        Usa INSERT OR IGNORE para não sobrescrever se já existir com outro ID.
        Se existir com ID diferente, atualiza para usar o ID remoto (sync).
        """
        try:
            cursor = await self._db.execute(
                "SELECT id FROM badges WHERE name = ?", (name,)
            )
            row = await cursor.fetchone()
            if not row:
                await self._db.execute(
                    "INSERT OR IGNORE INTO badges (id, name) VALUES (?, ?)",
                    (badge_id, name),
                )
                await self._db.commit()
            elif row["id"] != badge_id:
                local_id = row["id"]
                await self._db.execute(
                    "UPDATE products SET badge_id = ? WHERE badge_id = ?",
                    (badge_id, local_id),
                )
                await self._db.execute(
                    "UPDATE badges SET id = ? WHERE name = ?",
                    (badge_id, name),
                )
                await self._db.commit()
        except Exception as exc:
            logger.warning("sqlite_ensure_badge_id_failed", name=name, error=str(exc))

    # ------------------------------------------------------------------
    # categories
    # ------------------------------------------------------------------

    async def get_all_categories(self) -> dict[str, str]:
        """Retorna todas as categorias como {nome: uuid}."""
        try:
            cursor = await self._db.execute("SELECT id, name FROM categories")
            rows = await cursor.fetchall()
            return {row["name"]: row["id"] for row in rows}
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_all_categories") from exc

    async def get_or_create_category(self, name: str) -> Optional[str]:
        """Retorna o ID da categoria pelo nome. Cria se não existir."""
        if not name:
            return None
        try:
            cursor = await self._db.execute(
                "SELECT id FROM categories WHERE name = ?", (name,)
            )
            row = await cursor.fetchone()
            if row:
                return row["id"]
            cat_id = str(uuid.uuid4())
            await self._db.execute(
                "INSERT INTO categories (id, name) VALUES (?, ?)",
                (cat_id, name),
            )
            await self._db.commit()
            logger.debug("sqlite_category_created", name=name, category_id=cat_id)
            return cat_id
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_or_create_category") from exc

    async def ensure_category_id(self, name: str, category_id: str) -> None:
        """Garante que a categoria exista no SQLite com o UUID especificado (do Supabase).

        Mesma lógica do ensure_badge_id: sincroniza o UUID local com o remoto.
        """
        try:
            cursor = await self._db.execute(
                "SELECT id FROM categories WHERE name = ?", (name,)
            )
            row = await cursor.fetchone()
            if not row:
                await self._db.execute(
                    "INSERT OR IGNORE INTO categories (id, name) VALUES (?, ?)",
                    (category_id, name),
                )
                await self._db.commit()
            elif row["id"] != category_id:
                local_id = row["id"]
                await self._db.execute(
                    "UPDATE products SET category_id = ? WHERE category_id = ?",
                    (category_id, local_id),
                )
                await self._db.execute(
                    "UPDATE categories SET id = ? WHERE name = ?",
                    (category_id, name),
                )
                await self._db.commit()
        except Exception as exc:
            logger.warning(
                "sqlite_ensure_category_id_failed", name=name, error=str(exc)
            )

    async def get_all_marketplaces(self) -> dict[str, str]:
        """Retorna todos os marketplaces como {nome: uuid}."""
        try:
            cursor = await self._db.execute("SELECT id, name FROM marketplaces")
            rows = await cursor.fetchall()
            return {row["name"]: row["id"] for row in rows}
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_all_marketplaces") from exc

    async def get_or_create_marketplace(self, name: str) -> Optional[str]:
        """Retorna o ID do marketplace pelo nome. Cria se não existir."""
        if not name:
            return None
        try:
            cursor = await self._db.execute(
                "SELECT id FROM marketplaces WHERE name = ?", (name,)
            )
            row = await cursor.fetchone()
            if row:
                return row["id"]
            mp_id = str(uuid.uuid4())
            await self._db.execute(
                "INSERT INTO marketplaces (id, name) VALUES (?, ?)",
                (mp_id, name),
            )
            await self._db.commit()
            logger.debug("sqlite_marketplace_created", name=name, marketplace_id=mp_id)
            return mp_id
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_or_create_marketplace") from exc

    async def ensure_marketplace_id(self, name: str, marketplace_id: str) -> None:
        """Garante que o marketplace exista no SQLite com o UUID especificado (do Supabase)."""
        try:
            cursor = await self._db.execute(
                "SELECT id FROM marketplaces WHERE name = ?", (name,)
            )
            row = await cursor.fetchone()
            if not row:
                await self._db.execute(
                    "INSERT OR IGNORE INTO marketplaces (id, name) VALUES (?, ?)",
                    (marketplace_id, name),
                )
                await self._db.commit()
            elif row["id"] != marketplace_id:
                local_id = row["id"]
                await self._db.execute(
                    "UPDATE products SET marketplace_id = ? WHERE marketplace_id = ?",
                    (marketplace_id, local_id),
                )
                await self._db.execute(
                    "UPDATE marketplaces SET id = ? WHERE name = ?",
                    (marketplace_id, name),
                )
                await self._db.commit()
        except Exception as exc:
            logger.warning(
                "sqlite_ensure_marketplace_id_failed", name=name, error=str(exc)
            )

    # ------------------------------------------------------------------
    # products
    # ------------------------------------------------------------------

    async def upsert_product(
        self,
        product: ScrapedProduct,
        product_id: Optional[str] = None,
        badge_id: Optional[str] = None,
        category_id: Optional[str] = None,
        marketplace_id: Optional[str] = None,
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
            badge_id: UUID do badge (resolvido externamente).
            category_id: UUID da categoria (resolvido externamente).

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
                        discount_percent=?,
                        rating_stars=?, rating_count=?,
                        free_shipping=?, installments_without_interest=?, thumbnail_url=?,
                        product_url=?, category_id=?, badge_id=?, marketplace_id=?,
                        first_seen_at=?, last_seen_at=?, synced=0
                    WHERE ml_id=?
                    """,
                    (
                        product.title,
                        product.price,
                        product.original_price,
                        int(product.discount_pct),
                        product.rating,
                        product.review_count,
                        int(product.free_shipping),
                        int(product.installments_without_interest),
                        product.image_url,
                        product.url,
                        category_id,
                        badge_id,
                        marketplace_id,
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
                        discount_percent,
                        rating_stars, rating_count,
                        free_shipping, installments_without_interest, thumbnail_url, product_url,
                        category_id, badge_id, marketplace_id,
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
                        product.rating,
                        product.review_count,
                        int(product.free_shipping),
                        int(product.installments_without_interest),
                        product.image_url,
                        product.url,
                        category_id,
                        badge_id,
                        marketplace_id,
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

    async def check_duplicates_batch(self, ml_ids: list[str]) -> set[str]:
        """Retorna set dos ml_ids que já existem no banco local (1 query)."""
        if not ml_ids:
            return set()
        try:
            placeholders = ",".join("?" * len(ml_ids))
            cursor = await self._db.execute(
                f"SELECT ml_id FROM products WHERE ml_id IN ({placeholders})",  # noqa: S608
                ml_ids,
            )
            rows = await cursor.fetchall()
            return {row["ml_id"] for row in rows}
        except Exception as exc:
            raise SQLiteError(str(exc), operation="check_duplicates_batch") from exc

    async def upsert_products_batch(
        self,
        products: list[ScrapedProduct],
        product_ids: dict[str, str] | None = None,
        badge_ids: dict[str, str | None] | None = None,
        category_ids: dict[str, str | None] | None = None,
        marketplace_ids: dict[str, str | None] | None = None,
    ) -> dict[str, str]:
        """
        Upsert de múltiplos produtos em 1 transação (1 commit).
        Retorna dict mapeando ml_id → UUID.
        Deduplicates by ml_id (keeps last occurrence).

        Args:
            products: Lista de produtos a inserir/atualizar.
            product_ids: Mapa ml_id→UUID do Supabase (para manter FKs consistentes).
        """
        if not products:
            return {}

        # Deduplica por ml_id para evitar UNIQUE constraint violation
        seen: dict[str, ScrapedProduct] = {}
        for p in products:
            seen[p.ml_id] = p
        unique_products = list(seen.values())

        now = datetime.now(tz=timezone.utc).isoformat()
        result_ids: dict[str, str] = {}
        ids_map = product_ids or {}
        badges_map = badge_ids or {}
        cats_map = category_ids or {}
        mps_map = marketplace_ids or {}

        try:
            # Busca todos os existentes em 1 query
            ml_ids = [p.ml_id for p in unique_products]
            placeholders = ",".join("?" * len(ml_ids))
            cursor = await self._db.execute(
                f"SELECT id, ml_id, first_seen_at FROM products WHERE ml_id IN ({placeholders})",  # noqa: S608, E501
                ml_ids,
            )
            existing = {row["ml_id"]: dict(row) for row in await cursor.fetchall()}

            for p in unique_products:
                b_id = badges_map.get(p.ml_id)
                c_id = cats_map.get(p.ml_id)
                mp_id = mps_map.get(p.ml_id)
                if p.ml_id in existing:
                    pid = existing[p.ml_id]["id"]
                    first_seen = existing[p.ml_id]["first_seen_at"]
                    await self._db.execute(
                        """
                        UPDATE products SET
                            title=?, current_price=?, original_price=?,
                            discount_percent=?,
                            rating_stars=?, rating_count=?,
                            free_shipping=?, installments_without_interest=?, thumbnail_url=?,
                            product_url=?, category_id=?, badge_id=?, marketplace_id=?,
                            first_seen_at=?, last_seen_at=?, synced=0
                        WHERE ml_id=?
                        """,
                        (
                            p.title,
                            p.price,
                            p.original_price,
                            int(p.discount_pct),
                            p.rating,
                            p.review_count,
                            int(p.free_shipping),
                            int(p.installments_without_interest),
                            p.image_url,
                            p.url,
                            c_id,
                            b_id,
                            mp_id,
                            first_seen,
                            now,
                            p.ml_id,
                        ),
                    )
                    result_ids[p.ml_id] = pid
                else:
                    pid = ids_map.get(p.ml_id) or str(uuid.uuid4())
                    await self._db.execute(
                        """
                        INSERT INTO products (
                            id, ml_id, title, current_price,
                            original_price, discount_percent,
                            rating_stars, rating_count,
                            free_shipping, installments_without_interest, thumbnail_url,
                            product_url, category_id, badge_id, marketplace_id,
                            first_seen_at, last_seen_at
                        ) VALUES (
                            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                        )
                        """,
                        (
                            pid,
                            p.ml_id,
                            p.title,
                            p.price,
                            p.original_price,
                            int(p.discount_pct),
                            p.rating,
                            p.review_count,
                            int(p.free_shipping),
                            int(p.installments_without_interest),
                            p.image_url,
                            p.url,
                            c_id,
                            b_id,
                            mp_id,
                            now,
                            now,
                        ),
                    )
                    result_ids[p.ml_id] = pid

            await self._db.commit()  # 1 commit para tudo
            logger.debug("sqlite_products_batch_upserted", count=len(unique_products))
            return result_ids

        except Exception as exc:
            raise SQLiteError(str(exc), operation="upsert_products_batch") from exc

    async def add_price_history_batch(self, entries: list[dict]) -> bool:
        """Insere múltiplas entradas de histórico de preço em 1 commit."""
        if not entries:
            return True
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            await self._db.executemany(
                """
                INSERT INTO price_history
                    (id, product_id, price, original_price, recorded_at)
                VALUES (?,?,?,?,?)
                """,
                [
                    (
                        str(uuid.uuid4()),
                        e["product_id"],
                        e["price"],
                        e["original_price"],
                        now,
                    )
                    for e in entries
                ],
            )
            await self._db.commit()
            logger.debug("sqlite_price_history_batch_added", count=len(entries))
            return True
        except Exception as exc:
            raise SQLiteError(str(exc), operation="add_price_history_batch") from exc

    async def product_exists(self, product_id: str) -> bool:
        """Verifica se um produto existe no banco local pelo UUID."""
        try:
            cursor = await self._db.execute(
                "SELECT 1 FROM products WHERE id = ?", (product_id,)
            )
            return await cursor.fetchone() is not None
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="product_exists", ml_id=product_id
            ) from exc

    async def insert_product_from_dict(self, data: dict) -> None:
        """Insere um produto a partir de um dict (ex: dados do Supabase). Ignora se já existir."""
        try:
            await self._db.execute(
                """
                INSERT OR IGNORE INTO products (
                    id, ml_id, title, current_price, original_price,
                    discount_percent,
                    rating_stars, rating_count,
                    free_shipping, installments_without_interest, thumbnail_url, product_url,
                    category_id, badge_id,
                    first_seen_at, last_seen_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(data.get("id", "")),
                    str(data.get("ml_id", "")),
                    str(data.get("title", "")),
                    float(data.get("current_price", 0)),
                    data.get("original_price"),
                    int(data.get("discount_percent", 0)),
                    float(data.get("rating_stars", 0)),
                    int(data.get("rating_count", 0)),
                    int(bool(data.get("free_shipping", False))),
                    int(bool(data.get("installments_without_interest", False))),
                    str(data.get("thumbnail_url", "")),
                    str(data.get("product_url", "")),
                    data.get("category_id"),
                    data.get("badge_id"),
                    str(data.get("first_seen_at", "")),
                    str(data.get("last_seen_at", "")),
                ),
            )
            await self._db.commit()
            logger.debug("sqlite_product_inserted_from_dict", product_id=data.get("id"))
        except Exception as exc:
            raise SQLiteError(str(exc), operation="insert_product_from_dict") from exc

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

    async def get_last_prices_batch(
        self, product_ids: list[str]
    ) -> dict[str, tuple[float, float | None]]:
        """
        Retorna o último preço registrado para cada product_id.
        Usado para evitar gravar entradas duplicadas em price_history.

        Returns:
            {product_id: (price, original_price)} — apenas para produtos com histórico.
        """
        if not product_ids:
            return {}
        placeholders = ",".join("?" * len(product_ids))
        try:
            cursor = await self._db.execute(
                f"""
                SELECT ph.product_id, ph.price, ph.original_price
                FROM price_history ph
                INNER JOIN (
                    SELECT product_id, MAX(recorded_at) AS max_at
                    FROM price_history
                    WHERE product_id IN ({placeholders})
                    GROUP BY product_id
                ) latest
                ON ph.product_id = latest.product_id
                AND ph.recorded_at = latest.max_at
                """,  # noqa: S608
                product_ids,
            )
            rows = await cursor.fetchall()
            return {row["product_id"]: (row["price"], row["original_price"]) for row in rows}
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_last_prices_batch") from exc

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
        offer_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Salva o resultado da análise de uma oferta.

        Args:
            offer_id: UUID pré-definido (ex: vindo do Supabase) para manter
                      FKs consistentes entre os bancos.

        Returns:
            UUID do scored_offer criado, ou None em caso de erro.
        """
        row_id = offer_id or str(uuid.uuid4())
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            await self._db.execute(
                """
                INSERT INTO scored_offers (
                    id, product_id, rule_score, final_score, status, scored_at
                ) VALUES (?,?,?,?,?,?)
                """,
                (
                    row_id,
                    product_id,
                    rule_score,
                    final_score,
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

    async def save_scored_offers_batch(
        self,
        entries: list[dict],
        offer_ids: list[str] | None = None,
    ) -> list[str]:
        """
        Insere múltiplas scored_offers em 1 transação (1 commit).

        entries: lista de dicts com keys:
            product_id, rule_score, final_score, status.
        offer_ids: lista de UUIDs pré-definidos (ex: vindos do Supabase).
            Se fornecida, deve ter o mesmo tamanho de entries.

        Returns:
            Lista de UUIDs gerados para cada entrada.
        """
        if not entries:
            return []
        now = datetime.now(tz=timezone.utc).isoformat()
        ids: list[str] = []
        try:
            rows = []
            for i, e in enumerate(entries):
                row_id = (offer_ids[i] if offer_ids else None) or str(uuid.uuid4())
                ids.append(row_id)
                rows.append(
                    (
                        row_id,
                        e["product_id"],
                        e["rule_score"],
                        e["final_score"],
                        e["status"],
                        now,
                    )
                )
            await self._db.executemany(
                """
                INSERT OR IGNORE INTO scored_offers (
                    id, product_id, rule_score, final_score, status, scored_at
                ) VALUES (?,?,?,?,?,?)
                """,
                rows,
            )
            await self._db.commit()
            logger.debug("sqlite_scored_offers_batch_saved", count=len(ids))
            return ids
        except Exception as exc:
            raise SQLiteError(str(exc), operation="save_scored_offers_batch") from exc

    # ------------------------------------------------------------------
    # sent_offers
    # ------------------------------------------------------------------

    async def has_recent_sends(self, hours: int = 24) -> bool:
        """Verifica rapidamente se há ALGUM envio nas últimas N horas (1 query)."""
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).isoformat()
        try:
            cursor = await self._db.execute(
                "SELECT 1 FROM sent_offers WHERE sent_at >= ? LIMIT 1",
                (cutoff,),
            )
            return await cursor.fetchone() is not None
        except Exception as exc:
            raise SQLiteError(str(exc), operation="has_recent_sends") from exc

    async def get_recently_sent_ids(self, hours: int = 24) -> set[str]:
        """
        Retorna o conjunto de ml_ids enviados nas últimas N horas (1 query batch).
        Substitui N chamadas a was_recently_sent() por uma única query.
        """
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=hours)).isoformat()
        try:
            cursor = await self._db.execute(
                """
                SELECT DISTINCT p.ml_id
                FROM sent_offers se
                JOIN scored_offers so ON so.id = se.scored_offer_id
                JOIN products p       ON p.id  = so.product_id
                WHERE se.sent_at >= ?
                """,
                (cutoff,),
            )
            rows = await cursor.fetchall()
            return {row[0] for row in rows}
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_recently_sent_ids") from exc

    async def mark_as_sent(
        self,
        scored_offer_id: str,
        channel: str,
    ) -> bool:
        """Registra o envio de uma oferta para um canal."""
        now = datetime.now(tz=timezone.utc).isoformat()
        try:
            await self._db.execute(
                """
                INSERT INTO sent_offers
                    (id, scored_offer_id, channel, sent_at)
                VALUES (?,?,?,?)
                """,
                (
                    str(uuid.uuid4()),
                    scored_offer_id,
                    channel,
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

    async def get_next_unsent_offer(self) -> dict | None:
        """
        Retorna a oferta aprovada de maior score ainda não enviada (LIMIT 1).
        Equivalente SQLite da view vw_approved_unsent.
        """
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=24)).isoformat()
        try:
            cursor = await self._db.execute(
                """
                SELECT
                    p.id            AS product_id,
                    p.ml_id,
                    p.title,
                    p.current_price,
                    p.original_price,
                    p.discount_percent,
                    p.free_shipping,
                    p.thumbnail_url,
                    p.product_url,
                    p.rating_stars,
                    p.rating_count,
                    p.installments_without_interest,
                    c.name          AS category,
                    b.name          AS badge,
                    so.id           AS scored_offer_id,
                    so.final_score,
                    so.scored_at
                FROM scored_offers so
                JOIN products p ON p.id = so.product_id
                LEFT JOIN categories c ON c.id = p.category_id
                LEFT JOIN badges b ON b.id = p.badge_id
                WHERE so.status = 'approved'
                  AND so.final_score >= 60
                  AND NOT EXISTS (
                      SELECT 1 FROM sent_offers se
                      WHERE se.scored_offer_id = so.id
                        AND se.sent_at >= ?
                  )
                ORDER BY so.final_score DESC
                LIMIT 1
                """,
                (cutoff,),
            )
            row = await cursor.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cursor.description]
            return dict(zip(cols, row))
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_next_unsent_offer") from exc

    async def get_pending_scored_offers(self, limit: int = 50) -> list[dict]:
        """
        Retorna ofertas aprovadas ainda não enviadas.
        Equivalente SQLite da view vw_approved_unsent.
        """
        try:
            cursor = await self._db.execute(
                "SELECT * FROM vw_approved_unsent LIMIT ?",
                (limit,)
            )
            rows = await cursor.fetchall()
            if not rows:
                return []
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in rows]
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_pending_scored_offers") from exc

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
            "scored_offers": await self._sync_table(client, "scored_offers", "product_id"),
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
        chunk_size: int = 50,
    ) -> dict:
        """
        Sincroniza uma tabela genérica em batch (chunks).

        Para a tabela 'products', re-resolve badge_id e category_id
        usando os nomes das tabelas de lookup (os UUIDs do SQLite são
        diferentes dos UUIDs do Supabase).
        """
        ok_ids: list[str] = []
        fail_ids: list[str] = []
        try:
            cursor = await self._db.execute(
                f"SELECT * FROM {table} WHERE synced=0 LIMIT ?",  # noqa: S608
                (limit,),
            )
            rows = await cursor.fetchall()
            if not rows:
                return {"synced": 0, "errors": 0}

            # Prepara os dados removendo a coluna 'synced'
            all_data: list[tuple[str, dict]] = []
            for row in rows:
                row_id = str(row["id"])
                data = {k: row[k] for k in row.keys() if k != "synced"}
                all_data.append((row_id, data))

            # Para products: re-resolver FKs de badge/category pelo nome
            if table == "products":
                await self._resolve_product_fks_for_sync(client, all_data)

            # Envio em chunks (batch upsert)
            for i in range(0, len(all_data), chunk_size):
                chunk = all_data[i : i + chunk_size]
                chunk_rows = [data for _, data in chunk]
                chunk_ids = [row_id for row_id, _ in chunk]
                try:
                    res = (
                        await client._db.table(table)
                        .upsert(chunk_rows, on_conflict=conflict_col)
                        .execute()
                    )
                    if res.data:
                        ok_ids.extend(chunk_ids)
                    else:
                        fail_ids.extend(chunk_ids)
                except Exception as exc:
                    logger.warning(
                        f"sync_{table}_chunk_error",
                        error=str(exc),
                        chunk_start=i,
                    )
                    fail_ids.extend(chunk_ids)

            # Marca como sincronizados em batch
            if ok_ids:
                placeholders = ",".join("?" * len(ok_ids))
                await self._db.execute(
                    f"UPDATE {table} SET synced=1 WHERE id IN ({placeholders})",  # noqa: S608
                    ok_ids,
                )
                await self._db.commit()

        except Exception as exc:
            logger.error(f"sync_{table}_error", error=str(exc))
            fail_ids.append("outer_error")
        return {"synced": len(ok_ids), "errors": len(fail_ids)}

    async def _resolve_product_fks_for_sync(
        self,
        client: SupabaseClient,
        data_list: list[tuple[str, dict]],
    ) -> None:
        """
        Re-resolve badge_id e category_id pelo nome antes de enviar ao Supabase.

        Os UUIDs de lookup no SQLite são diferentes dos do Supabase.
        Busca os nomes via JOIN local e resolve para os UUIDs do Supabase.
        """
        # Coleta badge_ids e category_ids locais usados
        local_badge_ids = {d["badge_id"] for _, d in data_list if d.get("badge_id")}
        local_cat_ids = {d["category_id"] for _, d in data_list if d.get("category_id")}

        # Mapeia UUID local → nome (via SQLite)
        badge_local_to_name: dict[str, str] = {}
        if local_badge_ids:
            placeholders = ",".join("?" * len(local_badge_ids))
            cursor = await self._db.execute(
                f"SELECT id, name FROM badges WHERE id IN ({placeholders})",  # noqa: S608
                list(local_badge_ids),
            )
            for row in await cursor.fetchall():
                badge_local_to_name[row["id"]] = row["name"]

        cat_local_to_name: dict[str, str] = {}
        if local_cat_ids:
            placeholders = ",".join("?" * len(local_cat_ids))
            cursor = await self._db.execute(
                f"SELECT id, name FROM categories WHERE id IN ({placeholders})",  # noqa: S608
                list(local_cat_ids),
            )
            for row in await cursor.fetchall():
                cat_local_to_name[row["id"]] = row["name"]

        # Resolve nomes → UUIDs do Supabase (com cache)
        badge_name_to_remote: dict[str, str] = {}
        for name in set(badge_local_to_name.values()):
            try:
                remote_id = await client.get_or_create_badge(name)
                if remote_id:
                    badge_name_to_remote[name] = remote_id
            except Exception:
                pass

        cat_name_to_remote: dict[str, str] = {}
        for name in set(cat_local_to_name.values()):
            try:
                remote_id = await client.get_or_create_category(name)
                if remote_id:
                    cat_name_to_remote[name] = remote_id
            except Exception:
                pass

        # Substitui UUIDs locais pelos UUIDs do Supabase nos dados
        for _, data in data_list:
            local_bid = data.get("badge_id")
            if local_bid and local_bid in badge_local_to_name:
                name = badge_local_to_name[local_bid]
                data["badge_id"] = badge_name_to_remote.get(name)
            elif local_bid:
                data["badge_id"] = None  # UUID local sem nome correspondente

            local_cid = data.get("category_id")
            if local_cid and local_cid in cat_local_to_name:
                name = cat_local_to_name[local_cid]
                data["category_id"] = cat_name_to_remote.get(name)
            elif local_cid:
                data["category_id"] = None

    async def _sync_logs_table(
        self, client: SupabaseClient, chunk_size: int = 50
    ) -> dict:
        """Sincroniza system_logs em batch: deserializa details (TEXT→dict) antes."""
        ok_ids: list[str] = []
        fail_ids: list[str] = []
        try:
            cursor = await self._db.execute(
                "SELECT * FROM system_logs WHERE synced=0 LIMIT 500"
            )
            rows = await cursor.fetchall()
            if not rows:
                return {"synced": 0, "errors": 0}

            all_data: list[tuple[str, dict]] = []
            for row in rows:
                row_id = str(row["id"])
                data = {k: row[k] for k in row.keys() if k != "synced"}
                data["details"] = json.loads(data.get("details") or "{}")
                all_data.append((row_id, data))

            for i in range(0, len(all_data), chunk_size):
                chunk = all_data[i : i + chunk_size]
                chunk_rows = [data for _, data in chunk]
                chunk_ids = [row_id for row_id, _ in chunk]
                try:
                    res = (
                        await client._db.table("system_logs")
                        .upsert(chunk_rows, on_conflict="id")
                        .execute()
                    )
                    if res.data:
                        ok_ids.extend(chunk_ids)
                    else:
                        fail_ids.extend(chunk_ids)
                except Exception:
                    fail_ids.extend(chunk_ids)

            if ok_ids:
                placeholders = ",".join("?" * len(ok_ids))
                await self._db.execute(
                    f"UPDATE system_logs SET synced=1 WHERE id IN ({placeholders})",  # noqa: S608
                    ok_ids,
                )
                await self._db.commit()

        except Exception as exc:
            logger.error("sync_system_logs_error", error=str(exc))
            fail_ids.append("outer_error")
        return {"synced": len(ok_ids), "errors": len(fail_ids)}

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
        user_id: str | None = None,
    ) -> Optional[str]:
        """Retorna o ID do user pela tag. Cria se nao existir."""
        try:
            cursor = await self._db.execute(
                "SELECT id FROM users WHERE affiliate_tag = ?",
                (affiliate_tag,),
            )
            row = await cursor.fetchone()
            if row:
                return row[0]
            uid = user_id or str(uuid.uuid4())
            cookies_json = json.dumps(ml_cookies) if ml_cookies else None
            await self._db.execute(
                "INSERT INTO users (id, name, affiliate_tag, email, password_hash, ml_cookies) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (uid, name, affiliate_tag, email, password_hash, cookies_json),
            )
            await self._db.commit()
            return uid
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_or_create_user") from exc

    async def get_user_by_tag(self, affiliate_tag: str) -> Optional[dict]:
        """Retorna o user completo pela tag."""
        try:
            cursor = await self._db.execute(
                "SELECT id, name, affiliate_tag, ml_cookies, created_at FROM users WHERE affiliate_tag = ?",
                (affiliate_tag,),
            )
            row = await cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "name": row[1],
                "affiliate_tag": row[2],
                "ml_cookies": json.loads(row[3]) if row[3] else None,
                "created_at": row[4],
            }
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_user_by_tag") from exc

    # ------------------------------------------------------------------
    # affiliate_links
    # ------------------------------------------------------------------

    async def get_affiliate_link(
        self, product_id: str, user_id: str
    ) -> Optional[dict]:
        """Retorna o affiliate link para um produto+user, ou None."""
        try:
            cursor = await self._db.execute(
                "SELECT id, product_id, user_id, short_url, long_url, ml_link_id, created_at "
                "FROM affiliate_links WHERE product_id = ? AND user_id = ?",
                (product_id, user_id),
            )
            row = await cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "product_id": row[1],
                "user_id": row[2],
                "short_url": row[3],
                "long_url": row[4],
                "ml_link_id": row[5],
                "created_at": row[6],
            }
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_affiliate_link") from exc

    async def save_affiliate_link(
        self,
        product_id: str,
        user_id: str,
        short_url: str,
        long_url: str = "",
        ml_link_id: str = "",
    ) -> Optional[str]:
        """Salva um affiliate link (upsert por product_id+user_id)."""
        try:
            link_id = str(uuid.uuid4())
            await self._db.execute(
                """INSERT INTO affiliate_links
                    (id, product_id, user_id, short_url, long_url, ml_link_id)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT (product_id, user_id) DO UPDATE SET
                    short_url = excluded.short_url,
                    long_url = excluded.long_url,
                    ml_link_id = excluded.ml_link_id""",
                (link_id, product_id, user_id, short_url, long_url, ml_link_id),
            )
            await self._db.commit()
            return link_id
        except Exception as exc:
            raise SQLiteError(str(exc), operation="save_affiliate_link") from exc

    async def get_missing_affiliate_links(
        self, user_id: str, product_ids: list[str]
    ) -> list[str]:
        """Retorna product_ids que ainda nao tem affiliate link para este user."""
        if not product_ids:
            return []
        try:
            placeholders = ",".join("?" for _ in product_ids)
            cursor = await self._db.execute(
                f"SELECT product_id FROM affiliate_links WHERE user_id = ? AND product_id IN ({placeholders})",  # noqa: S608
                [user_id, *product_ids],
            )
            rows = await cursor.fetchall()
            existing = {row[0] for row in rows}
            return [pid for pid in product_ids if pid not in existing]
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="get_missing_affiliate_links"
            ) from exc

    async def save_affiliate_links_batch(self, links: list[dict]) -> list[str]:
        """Salva multiplos affiliate links."""
        if not links:
            return []
        ids: list[str] = []
        try:
            for link in links:
                link_id = str(uuid.uuid4())
                await self._db.execute(
                    """INSERT INTO affiliate_links
                        (id, product_id, user_id, short_url, long_url, ml_link_id)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT (product_id, user_id) DO UPDATE SET
                        short_url = excluded.short_url,
                        long_url = excluded.long_url,
                        ml_link_id = excluded.ml_link_id""",
                    (
                        link_id,
                        link["product_id"],
                        link["user_id"],
                        link["short_url"],
                        link.get("long_url", ""),
                        link.get("ml_link_id", ""),
                    ),
                )
                ids.append(link_id)
            await self._db.commit()
            return ids
        except Exception as exc:
            raise SQLiteError(
                str(exc), operation="save_affiliate_links_batch"
            ) from exc

    # ------------------------------------------------------------------
    # Image Worker
    # ------------------------------------------------------------------

    async def get_pending_images(self, batch_size: int = 5) -> list[dict]:
        """Retorna produtos que precisam de processamento de imagem."""
        try:
            cursor = await self._db.execute(
                """
                SELECT id, ml_id, title, thumbnail_url
                FROM products
                WHERE image_status = 'pending'
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (batch_size,),
            )
            rows = await cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "ml_id": row[1],
                    "title": row[2],
                    "thumbnail_url": row[3],
                }
                for row in rows
            ]
        except Exception as exc:
            raise SQLiteError(str(exc), operation="get_pending_images") from exc

    async def update_image_status(
        self,
        product_id: str,
        status: str,
        enhanced_url: str | None = None,
    ) -> bool:
        """Atualiza o status de processamento de imagem de um produto."""
        try:
            if enhanced_url:
                await self._db.execute(
                    "UPDATE products SET image_status = ?, enhanced_image_url = ? WHERE id = ?",
                    (status, enhanced_url, product_id),
                )
            else:
                await self._db.execute(
                    "UPDATE products SET image_status = ? WHERE id = ?",
                    (status, product_id),
                )
            await self._db.commit()
            return True
        except Exception as exc:
            raise SQLiteError(str(exc), operation="update_image_status") from exc

    async def get_enhanced_image_url(self, product_id: str) -> str | None:
        """Retorna a URL da imagem aprimorada, se existir."""
        try:
            cursor = await self._db.execute(
                "SELECT enhanced_image_url FROM products WHERE id = ? AND image_status = 'enhanced'",
                (product_id,),
            )
            row = await cursor.fetchone()
            return row[0] if row and row[0] else None
        except Exception:
            return None

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
