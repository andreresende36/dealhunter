#!/usr/bin/env python3
"""
DealHunter — Reset Databases Script
Limpa todas as tabelas mantendo o schema intacto.

Uso:
    python scripts/reset_databases.py --truncate
    python scripts/reset_databases.py --truncate --include-supabase
"""

import sys
import asyncio
import sqlite3
from pathlib import Path

# Adiciona src/ ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings
from src.database.sqlite_fallback import SQLiteFallback
from src.database.supabase_client import SupabaseClient
import structlog

logger = structlog.get_logger(__name__)

# Tabelas a truncar (ordem respeita FKs — dependentes primeiro)
TABLES_TO_TRUNCATE = [
    "sent_offers",      # FK: scored_offer_id
    "scored_offers",    # FK: product_id
    "price_history",    # FK: product_id
    "products",         # FK: badge_id, category_id
    "system_logs",      # sem FKs
    "badges",           # lookup (products já foi limpa)
    "categories",       # lookup (products já foi limpa)
]


def truncate_sqlite() -> None:
    """Trunca todas as tabelas do SQLite local."""
    db_path = settings.sqlite.db_path

    if not db_path.exists():
        logger.warning("sqlite_not_found", path=str(db_path))
        return

    logger.info("sqlite_truncating", path=str(db_path))

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
        # Desativa FK constraint temporariamente
        cursor.execute("PRAGMA foreign_keys = OFF")

        for table in TABLES_TO_TRUNCATE:
            cursor.execute(f"DELETE FROM {table}")
            count = cursor.rowcount
            logger.info("table_truncated", table=table, rows_deleted=count)

        conn.commit()
        logger.info("sqlite_truncate_success")

    except sqlite3.Error as e:
        logger.error("sqlite_truncate_failed", error=str(e))
        conn.rollback()
        raise
    finally:
        cursor.execute("PRAGMA foreign_keys = ON")
        conn.close()


async def truncate_supabase() -> None:
    """Trunca todas as tabelas do Supabase."""
    supabase = SupabaseClient()

    try:
        await supabase.connect()
        ok = await supabase.ping()

        if not ok:
            logger.warning("supabase_not_available")
            return

        logger.info("supabase_truncating")

        # No Supabase, usa DELETE sem WHERE clause
        # Acessa via _db que retorna o AsyncClient conectado
        for table in TABLES_TO_TRUNCATE:
            try:
                # Delete sem where = truncate (deleta todas as linhas)
                result = await supabase._db.table(table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
                logger.info("supabase_table_truncated", table=table)
            except Exception as e:
                logger.error("supabase_table_delete_failed", table=table, error=str(e))

        logger.info("supabase_truncate_success")

    except Exception as e:
        logger.error("supabase_connect_failed", error=str(e))
    finally:
        await supabase.close()


async def main(include_supabase: bool = False) -> None:
    """Executa o reset."""
    print("\n" + "=" * 70)
    print("DealHunter — Reset Databases (Opção A: Truncate)")
    print("=" * 70)
    print(f"\n📋 Tabelas a truncar: {', '.join(TABLES_TO_TRUNCATE)}")
    print(f"🔒 Schema será PRESERVADO\n")

    if include_supabase:
        print("⚠️  Supabase: SIM (vai truncar remoto também)")
    else:
        print("⚠️  Supabase: NÃO (apenas SQLite local)")

    print("\n" + "-" * 70)
    response = input("Tem certeza? (digite 'SIM' para confirmar): ").strip()

    if response.upper() != "SIM":
        print("\n❌ Operação cancelada.")
        return

    print("\n🧹 Truncando SQLite local...")
    try:
        truncate_sqlite()
        print("✅ SQLite truncado com sucesso!\n")
    except Exception as e:
        print(f"❌ Erro ao truncar SQLite: {e}\n")
        sys.exit(1)

    if include_supabase:
        print("🧹 Truncando Supabase...")
        try:
            await truncate_supabase()
            print("✅ Supabase truncado com sucesso!\n")
        except Exception as e:
            print(f"⚠️  Erro ao truncar Supabase (SQLite já foi truncado): {e}\n")

    print("=" * 70)
    print("✨ Reset concluído! Bancos estão zerados e prontos para novos dados.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Reset DealHunter databases (truncate mode)"
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Trunca as tabelas (mantém schema)"
    )
    parser.add_argument(
        "--include-supabase",
        action="store_true",
        help="Também trunca Supabase (se configurado)"
    )

    args = parser.parse_args()

    if not args.truncate:
        print("❌ Use: python scripts/reset_databases.py --truncate")
        sys.exit(1)

    asyncio.run(main(include_supabase=args.include_supabase))
