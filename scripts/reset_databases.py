#!/usr/bin/env python3
"""
Crivo — Reset Databases Script
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
    "affiliate_links",  # FK: product_id, user_id
    "products",         # FK: badge_id, category_id, marketplace_id
    "users",            # sem FKs externas
    "system_logs",      # sem FKs
    "badges",           # lookup (products já foi limpa)
    "categories",       # lookup (products já foi limpa)
    "marketplaces",     # lookup (products já foi limpa)
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


async def clear_supabase_storage(bucket: str = "images", folder: str = "products") -> int:
    """
    Apaga todos os arquivos de uma pasta no Supabase Storage.
    Itera recursivamente pelas subpastas (products/{uuid}/enhanced.jpg).
    Retorna o número de arquivos deletados.
    """
    supabase = SupabaseClient()
    deleted = 0

    try:
        await supabase.connect()
        client = supabase._db

        # Passo 1: lista subpastas em products/ (cada subpasta é um product_id)
        subdirs = await client.storage.from_(bucket).list(folder)
        if not subdirs:
            logger.info("storage_folder_empty", bucket=bucket, folder=folder)
            return 0

        # Passo 2: para cada subpasta, lista os arquivos dentro dela
        all_paths: list[str] = []
        for subdir in subdirs:
            subdir_name = subdir.get("name", "")
            if not subdir_name:
                continue
            subdir_path = f"{folder}/{subdir_name}"
            files = await client.storage.from_(bucket).list(subdir_path)
            if files:
                for f in files:
                    fname = f.get("name", "")
                    if fname:
                        all_paths.append(f"{subdir_path}/{fname}")

        if not all_paths:
            logger.info("storage_folder_empty", bucket=bucket, folder=folder)
            return 0

        # Passo 3: deleta em batches de 100
        batch_size = 100
        for i in range(0, len(all_paths), batch_size):
            batch = all_paths[i:i + batch_size]
            await client.storage.from_(bucket).remove(batch)
            deleted += len(batch)
            logger.info("storage_batch_deleted", count=len(batch))

        logger.info("storage_cleared", bucket=bucket, folder=folder, total=deleted)
        return deleted

    except Exception as e:
        logger.error("storage_clear_failed", bucket=bucket, folder=folder, error=str(e))
        raise
    finally:
        await supabase.close()


async def main(include_supabase: bool = False, clear_storage: bool = False) -> None:
    """Executa o reset."""
    print("\n" + "=" * 70)
    print("Crivo — Reset Databases (Opção A: Truncate)")
    print("=" * 70)
    print(f"\n📋 Tabelas a truncar: {', '.join(TABLES_TO_TRUNCATE)}")
    print(f"🔒 Schema será PRESERVADO\n")

    if include_supabase:
        print("⚠️  Supabase: SIM (vai truncar remoto também)")
    else:
        print("⚠️  Supabase: NÃO (apenas SQLite local)")

    if clear_storage:
        print("⚠️  Storage: SIM (vai apagar images/products do Supabase Storage)")
    else:
        print("⚠️  Storage: NÃO (imagens serão mantidas)")

    print("\n" + "-" * 70)
    response = (await asyncio.to_thread(input, "Tem certeza? (digite 'SIM' para confirmar): ")).strip()

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

    if clear_storage:
        print("🗑️  Apagando imagens do Supabase Storage (images/products)...")
        try:
            count = await clear_supabase_storage(bucket="images", folder="products")
            print(f"✅ {count} arquivo(s) deletado(s) do Storage!\n")
        except Exception as e:
            print(f"⚠️  Erro ao limpar Storage: {e}\n")

    print("=" * 70)
    print("✨ Reset concluído! Bancos estão zerados e prontos para novos dados.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Reset Crivo databases (truncate mode)"
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
    parser.add_argument(
        "--clear-storage",
        action="store_true",
        help="Apaga todos os arquivos em images/products no Supabase Storage"
    )

    args = parser.parse_args()

    if not args.truncate:
        print("❌ Use: python scripts/reset_databases.py --truncate")
        sys.exit(1)

    asyncio.run(main(include_supabase=args.include_supabase, clear_storage=args.clear_storage))
