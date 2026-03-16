"""
DealHunter — Image Worker
Worker assíncrono que busca, avalia e salva imagens aprimoradas para produtos.

Pipeline por produto:
  1. Serper.dev → busca 8-10 candidatas no Google Images
  2. Filtros locais → resolução, fundo branco, marca d'água
  3. Claude Haiku Vision → valida se é o mesmo produto + ranking
  4. Supabase Storage → salva a melhor imagem

Uso:
    python -m src.image_worker
"""

from __future__ import annotations

import asyncio
import signal
import sys

import structlog

from src.config import settings
from src.database.storage_manager import StorageManager
from src.image.serper_client import search_product_images
from src.image.local_filters import filter_candidates
from src.image.vision_evaluator import evaluate_candidates
from src.image.image_storage import download_image_bytes, upload_to_supabase

logger = structlog.get_logger(__name__)

# Flag para shutdown graceful
_shutdown = False


def _handle_signal(sig: int, frame: object) -> None:
    global _shutdown
    logger.info("image_worker_shutdown_requested", signal=sig)
    _shutdown = True


async def process_product(
    storage: StorageManager,
    product: dict,
) -> None:
    """
    Processa um único produto: busca → filtra → avalia → salva.

    Args:
        storage: StorageManager conectado.
        product: Dict com id, ml_id, title, thumbnail_url.
    """
    product_id = product["id"]
    title = product["title"]
    thumbnail_url = product.get("thumbnail_url", "")

    logger.info(
        "image_processing_start",
        product_id=product_id,
        title=title[:50],
    )

    try:
        # Marca como in_progress
        await storage.update_image_status(product_id, "in_progress")

        # 1. Buscar candidatas via Serper.dev
        candidates = await search_product_images(title)
        if not candidates:
            logger.info("image_no_candidates", product_id=product_id)
            await storage.update_image_status(product_id, "no_match")
            return

        # 2. Filtros locais (resolução, fundo branco, watermark)
        survivors = await filter_candidates(candidates)
        if not survivors:
            logger.info("image_all_filtered", product_id=product_id)
            await storage.update_image_status(product_id, "no_match")
            return

        # 3. Avaliação com Claude Haiku Vision
        best = await evaluate_candidates(thumbnail_url, title, survivors)
        if best is None:
            logger.info("image_ai_rejected_all", product_id=product_id)
            await storage.update_image_status(product_id, "no_match")
            return

        # 4. Baixar a imagem vencedora
        best_url = best["url"]
        image_bytes = await download_image_bytes(best_url)
        if image_bytes is None:
            logger.warning("image_download_winner_failed", product_id=product_id)
            await storage.update_image_status(product_id, "failed")
            return

        # Determinar extensão pelo URL
        ext = "jpg"
        lower_url = best_url.lower()
        if ".png" in lower_url:
            ext = "png"
        elif ".webp" in lower_url:
            ext = "webp"

        # 5. Upload para Supabase Storage
        public_url = await upload_to_supabase(product_id, image_bytes, ext)
        if public_url is None:
            await storage.update_image_status(product_id, "failed")
            return

        # 6. Atualizar banco com URL e status
        await storage.update_image_status(
            product_id, "enhanced", enhanced_url=public_url
        )

        logger.info(
            "image_processing_done",
            product_id=product_id,
            enhanced_url=public_url[:80],
            source=best.get("source", ""),
        )

    except Exception as exc:
        logger.error(
            "image_processing_error",
            product_id=product_id,
            error=str(exc),
        )
        try:
            await storage.update_image_status(product_id, "failed")
        except Exception:
            pass


async def run_worker() -> None:
    """Loop principal do image worker."""
    cfg = settings.image_worker

    if not cfg.enabled:
        logger.info("image_worker_disabled")
        return

    if not settings.serper.api_key:
        logger.error("image_worker_aborted", reason="SERPER_API_KEY not set")
        return

    logger.info(
        "image_worker_started",
        poll_interval=cfg.poll_interval,
        batch_size=cfg.batch_size,
    )

    async with StorageManager() as storage:
        await storage.log_event("image_worker_start")

        while not _shutdown:
            try:
                # Buscar produtos pendentes de processamento de imagem
                pending = await storage.get_pending_images(cfg.batch_size)

                if pending:
                    logger.info("image_worker_batch", count=len(pending))
                    for product in pending:
                        if _shutdown:
                            break
                        await process_product(storage, product)
                else:
                    logger.debug("image_worker_idle")

            except Exception as exc:
                logger.error("image_worker_loop_error", error=str(exc))

            # Esperar antes do próximo poll
            if not _shutdown:
                await asyncio.sleep(cfg.poll_interval)

        await storage.log_event("image_worker_stop")
        logger.info("image_worker_stopped")


def main() -> None:
    """Entry point para execução direta."""
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("image_worker_interrupted")
        sys.exit(0)


if __name__ == "__main__":
    main()
