"""
DealHunter — Entry Point Principal (Scraper Pipeline)
Coleta ofertas do Mercado Livre, pontua e salva no banco.

Fluxo: scraping cards → dedup → fake discount filter → salvar.
"""

import asyncio

import structlog

from src.logging_config import setup_logging
from src.config import settings
from src.scraper.ml_scraper import MLScraper
from src.analyzer.fake_discount_detector import FakeDiscountDetector
from src.database.storage_manager import StorageManager
from src.monitoring.alert_bot import AlertBot
from src.monitoring.health_check import HealthCheck

setup_logging()
logger = structlog.get_logger(__name__)


async def run_pipeline() -> dict:
    """
    Executa o pipeline de scraping do DealHunter.

    Coleta ofertas dos cards de listagem, filtra duplicatas e descontos falsos,
    e salva no banco.

    Retorna dict com estatísticas da execução.
    """
    stats = {
        "scraped": 0,
        "new": 0,
        "saved": 0,
        "errors": 0,
    }

    logger.info("pipeline_start", env=settings.env)

    fake_detector = FakeDiscountDetector()
    alert_bot = AlertBot()

    async with StorageManager() as storage:
        # 1. SCRAPING — Coleta ofertas de todas as fontes (scraper unificado)
        scraper = MLScraper()
        try:
            all_products = await scraper.scrape()
            logger.info("scraper_done", count=len(all_products))
        except Exception as exc:
            all_products = []
            logger.error("scraper_failed", error=str(exc))
            stats["errors"] += 1
            try:
                await alert_bot.send_error(exc, context="MLScraper")
            except Exception:
                pass

        stats["scraped"] = len(all_products)

        if not all_products:
            logger.warning("no_products_scraped")
            return stats

        # 2. DEDUPLICAÇÃO — Remove produtos já publicados recentemente
        new_products = []
        for product in all_products:
            if not await storage.was_recently_sent(product.ml_id, hours=24):
                new_products.append(product)

        logger.info("dedup_done", total=len(all_products), new=len(new_products))

        # 3. FAKE DISCOUNT FILTER — Usa dados do card (pré-enrichment)
        fake_results = fake_detector.check_batch(new_products)
        genuine_products = [p for p, r in fake_results if not r.is_fake]
        stats["new"] = len(genuine_products)

        logger.info(
            "fake_filter_done",
            genuine=len(genuine_products),
            fake=len(new_products) - len(genuine_products),
        )

        # 4. SALVAR NO BANCO
        for product in genuine_products:
            try:
                product_id = await storage.upsert_product(product)
                await storage.add_price_history(
                    product_id, product.price, product.original_price
                )
                stats["saved"] += 1
            except Exception as exc:
                logger.error(
                    "save_failed",
                    ml_id=product.ml_id,
                    error=str(exc),
                )
                stats["errors"] += 1

    logger.info("pipeline_done", **stats)
    return stats


async def main():
    """Entry point com health check inicial."""
    # Health check antes de começar
    checker = HealthCheck()
    report = await checker.run()

    if not report.overall_healthy:
        logger.warning("unhealthy_services", summary=report.summary())

    # Executa pipeline
    stats = await run_pipeline()
    logger.info("execution_complete", **stats)


if __name__ == "__main__":
    asyncio.run(main())
