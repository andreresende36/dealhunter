"""
DealHunter — Entry Point Principal (Scraper Pipeline)
Coleta ofertas do Mercado Livre e salva no banco para enriquecimento.

Fluxo: scraping cards → dedup → fake discount filter → salvar com status pending.
O worker (src/worker.py) consome a fila e faz deep scrape → score → publicação.
"""

import asyncio
import logging
import re

import structlog

from src.config import settings
from src.scraper.ml_scraper import MLScraper
from src.analyzer.fake_discount_detector import FakeDiscountDetector
from src.database.storage_manager import StorageManager
from src.monitoring.alert_bot import AlertBot
from src.monitoring.health_check import HealthCheck


# ---------------------------------------------------------------------------
# Processador de redação de dados sensíveis nos logs
# ---------------------------------------------------------------------------
_SENSITIVE_PATTERNS = [
    (re.compile(r"(sk-ant-api\w{2}-)[\w-]+"), r"\1****"),  # Anthropic API keys
    (re.compile(r"(eyJ[\w-]+\.eyJ[\w-]+)\.[\w-]+"), r"\1.****"),  # JWTs (Supabase keys)
    (re.compile(r"(Bearer\s+)[\w.-]+"), r"\1****"),  # Bearer tokens
    (re.compile(r"(apikey[=:\s]+)[\w-]+", re.I), r"\1****"),  # API keys genéricos
    (re.compile(r"(\d{6,}:[\w-]{30,})"), "****:****"),  # Telegram bot tokens
]


def _redact_sensitive_data(logger, method_name, event_dict):
    """Processador structlog que mascara dados sensíveis nos valores dos logs."""
    for key, value in event_dict.items():
        if not isinstance(value, str):
            continue
        for pattern, replacement in _SENSITIVE_PATTERNS:
            value = pattern.sub(replacement, value)
        event_dict[key] = value
    return event_dict


# Configura logging estruturado
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        _redact_sensitive_data,
        (
            structlog.dev.ConsoleRenderer()
            if not settings.is_production
            else structlog.processors.JSONRenderer()
        ),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        logging.getLevelName(settings.log_level)
    ),
)

logger = structlog.get_logger(__name__)


async def run_pipeline() -> dict:
    """
    Executa o pipeline de scraping do DealHunter.

    Coleta ofertas dos cards de listagem, filtra duplicatas e descontos falsos,
    e salva no banco com enrichment_status='pending' para o worker processar.

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

        logger.info(
            "dedup_done", total=len(all_products), new=len(new_products)
        )

        # 3. FAKE DISCOUNT FILTER — Usa dados do card (pré-enrichment)
        fake_results = fake_detector.check_batch(new_products)
        genuine_products = [p for p, r in fake_results if not r.is_fake]
        stats["new"] = len(genuine_products)

        logger.info(
            "fake_filter_done",
            genuine=len(genuine_products),
            fake=len(new_products) - len(genuine_products),
        )

        # 4. SALVAR NO BANCO — Com enrichment_status='pending'
        #    O worker (src/worker.py) consumirá esta fila para:
        #    deep scrape → score → publicação
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
