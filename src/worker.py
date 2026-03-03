"""
DealHunter — Deep Scrape Worker
Processo separado que enriquece produtos visitando páginas individuais
do Mercado Livre, pontua com ScoreEngine e publica ofertas aprovadas.

Consome a fila de produtos com enrichment_status='pending' no banco.

Executar:
    python -m src.worker
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import signal

import structlog

from src.config import settings
from src.scraper.base_scraper import ScrapedProduct
from src.scraper.product_detail_scraper import ProductDetailScraper
from src.analyzer.score_engine import ScoreEngine
from src.distributor.message_formatter import MessageFormatter
from src.distributor.affiliate_links import AffiliateLinkBuilder
from src.distributor.shlink_client import ShlinkClient
from src.distributor.telegram_bot import TelegramBot
from src.distributor.whatsapp_notifier import WhatsAppNotifier
from src.database.storage_manager import StorageManager
from src.monitoring.alert_bot import AlertBot


# ---------------------------------------------------------------------------
# Redação de dados sensíveis (mesmo padrão do main.py)
# ---------------------------------------------------------------------------
_SENSITIVE_PATTERNS = [
    (re.compile(r"(sk-ant-api\w{2}-)[\w-]+"), r"\1****"),
    (re.compile(r"(eyJ[\w-]+\.eyJ[\w-]+)\.[\w-]+"), r"\1.****"),
    (re.compile(r"(Bearer\s+)[\w.-]+"), r"\1****"),
    (re.compile(r"(apikey[=:\s]+)[\w-]+", re.I), r"\1****"),
    (re.compile(r"(\d{6,}:[\w-]{30,})"), "****:****"),
]


def _redact_sensitive_data(logger, method_name, event_dict):
    for key, value in event_dict.items():
        if not isinstance(value, str):
            continue
        for pattern, replacement in _SENSITIVE_PATTERNS:
            value = pattern.sub(replacement, value)
        event_dict[key] = value
    return event_dict


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


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class DeepScrapeWorker:
    """
    Worker de longa duração que:
    1. Poll DB por produtos com enrichment_status='pending'
    2. Visita páginas individuais para coletar dados detalhados
    3. Atualiza DB com dados enriquecidos
    4. Roda ScoreEngine com dados completos
    5. Publica ofertas aprovadas em Telegram + WhatsApp
    """

    def __init__(self) -> None:
        self._shutdown = asyncio.Event()
        self._stats = {
            "enriched": 0,
            "scored": 0,
            "published": 0,
            "errors": 0,
        }
        self._cfg = settings.deep_scrape
        self._consecutive_captchas = 0

        # Componentes
        self._score_engine = ScoreEngine()
        self._formatter = MessageFormatter()
        self._affiliate_builder = AffiliateLinkBuilder()
        self._shlink = ShlinkClient()
        self._telegram = TelegramBot()
        self._whatsapp = WhatsAppNotifier()
        self._alert_bot = AlertBot()

    async def run(self) -> dict:
        """Loop principal — poll + process até shutdown."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._shutdown.set)

        logger.info("worker_start", config={
            "batch_size": self._cfg.batch_size,
            "max_concurrent": self._cfg.max_concurrent,
            "poll_interval": self._cfg.poll_interval,
        })

        async with StorageManager() as storage:
            async with ProductDetailScraper() as scraper:
                self._scraper = scraper

                # Crash recovery: resetar claims antigos
                await storage.reset_stale_claims(stale_minutes=30)

                while not self._shutdown.is_set():
                    # Buscar produtos pendentes
                    batch = await storage.claim_for_enrichment(
                        batch_size=self._cfg.batch_size
                    )

                    # Buscar também retries (produtos com falha)
                    if len(batch) < self._cfg.batch_size:
                        retries = await storage.get_products_needing_retry(
                            max_attempts=self._cfg.max_attempts,
                            batch_size=self._cfg.batch_size - len(batch),
                        )
                        batch.extend(retries)

                    if not batch:
                        # Fila vazia — aguardar antes de novo poll
                        try:
                            await asyncio.wait_for(
                                self._shutdown.wait(),
                                timeout=self._cfg.poll_interval,
                            )
                        except asyncio.TimeoutError:
                            pass
                        continue

                    # Shuffle para evitar padrão previsível de acesso
                    random.shuffle(batch)

                    await self._process_batch(storage, batch)

        logger.info("worker_shutdown", stats=self._stats)
        return self._stats

    async def _process_batch(
        self, storage: StorageManager, batch: list[dict]
    ) -> None:
        """Processa um lote com concorrência limitada."""
        sem = asyncio.Semaphore(self._cfg.max_concurrent)

        async def _process_one(product_row: dict) -> None:
            async with sem:
                await self._enrich_score_publish(storage, product_row)

        tasks = [_process_one(row) for row in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(
                    "batch_item_exception",
                    ml_id=batch[i].get("ml_id", "?"),
                    error=str(result),
                )

    async def _enrich_score_publish(
        self, storage: StorageManager, product_row: dict
    ) -> None:
        """Pipeline completo para um produto: deep scrape → score → publish."""
        product_id = product_row["id"]
        ml_id = product_row["ml_id"]
        url = product_row["product_url"]

        try:
            # 1. DEEP SCRAPE — visitar página individual
            enriched = await self._scraper.enrich_product(url, ml_id)
            await storage.increment_enrichment_attempts(product_id)

            if not enriched.enrichment_success:
                if enriched.error_message == "product_unavailable":
                    await storage.set_enrichment_status(
                        product_id, "skipped"
                    )
                    logger.info(
                        "product_skipped", ml_id=ml_id, reason="unavailable"
                    )
                else:
                    await storage.set_enrichment_status(
                        product_id,
                        "failed",
                        error=enriched.error_message,
                    )
                    logger.warning(
                        "enrichment_failed",
                        ml_id=ml_id,
                        error=enriched.error_message,
                    )
                self._stats["errors"] += 1

                # Detectar CAPTCHAs consecutivos
                if "captcha" in enriched.error_message.lower():
                    self._consecutive_captchas += 1
                    if (
                        self._consecutive_captchas
                        >= self._cfg.max_consecutive_captchas
                    ):
                        logger.warning(
                            "captcha_pause",
                            consecutive=self._consecutive_captchas,
                            pause_seconds=self._cfg.captcha_pause_seconds,
                        )
                        await asyncio.sleep(self._cfg.captcha_pause_seconds)
                        self._consecutive_captchas = 0
                return

            # Reset CAPTCHA counter on success
            self._consecutive_captchas = 0

            # 2. ATUALIZAR DB com dados enriquecidos
            await storage.update_enriched_data(product_id, {
                "seller_name": enriched.seller_name,
                "seller_reputation": enriched.seller_reputation,
                "sold_quantity": enriched.sold_quantity,
                "rating_stars": enriched.rating,
                "rating_count": enriched.review_count,
            })
            await storage.set_enrichment_status(product_id, "enriched")
            self._stats["enriched"] += 1

            # 3. SCORE — reconstruir ScrapedProduct com dados completos
            product_data = await storage.get_product_for_scoring(product_id)
            if not product_data:
                logger.error(
                    "product_not_found_for_scoring", product_id=product_id
                )
                return

            scraped = self._row_to_scraped_product(product_data)
            scored = self._score_engine.evaluate(scraped)

            status = "approved" if scored.passed else "rejected"
            scored_offer_id = await storage.save_scored_offer(
                product_id=product_id,
                rule_score=int(scored.score),
                final_score=int(scored.score),
                status=status,
            )
            await storage.set_enrichment_status(product_id, "scored")
            self._stats["scored"] += 1

            if not scored.passed:
                logger.info(
                    "offer_rejected",
                    ml_id=ml_id,
                    score=scored.score,
                    reason=scored.reject_reason,
                )
                return

            # 4. PUBLICAR — mesma lógica que estava no main.py
            await self._publish_offer(
                storage, scraped, scored_offer_id, product_id
            )

        except Exception as exc:
            logger.error(
                "enrich_score_publish_error",
                ml_id=ml_id,
                error=str(exc),
            )
            await storage.set_enrichment_status(
                product_id, "failed", error=str(exc)[:200]
            )
            self._stats["errors"] += 1

    async def _publish_offer(
        self,
        storage: StorageManager,
        product: ScrapedProduct,
        scored_offer_id: str,
        product_id: str,
    ) -> None:
        """Publica uma oferta aprovada nos canais configurados."""
        # URL de afiliado + encurtamento
        affiliate_url = self._affiliate_builder.build(product.url)
        try:
            short_url = await self._shlink.shorten(
                affiliate_url,
                tags=[
                    "dealhunter",
                    (
                        product.category.lower()[:20]
                        if product.category
                        else "moda"
                    ),
                ],
            )
        except Exception:
            short_url = affiliate_url

        # Formatar mensagem
        message = self._formatter.format(product, short_link=short_url)

        # Publicar nos canais
        channels_published = []

        try:
            tg_results = await self._telegram.publish(message)
            if any(r.get("success") for r in tg_results):
                channels_published.append("telegram")
        except Exception as tg_exc:
            logger.warning("telegram_publish_failed", error=str(tg_exc))

        try:
            wa_results = await self._whatsapp.publish(message)
            if any(r.get("success") for r in wa_results):
                channels_published.append("whatsapp")
        except Exception as wa_exc:
            logger.warning("whatsapp_publish_failed", error=str(wa_exc))

        # Registrar envio
        for channel in channels_published:
            await storage.mark_as_sent(
                scored_offer_id=scored_offer_id,
                channel=channel,
                shlink_short_url=short_url,
            )

        if channels_published:
            await storage.set_enrichment_status(product_id, "published")
            self._stats["published"] += 1
            logger.info(
                "offer_published",
                ml_id=product.ml_id,
                channels=channels_published,
            )

    @staticmethod
    def _row_to_scraped_product(row: dict) -> ScrapedProduct:
        """Converte uma row do banco de volta para ScrapedProduct."""
        return ScrapedProduct(
            ml_id=row.get("ml_id", ""),
            url=row.get("product_url", ""),
            title=row.get("title", ""),
            price=float(row.get("current_price", 0)),
            original_price=(
                float(row["original_price"])
                if row.get("original_price")
                else None
            ),
            discount_pct=float(row.get("discount_percent", 0)),
            seller=row.get("seller_name", ""),
            rating=float(row.get("rating_stars", 0)),
            review_count=int(row.get("rating_count", 0)),
            category=row.get("category", ""),
            image_url=row.get("thumbnail_url", ""),
            free_shipping=bool(row.get("free_shipping", False)),
            is_official_store=bool(row.get("is_official_store", False)),
            seller_reputation=row.get("seller_reputation", ""),
            sold_quantity=int(row.get("sold_quantity", 0)),
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    """Entry point do worker."""
    worker = DeepScrapeWorker()
    stats = await worker.run()
    logger.info("worker_complete", **stats)


if __name__ == "__main__":
    asyncio.run(main())
