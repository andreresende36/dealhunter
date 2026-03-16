"""
DealHunter — Entry Point Principal (Scraper Pipeline)
Coleta ofertas do Mercado Livre, pontua, salva e publica nos grupos.

Fluxo: scraping cards → dedup → fake filter → score → save → publish.
"""

import asyncio
import time

import structlog

from src.logging_config import setup_logging
from src.config import settings
from src.scraper.ml_scraper import MLScraper
from src.analyzer.fake_discount_detector import FakeDiscountDetector
from src.analyzer.score_engine import ScoreEngine
from src.analyzer.card_debugger import generate_report
from src.database.storage_manager import StorageManager
from src.distributor.affiliate_links import AffiliateLinkBuilder
from src.distributor.message_formatter import MessageFormatter
from src.distributor.telegram_bot import TelegramBot
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
        "scored": 0,
        "approved": 0,
        "rejected": 0,
        "saved": 0,
        "affiliate_links": 0,
        "published": 0,
        "publish_errors": 0,
        "errors": 0,
    }
    timings: dict[str, float] = {}

    logger.info("pipeline_start", env=settings.env)

    fake_detector = FakeDiscountDetector()
    alert_bot = AlertBot()

    async with StorageManager() as storage:
        # 1. SCRAPING — Coleta ofertas de todas as fontes (scraper unificado)
        scraper = MLScraper()
        t0 = time.time()
        try:
            all_products = await scraper.scrape()
        except Exception as exc:
            all_products = []
            logger.error("scraper_failed", error=str(exc))
            stats["errors"] += 1
            try:
                await alert_bot.send_error(exc, context="MLScraper")
            except Exception:
                pass
        timings["scraping"] = round(time.time() - t0, 2)

        stats["scraped"] = len(all_products)

        if not all_products:
            logger.warning("no_products_scraped")
            return stats

        # 2. DEDUPLICAÇÃO — Remove produtos já publicados recentemente
        t0 = time.time()
        if await storage.has_recent_sends(hours=24):
            new_products = []
            for product in all_products:
                if not await storage.was_recently_sent(product.ml_id, hours=24):
                    new_products.append(product)
        else:
            new_products = all_products
        timings["dedup"] = round(time.time() - t0, 2)

        logger.info("dedup_done", total=len(all_products), new=len(new_products))

        # 3. FAKE DISCOUNT FILTER — Usa dados do card (pré-enrichment)
        t0 = time.time()
        fake_results = fake_detector.check_batch(new_products)
        genuine_products = [p for p, r in fake_results if not r.is_fake]
        stats["new"] = len(genuine_products)
        timings["fake_filter"] = round(time.time() - t0, 2)

        logger.info(
            "fake_filter_done",
            genuine=len(genuine_products),
            fake=len(new_products) - len(genuine_products),
        )

        # 4. SCORE — Avalia e filtra por pontuação mínima
        t0 = time.time()
        score_engine = ScoreEngine()
        all_scored = [score_engine.evaluate(p) for p in genuine_products]
        scored_products = sorted(
            [s for s in all_scored if s.passed],
            key=lambda s: s.score,
            reverse=True,
        )
        rejected_products = [s for s in all_scored if not s.passed]

        # Resumo consolidado da avaliação (detalhes por item no debug report)
        reject_reasons: dict[str, int] = {}
        for s in rejected_products:
            reason = s.reject_reason or "unknown"
            if reason.startswith("Score"):
                reason = "score_baixo"
            elif reason.startswith("Desconto"):
                reason = "desconto_baixo"
            elif reason.startswith("Avaliacao"):
                reason = "avaliacao_baixa"
            elif reason.startswith("Apenas"):
                reason = "poucas_avaliacoes"
            elif reason.startswith("Preco"):
                reason = "preco_invalido"
            elif reason.startswith("Titulo"):
                reason = "titulo_curto"
            reject_reasons[reason] = reject_reasons.get(reason, 0) + 1
        logger.info(
            "score_summary",
            total=len(genuine_products),
            approved=len(scored_products),
            rejected=len(rejected_products),
            reject_reasons=reject_reasons or None,
        )

        stats["scored"] = len(genuine_products)
        stats["approved"] = len(scored_products)
        stats["rejected"] = len(rejected_products)
        timings["scoring"] = round(time.time() - t0, 2)

        # Debug: gera relatório HTML com os cards rejeitados
        if settings.scraper.debug_screenshots and rejected_products:
            try:
                report_path = generate_report(
                    rejected=rejected_products,
                    screenshots=scraper.card_screenshots,
                    run_id=scraper.run_id,
                    min_score=settings.score.min_score,
                )
                if report_path:
                    logger.info("debug_report_ready", path=str(report_path))
            except Exception as exc_dbg:
                logger.warning("debug_report_failed", error=str(exc_dbg))

        # 5. SALVAR NO BANCO — Só produtos aprovados pelo score
        approved_products = [s.product for s in scored_products]

        t0 = time.time()
        if approved_products:
            try:
                ids = await storage.upsert_products_batch(approved_products)
                entries = [
                    {
                        "product_id": ids[p.ml_id],
                        "price": p.price,
                        "original_price": p.original_price,
                    }
                    for p in approved_products
                    if p.ml_id in ids
                ]
                await storage.add_price_history_batch(entries)
                stats["saved"] = len(ids)

                # Salva scored_offers em batch (1 transação)
                scored_entries = [
                    {
                        "product_id": ids[s.product.ml_id],
                        "rule_score": int(s.score),
                        "final_score": int(s.score),
                        "status": "approved",
                    }
                    for s in scored_products
                    if s.product.ml_id in ids
                ]
                scored_offer_ids: list[str] = []
                if scored_entries:
                    try:
                        scored_offer_ids = await storage.save_scored_offers_batch(
                            scored_entries
                        )
                    except Exception as exc_so:
                        logger.warning(
                            "scored_offers_batch_save_failed",
                            error=str(exc_so),
                        )

                # 6. AFFILIATE LINKS — Gera links oficiais via API do ML
                t1 = time.time()
                aff_results: dict[str, str] = {}
                try:
                    ml_cfg = settings.mercado_livre
                    user_id = await storage.get_or_create_user(
                        name=ml_cfg.user_name or ml_cfg.affiliate_tag,
                        affiliate_tag=ml_cfg.affiliate_tag,
                        email=ml_cfg.user_email or None,
                        password=ml_cfg.user_password or None,
                    )
                    if user_id:
                        aff_builder = AffiliateLinkBuilder(storage, user_id=user_id)
                        products_map = {
                            ids[p.ml_id]: p.url
                            for p in approved_products
                            if p.ml_id in ids
                        }
                        aff_results = await aff_builder.get_or_create_batch(products_map)
                        stats["affiliate_links"] = len(aff_results)
                except Exception as exc_aff:
                    logger.warning("affiliate_links_failed", error=str(exc_aff))
                timings["affiliate_links"] = round(time.time() - t1, 2)

                # 7. PUBLICAR NO TELEGRAM — Formata e envia ofertas aprovadas
                t2 = time.time()
                if settings.telegram.bot_token and settings.telegram.group_ids:
                    try:
                        telegram_bot = TelegramBot()
                        formatter = MessageFormatter()

                        failed_ids: list[str] = []

                        for idx, s in enumerate(scored_products):
                            product = s.product
                            product_id = ids.get(product.ml_id)
                            if not product_id:
                                continue

                            # Link de afiliado (meli.la) ou URL original do produto
                            aff_url = aff_results.get(product_id, "") if aff_results else ""
                            short_url = aff_url or product.url

                            # Formatar mensagem
                            msg = formatter.format(product, short_link=short_url)

                            # Enviar para os grupos
                            results = await telegram_bot.publish(msg)
                            sent_ok = any(r["success"] for r in results)

                            if sent_ok:
                                stats["published"] += 1
                                # Registrar no sent_offers
                                scored_offer_id = (
                                    scored_offer_ids[idx]
                                    if idx < len(scored_offer_ids)
                                    else None
                                )
                                if scored_offer_id:
                                    try:
                                        await storage.mark_as_sent(
                                            scored_offer_id,
                                            channel="telegram",
                                        )
                                    except Exception as exc_mark:
                                        logger.debug(
                                            "mark_as_sent_failed",
                                            ml_id=product.ml_id,
                                            error=str(exc_mark),
                                        )
                            else:
                                stats["publish_errors"] += 1
                                failed_ids.append(product.ml_id)

                        logger.info(
                            "telegram_publish_done",
                            published=stats["published"],
                            errors=stats["publish_errors"],
                            failed_ids=failed_ids or None,
                        )
                    except Exception as exc_tg:
                        logger.error("telegram_publish_error", error=str(exc_tg))
                else:
                    logger.info("telegram_skip_not_configured")
                timings["publishing"] = round(time.time() - t2, 2)

            except Exception as exc:
                logger.error("batch_save_failed", error=str(exc))
                # Fallback: salva individualmente
                for s in scored_products:
                    product = s.product
                    try:
                        product_id = await storage.upsert_product(product)
                        await storage.add_price_history(
                            product_id, product.price, product.original_price
                        )
                        await storage.save_scored_offer(
                            product_id=product_id,
                            rule_score=int(s.score),
                            final_score=int(s.score),
                            status="approved",
                        )
                        stats["saved"] += 1
                    except Exception as exc2:
                        logger.error(
                            "save_failed",
                            ml_id=product.ml_id,
                            error=str(exc2),
                        )
                        stats["errors"] += 1
        timings["saving"] = round(time.time() - t0, 2)

        # ── Build enriched stats for final banner ──
        score_stats = {}
        if scored_products:
            scores = [s.score for s in scored_products]
            score_stats["score_avg"] = round(sum(scores) / len(scores), 1)
            score_stats["score_min"] = round(min(scores), 1)
            score_stats["score_max"] = round(max(scores), 1)

        price_stats = {}
        if approved_products:
            prices = [p.price for p in approved_products]
            discounts = [
                p.discount_pct for p in approved_products if p.discount_pct > 0
            ]
            price_stats["price_min"] = min(prices)
            price_stats["price_max"] = max(prices)
            if discounts:
                price_stats["discount_avg"] = round(sum(discounts) / len(discounts), 1)

        stats["timings"] = timings
        stats["score_stats"] = score_stats
        stats["price_stats"] = price_stats

    return stats


async def main():
    """Entry point com health check inicial."""
    start_time = time.time()

    # Health check antes de começar
    checker = HealthCheck()
    report = await checker.run()

    if not report.overall_healthy:
        logger.warning("unhealthy_services", summary=report.summary())

    # Executa pipeline
    stats = await run_pipeline()

    elapsed_time = round(time.time() - start_time, 2)
    stats["elapsed_seconds"] = elapsed_time
    logger.info("pipeline_summary", **stats)


if __name__ == "__main__":
    asyncio.run(main())
