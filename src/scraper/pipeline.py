"""
Crivo — Pipeline de Scraping
Coleta ofertas do Mercado Livre, pontua, salva no banco.

Fluxo: scraping cards → dedup → fake filter → score → save → affiliate links.
O envio ao Telegram é feito pelo sender_loop em runner.py.
"""

import time
from typing import Any

import structlog

from src.config import settings
from src.scraper.ml_scraper import MLScraper
from src.analyzer.fake_discount_detector import FakeDiscountDetector
from src.analyzer.score_engine import ScoreEngine, RejectReason
from src.analyzer.card_debugger import generate_report
from src.database.storage_manager import StorageManager
from src.distributor.affiliate_links import AffiliateLinkBuilder
from src.monitoring.alert_bot import AlertBot

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Sub-etapas do pipeline
# ---------------------------------------------------------------------------


async def _run_scraping(
    stats: dict[str, Any], timings: dict[str, float]
) -> tuple[list, MLScraper]:
    """Etapa 1: Coleta ofertas via scraper."""
    scraper = MLScraper()
    alert_bot = AlertBot()
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
    return all_products, scraper


async def _filter_products(
    all_products: list,
    storage: StorageManager,
    stats: dict[str, Any],
    timings: dict[str, float],
) -> list:
    """Etapas 2-3: Deduplicação + filtro de desconto falso."""
    t0 = time.time()
    recent_ids = await storage.get_recently_sent_ids(hours=24)
    new_products = [p for p in all_products if p.ml_id not in recent_ids]
    timings["dedup"] = round(time.time() - t0, 2)
    logger.info("dedup_done", total=len(all_products), new=len(new_products))

    t0 = time.time()
    fake_detector = FakeDiscountDetector()
    fake_results = fake_detector.check_batch(new_products)
    genuine_products = [p for p, r in fake_results if not r.is_fake]
    stats["new"] = len(genuine_products)
    timings["fake_filter"] = round(time.time() - t0, 2)
    logger.info(
        "fake_filter_done",
        genuine=len(genuine_products),
        fake=len(new_products) - len(genuine_products),
    )
    return genuine_products


def _score_products(
    genuine_products: list,
    scraper: MLScraper,
    stats: dict[str, Any],
    timings: dict[str, float],
) -> list:
    """Etapa 4: Avaliação por score + debug report."""
    t0 = time.time()
    score_engine = ScoreEngine()
    all_scored = [score_engine.evaluate(p) for p in genuine_products]
    scored_products = sorted(
        [s for s in all_scored if s.passed],
        key=lambda s: s.score,
        reverse=True,
    )
    rejected_products = [s for s in all_scored if not s.passed]

    reject_reasons: dict[str, int] = {}
    for s in rejected_products:
        key = s.reject_reason.value if s.reject_reason else RejectReason.LOW_SCORE.value
        reject_reasons[key] = reject_reasons.get(key, 0) + 1
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

    if settings.scraper.debug_screenshots and rejected_products:
        _generate_debug_report(rejected_products, scraper)

    return scored_products


def _generate_debug_report(rejected_products: list, scraper: MLScraper) -> None:
    """Gera relatório HTML com os cards rejeitados (modo debug)."""
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


async def _build_affiliate_links(
    storage: StorageManager,
    ids: dict,
    approved_products: list,
    stats: dict[str, Any],
    timings: dict[str, float],
) -> None:
    """Etapa 6: Gera links de afiliado via API do ML."""
    t1 = time.time()
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


async def _save_products_fallback(
    storage: StorageManager,
    scored_products: list,
    stats: dict[str, Any],
) -> None:
    """Fallback: salva individualmente se o batch falhar."""
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
            logger.error("save_failed", ml_id=product.ml_id, error=str(exc2))
            stats["errors"] += 1


async def _save_approved(
    storage: StorageManager,
    scored_products: list,
    stats: dict[str, Any],
    timings: dict[str, float],
) -> list:
    """Etapa 5-6: Salva produtos aprovados no banco + cria affiliate links."""
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
            if scored_entries:
                try:
                    await storage.save_scored_offers_batch(scored_entries)
                except Exception as exc_so:
                    logger.warning("scored_offers_batch_save_failed", error=str(exc_so))

            await _build_affiliate_links(storage, ids, approved_products, stats, timings)

        except Exception as exc:
            logger.error("batch_save_failed", error=str(exc))
            await _save_products_fallback(storage, scored_products, stats)

    timings["saving"] = round(time.time() - t0, 2)
    return approved_products


# ---------------------------------------------------------------------------
# Função principal do pipeline
# ---------------------------------------------------------------------------


async def run_pipeline(storage: StorageManager) -> dict:
    """
    Executa o pipeline de scraping do Crivo.

    Coleta ofertas dos cards de listagem, filtra duplicatas e descontos falsos,
    pontua e salva no banco. O envio é feito separadamente pelo sender.

    Args:
        storage: Instância do StorageManager (compartilhada com o sender).

    Retorna dict com estatísticas da execução.
    """
    stats: dict[str, Any] = {
        "scraped": 0,
        "new": 0,
        "scored": 0,
        "approved": 0,
        "rejected": 0,
        "saved": 0,
        "affiliate_links": 0,
        "errors": 0,
    }
    timings: dict[str, float] = {}

    logger.info("pipeline_start", env=settings.env)

    all_products, scraper = await _run_scraping(stats, timings)
    if not all_products:
        logger.warning("no_products_scraped")
        return stats

    genuine_products = await _filter_products(all_products, storage, stats, timings)
    scored_products = _score_products(genuine_products, scraper, stats, timings)
    approved_products = await _save_approved(storage, scored_products, stats, timings)

    score_stats: dict[str, Any] = {}
    if scored_products:
        scores = [s.score for s in scored_products]
        score_stats["score_avg"] = round(sum(scores) / len(scores), 1)
        score_stats["score_min"] = round(min(scores), 1)
        score_stats["score_max"] = round(max(scores), 1)

    price_stats: dict[str, Any] = {}
    if approved_products:
        prices = [p.price for p in approved_products]
        discounts = [p.discount_pct for p in approved_products if p.discount_pct > 0]
        price_stats["price_min"] = min(prices)
        price_stats["price_max"] = max(prices)
        if discounts:
            price_stats["discount_avg"] = round(sum(discounts) / len(discounts), 1)

    stats["timings"] = timings
    stats["score_stats"] = score_stats
    stats["price_stats"] = price_stats

    return stats
