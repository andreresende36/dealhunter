"""
DealHunter — Sender
Envia a próxima oferta da fila de prioridade (maior score primeiro).

Fluxo:
  1. Consulta próxima oferta não enviada (view vw_approved_unsent)
  2. Gera imagem lifestyle via IA (Haiku + Gemini)
  3. Upload da imagem para Supabase Storage
  4. Obtém/cria link de afiliado
  5. Formata mensagem com imagem lifestyle
  6. Publica via Telegram
  7. Marca como enviada
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

import structlog

from src.config import settings
from src.scraper.base_scraper import ScrapedProduct
from src.distributor.affiliate_links import AffiliateLinkBuilder
from src.distributor.message_formatter import MessageFormatter
from src.distributor.telegram_bot import TelegramBot
from src.image.lifestyle_generator import generate_lifestyle_image
from src.image.image_storage import upload_to_supabase

if TYPE_CHECKING:
    from src.database.storage_manager import StorageManager

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Tipagem da linha retornada pela view vw_approved_unsent
# ---------------------------------------------------------------------------


class UnsentOfferRow(TypedDict):
    product_id: str
    scored_offer_id: str
    ml_id: str
    title: str
    product_url: str
    current_price: float
    original_price: float | None
    discount_percent: float
    rating_stars: float | None
    rating_count: int | None
    category: str | None
    thumbnail_url: str | None
    free_shipping: bool
    installments_without_interest: bool
    badge: str | None
    final_score: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _offer_to_product(offer: UnsentOfferRow) -> ScrapedProduct:
    """Converte a linha da view vw_approved_unsent em ScrapedProduct."""
    return ScrapedProduct(
        ml_id=offer["ml_id"],
        url=offer["product_url"],
        title=offer["title"],
        price=float(offer["current_price"]),
        original_price=float(offer["original_price"]) if offer.get("original_price") else None,
        discount_pct=float(offer.get("discount_percent", 0)),
        rating=float(offer.get("rating_stars") or 0),
        review_count=int(offer.get("rating_count") or 0),
        category=offer.get("category") or "",
        image_url=offer.get("thumbnail_url") or "",
        free_shipping=bool(offer.get("free_shipping", False)),
        installments_without_interest=bool(offer.get("installments_without_interest", False)),
        badge=offer.get("badge") or "",
    )


async def _generate_and_upload_image(
    storage: StorageManager,
    product_id: str,
    thumbnail_url: str,
    ml_id: str,
) -> str | None:
    """
    Gera imagem lifestyle e faz upload para Supabase Storage.
    Retorna a URL pública da imagem ou None se todas as tentativas falharem.
    """
    max_retries = settings.sender.image_max_retries

    for attempt in range(1, max_retries + 1):
        logger.info(
            "lifestyle_generating",
            ml_id=ml_id,
            attempt=attempt,
            max_retries=max_retries,
        )

        image_bytes = await generate_lifestyle_image(thumbnail_url)
        if image_bytes is None:
            logger.warning("lifestyle_attempt_failed", ml_id=ml_id, attempt=attempt)
            continue

        public_url = await upload_to_supabase(product_id, image_bytes, "jpg")
        if public_url:
            await storage.update_image_status(
                product_id, "enhanced", enhanced_url=public_url
            )
            logger.info("lifestyle_uploaded", ml_id=ml_id, url=public_url[:80])
            return public_url

        logger.warning("lifestyle_upload_failed", ml_id=ml_id, attempt=attempt)

    logger.error("lifestyle_permanently_failed", ml_id=ml_id, retries=max_retries)
    return None


# ---------------------------------------------------------------------------
# Interface pública
# ---------------------------------------------------------------------------


async def send_next_offer(
    storage: StorageManager,
    telegram_bot: TelegramBot | None = None,
) -> bool:
    """
    Envia a próxima oferta da fila.

    Args:
        storage: StorageManager compartilhado com o pipeline.
        telegram_bot: Instância injetável (criada internamente se None).
                      Útil para testes e para reutilizar a mesma conexão.

    Returns:
        True se uma oferta foi enviada, False se a fila está vazia.
    """
    offer: UnsentOfferRow | None = await storage.get_next_unsent_offer()
    if not offer:
        logger.debug("send_queue_empty")
        return False

    product_id = offer["product_id"]
    scored_offer_id = offer["scored_offer_id"]
    thumbnail_url = offer.get("thumbnail_url") or ""
    ml_id = offer["ml_id"]

    logger.info(
        "sending_offer",
        ml_id=ml_id,
        title=offer["title"][:50],
        score=offer["final_score"],
    )

    # 1. Gerar imagem lifestyle via IA
    enhanced_image_url: str | None = None
    if thumbnail_url and settings.openrouter.api_key:
        enhanced_image_url = await _generate_and_upload_image(
            storage, product_id, thumbnail_url, ml_id
        )

    if not enhanced_image_url:
        logger.info("sending_with_original_thumbnail", ml_id=ml_id)

    # 2. Link de afiliado
    short_url = offer["product_url"]
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
            aff_url = await aff_builder.get_or_create(offer["product_url"], product_id)
            short_url = aff_url or offer["product_url"]
    except Exception as exc:
        logger.warning("affiliate_link_failed", ml_id=ml_id, error=str(exc))

    # 3. Formatar mensagem (com imagem lifestyle ou thumbnail original)
    product = _offer_to_product(offer)
    formatter = MessageFormatter()
    msg = formatter.format(
        product,
        short_link=short_url,
        enhanced_image_url=enhanced_image_url,
    )

    # 4. Publicar no Telegram
    if not settings.telegram.bot_token or not settings.telegram.group_ids:
        logger.warning("telegram_not_configured")
        return False

    bot = telegram_bot or TelegramBot()
    results = await bot.publish(msg)
    sent_ok = any(r["success"] for r in results)

    if sent_ok:
        try:
            await storage.mark_as_sent(scored_offer_id, channel="telegram")
        except Exception as exc:
            logger.warning("mark_as_sent_failed", ml_id=ml_id, error=str(exc))

        logger.info(
            "offer_sent",
            ml_id=ml_id,
            score=offer["final_score"],
            has_lifestyle_image=enhanced_image_url is not None,
            link=short_url[:60],
        )
    else:
        logger.error("offer_send_failed", ml_id=ml_id, results=results)

    return sent_ok
