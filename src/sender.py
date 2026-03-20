"""
DealHunter — Sender (Style Guide v3)
Envia a próxima oferta da fila de prioridade (maior score primeiro).

Fluxo:
  1. Consulta próxima oferta não enviada (view vw_approved_unsent)
  2. Gera título catchy via IA (Haiku)
  3. Seleciona melhor imagem real do produto (3 camadas)
  4. Upload da imagem para Supabase Storage
  5. Obtém/cria link de afiliado
  6. Formata mensagem com template Style Guide v3
  7. Publica via Telegram
  8. Marca como enviada
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

import structlog

from src.config import settings
from src.scraper.base_scraper import ScrapedProduct
from src.distributor.affiliate_links import AffiliateLinkBuilder
from src.distributor.message_formatter import MessageFormatter
from src.distributor.telegram_bot import TelegramBot
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
    pix_price: float | None
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
    pix_price_raw = offer.get("pix_price")
    orig_price_raw = offer.get("original_price")
    return ScrapedProduct(
        ml_id=offer["ml_id"],
        url=offer["product_url"],
        title=offer["title"],
        price=float(offer["current_price"]),
        original_price=(
            float(orig_price_raw) if orig_price_raw is not None else None
        ),
        pix_price=float(pix_price_raw) if pix_price_raw else None,
        discount_pct=float(offer.get("discount_percent") or 0),
        rating=float(offer.get("rating_stars") or 0),
        review_count=int(offer.get("rating_count") or 0),
        category=offer.get("category") or "",
        image_url=offer.get("thumbnail_url") or "",
        free_shipping=bool(offer.get("free_shipping", False)),
        installments_without_interest=bool(
            offer.get("installments_without_interest", False)
        ),
        badge=offer.get("badge") or "",
    )


async def _select_and_upload_image(
    storage: StorageManager,
    product_id: str,
    ml_id: str,
    product_title: str,
    thumbnail_url: str,
    category: str,
    product_url: str = "",
    force_new: bool = False,
) -> tuple[str | None, bytes | None]:
    """
    Gera imagem lifestyle via IA e faz upload para Supabase Storage.
    Reutiliza imagem existente se já houver uma selecionada para este produto.

    Args:
        force_new: Pula cache de imagem existente (usado no retry de validação).

    Returns:
        Tupla (URL pública, bytes da imagem) ou (None, None).
    """
    # Reutiliza imagem já processada — evita custo duplicado
    if not force_new:
        existing_url = await storage.get_enhanced_image_url(product_id)
        if existing_url:
            logger.info("image_reusing_existing", ml_id=ml_id, url=existing_url[:80])
            return existing_url, None

    from src.image.lifestyle_generator import generate_lifestyle_image

    image_bytes: bytes | None = None
    max_retries = settings.sender.image_max_retries
    for attempt in range(1, max_retries + 1):
        image_bytes = await generate_lifestyle_image(thumbnail_url)
        if image_bytes:
            break
        logger.warning(
            "lifestyle_retry",
            ml_id=ml_id,
            attempt=attempt,
            max_retries=max_retries,
        )

    if not image_bytes:
        logger.warning("image_selection_no_bytes", ml_id=ml_id)
        return None, None

    # Upload para Supabase Storage
    public_url = await upload_to_supabase(product_id, image_bytes, "jpg")
    if public_url:
        await storage.update_image_status(
            product_id, "enhanced", enhanced_url=public_url
        )
        logger.info("image_uploaded", ml_id=ml_id, source="lifestyle", url=public_url[:80])
        return public_url, image_bytes

    logger.warning("image_upload_failed", ml_id=ml_id)
    return None, None


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
    raw_offer = await storage.get_next_unsent_offer()
    if not raw_offer:
        logger.debug("send_queue_empty")
        return False
    offer: UnsentOfferRow = raw_offer  # type: ignore[assignment]

    product_id = offer["product_id"]
    scored_offer_id = offer["scored_offer_id"]
    thumbnail_url = offer.get("thumbnail_url") or ""
    ml_id = offer["ml_id"]
    product_title = offer["title"]
    category = offer.get("category") or ""

    logger.info(
        "sending_offer",
        ml_id=ml_id,
        title=product_title[:50],
        score=offer["final_score"],
    )

    # 1. Link de afiliado (não muda entre tentativas)
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
            aff_url = await aff_builder.get_or_create(
                offer["product_url"], product_id
            )
            short_url = aff_url or offer["product_url"]
    except Exception as exc:
        logger.warning("affiliate_link_failed", ml_id=ml_id, error=str(exc))

    product = _offer_to_product(offer)
    formatter = MessageFormatter()

    from src.distributor.title_generator import generate_catchy_title

    # 2. Gerar título catchy via IA (se disponível)
    catchy_title: str | None = None
    if settings.openrouter.api_key:
        try:
            catchy_title = await generate_catchy_title(
                product_title=product_title,
                category=category,
                price=float(offer["current_price"]),
                original_price=(
                    float(offer["original_price"])
                    if offer.get("original_price") else None
                ),
            )
        except Exception as exc:
            logger.warning("title_generation_failed", ml_id=ml_id, error=str(exc))

    # 3. Selecionar melhor imagem real do produto
    enhanced_image_url: str | None = None
    image_bytes: bytes | None = None
    if thumbnail_url:
        enhanced_image_url, image_bytes = await _select_and_upload_image(
            storage, product_id, ml_id, product_title,
            thumbnail_url, category,
            product_url=offer["product_url"],
        )

    if not enhanced_image_url:
        logger.info("sending_with_original_thumbnail", ml_id=ml_id)

    # 4. Formatar mensagem (Style Guide v3)
    msg = formatter.format(
        product,
        short_link=short_url,
        catchy_title=catchy_title,
        enhanced_image_url=enhanced_image_url,
    )

    # 5. Validar mensagem (soft — loga mas não bloqueia)
    from src.distributor.message_validator import validate_message
    validate_message(
        whatsapp_text=msg.whatsapp_text,
        free_shipping=product.free_shipping,
        rating=product.rating,
        review_count=product.review_count,
        has_image=msg.image_url is not None,
    )

    # 7. Publicar no Telegram
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
            image_source=enhanced_image_url is not None,
            catchy_title=catchy_title or "(fallback)",
            link=short_url[:60],
        )
    else:
        logger.error("offer_send_failed", ml_id=ml_id, results=results)

    return sent_ok
