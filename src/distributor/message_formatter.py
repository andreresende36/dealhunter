"""
DealHunter — Message Formatter (Style Guide v3)
Gera mensagens formatadas para WhatsApp e Telegram.
Template baseado na análise de 2.104 mensagens do grupo Sempre Black.
"""


from dataclasses import dataclass
from typing import Optional

import structlog

from src.scraper.base_scraper import ScrapedProduct
from src.utils.brands import extract_brand as _extract_brand_raw

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# MarkdownV2 escaping (Telegram)
# ---------------------------------------------------------------------------


_MDV2_TRANS: dict[int, str] = {
    ord(c): f'\\{c}' for c in r'_*[]()~`>#+-=|{}.!'
}

_MDV2_URL_UNSAFE: frozenset[str] = (
    frozenset(r'_*[]()~`>#+-=|{}.!') - frozenset(':/?=&#@%+,;')
)
_MDV2_URL_TRANS: dict[int, str] = {
    ord(c): f'\\{c}' for c in _MDV2_URL_UNSAFE
}


def _escape_mdv2(text: str) -> str:
    """Escapa caracteres reservados do Telegram MarkdownV2."""
    return text.translate(_MDV2_TRANS)


def _escape_mdv2_url(url: str) -> str:
    """Escapa URL para uso dentro de (...) em inline links do MarkdownV2."""
    return url.translate(_MDV2_URL_TRANS)




def _extract_brand(title: str) -> str | None:
    """Extrai marca conhecida do título do produto (UPPER CASE)."""
    brand = _extract_brand_raw(title)
    return brand.upper() if brand else None


def _fallback_catchy_title(product_title: str) -> str:
    """Gera título catchy rule-based quando IA não está disponível."""
    brand = _extract_brand(product_title)
    if brand:
        title = f"{brand} COM PREÇÃO"
    else:
        title = "OFERTA IMPERDÍVEL"
    return title[:35]


# ---------------------------------------------------------------------------
# Dataclass de saída
# ---------------------------------------------------------------------------


@dataclass
class FormattedMessage:
    """Mensagem pronta para envio."""
    telegram_text: str
    whatsapp_text: str
    image_url: Optional[str]
    short_link: str
    product_ml_id: str


# ---------------------------------------------------------------------------
# Formatter principal
# ---------------------------------------------------------------------------


class MessageFormatter:
    """
    Formata ofertas para publicação nos grupos Sempre Black.

    Template segue o Style Guide v3:
    - Título catchy em CAPS LOCK (gancho, não nome do produto)
    - Nome completo do produto
    - 📉 XX% OFF + bloco de preço com 🤘🏻
    - ✅ Frete Grátis (condicional)
    - ⭐ Avaliação (condicional: rating >= 4.0 e reviews >= 50)
    - 🛒 CTA + link
    - Rodapé fixo "Sempre Black"

    Uso:
        formatter = MessageFormatter()
        msg = formatter.format(product, short_link="https://s.black/abc123")
    """

    def format(
        self,
        product: ScrapedProduct,
        short_link: str,
        catchy_title: Optional[str] = None,
        enhanced_image_url: Optional[str] = None,
    ) -> FormattedMessage:
        """Gera mensagem formatada para Telegram e WhatsApp."""

        # Título catchy (IA ou fallback)
        title = catchy_title or _fallback_catchy_title(product.title)
        title = title.upper()

        # Nome completo do produto (sem truncar)
        product_name = product.title

        # Preço final (pix tem prioridade)
        final_price = product.pix_price or product.price
        original_price = product.original_price or product.price

        # Desconto calculado
        if original_price > 0 and final_price < original_price:
            discount_pct = round((1 - final_price / original_price) * 100)
        else:
            discount_pct = round(product.discount_pct)

        # Sufixo de pagamento
        payment_suffix = self._build_payment_suffix(product)

        # Linhas condicionais
        free_shipping_line = "✅ Frete Grátis\n" if product.free_shipping else ""
        rating_line = self._build_rating_line(product)

        # Formata preços
        final_price_str = self._format_price(final_price)
        original_price_str = self._format_price(original_price)

        # --- WhatsApp (plain text com bold *...* ) ---
        whatsapp_text = self._build_whatsapp(
            title=title,
            product_name=product_name,
            discount_pct=discount_pct,
            original_price_str=original_price_str,
            final_price_str=final_price_str,
            payment_suffix=payment_suffix,
            free_shipping_line=free_shipping_line,
            rating_line=rating_line,
            link=short_link,
        )

        # --- Telegram (MarkdownV2) ---
        telegram_text = self._build_telegram(
            title=title,
            product_name=product_name,
            discount_pct=discount_pct,
            original_price_str=original_price_str,
            final_price_str=final_price_str,
            payment_suffix=payment_suffix,
            free_shipping_line=free_shipping_line,
            rating_line=rating_line,
            link=short_link,
        )

        image_url = enhanced_image_url or product.image_url or None

        return FormattedMessage(
            telegram_text=telegram_text,
            whatsapp_text=whatsapp_text,
            image_url=image_url,
            short_link=short_link,
            product_ml_id=product.ml_id,
        )

    # ------------------------------------------------------------------
    # Builders
    # ------------------------------------------------------------------

    def _build_whatsapp(
        self,
        title: str,
        product_name: str,
        discount_pct: int,
        original_price_str: str,
        final_price_str: str,
        payment_suffix: str,
        free_shipping_line: str,
        rating_line: str,
        link: str,
    ) -> str:
        """Monta mensagem WhatsApp (plain text com bold *...*)."""
        lines = [
            f"*{title}*",
            "",
            product_name,
            "",
            f"📉 {discount_pct}% OFF",
            f"de R${original_price_str} *por R$ {final_price_str}{payment_suffix}* 🤘🏻",
        ]

        # Bloco condicional (frete + avaliação)
        conditional = ""
        if free_shipping_line or rating_line:
            conditional = f"\n{free_shipping_line}{rating_line}"
        lines.append(conditional)

        lines.extend([
            f"🛒 Comprar agora! {link}",
            "",
            "━━━━━━━━━━━━━━━",
            "Sempre Black — Aqui todo dia é Black Friday 🖤",
        ])

        return "\n".join(lines)

    def _build_telegram(
        self,
        title: str,
        product_name: str,
        discount_pct: int,
        original_price_str: str,
        final_price_str: str,
        payment_suffix: str,
        free_shipping_line: str,
        rating_line: str,
        link: str,
    ) -> str:
        """Monta mensagem Telegram (MarkdownV2)."""
        esc_title = _escape_mdv2(title)
        esc_name = _escape_mdv2(product_name)
        esc_discount = _escape_mdv2(str(discount_pct))
        esc_original = _escape_mdv2(original_price_str)
        esc_final = _escape_mdv2(final_price_str)
        esc_suffix = _escape_mdv2(payment_suffix)
        esc_link = _escape_mdv2_url(link)

        # Frete e avaliação já vêm como texto puro — escapar
        esc_shipping = (
            _escape_mdv2("✅ Frete Grátis") + "\n"
            if free_shipping_line else ""
        )
        esc_rating = _escape_mdv2(rating_line) if rating_line else ""

        lines = [
            f"*{esc_title}*",
            "",
            esc_name,
            "",
            f"📉 {esc_discount}% OFF",
            f"de R$ {esc_original} *por R$ {esc_final}{esc_suffix}* 🤘🏻",
        ]

        conditional = ""
        if esc_shipping or esc_rating:
            conditional = f"\n{esc_shipping}{esc_rating}"
        lines.append(conditional)

        lines.extend([
            f"🛒 [Comprar agora\\!]({esc_link})",
            "",
            "━━━━━━━━━━━━━━━",
            "_Sempre Black — Aqui todo dia é Black Friday_ 🖤",
        ])

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_payment_suffix(self, product: ScrapedProduct) -> str:
        """Retorna sufixo de pagamento: ' no pix', ' em Nx', ou vazio."""
        if product.pix_price and product.pix_price < product.price:
            return " no pix"
        if product.installments_without_interest:
            return " em 10x"
        return ""

    def _build_rating_line(self, product: ScrapedProduct) -> str:
        """Retorna linha de avaliação se rating >= 4.0 e reviews >= 50."""
        if product.rating >= 4.0 and product.review_count >= 50:
            count_fmt = f"{product.review_count:,}".replace(",", ".")
            return f"⭐ {product.rating}/5 ({count_fmt} avaliações)\n"
        return ""

    def _format_price(self, value: float) -> str:
        """Formata número para preço brasileiro. Ex: 1299.9 → '1.299,90'"""
        return (
            f"{value:,.2f}"
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
