"""
DealHunter — Message Formatter
Gera mensagens formatadas para WhatsApp e Telegram.
Slogan: "Todo dia é Black Friday" — tom entusiasmado mas não spam.
"""

from dataclasses import dataclass
from typing import Optional

import structlog

from src.scraper.base_scraper import ScrapedProduct

logger = structlog.get_logger(__name__)

# Tabela de tradução pré-computada para MarkdownV2 (str.translate é ~5-10x mais rápido
# que iterar char-a-char). Mapeia cada char reservado para '\\' + char.
_MDV2_TRANS: dict[int, str] = {
    ord(c): f'\\{c}' for c in r'_*[]()~`>#+-=|{}.!'
}

# Para URLs: mesmos chars reservados exceto os estruturais de URL
_MDV2_URL_UNSAFE: frozenset[str] = frozenset(r'_*[]()~`>#+-=|{}.!') - frozenset(':/?=&#@%+,;')
_MDV2_URL_TRANS: dict[int, str] = {
    ord(c): f'\\{c}' for c in _MDV2_URL_UNSAFE
}


def _escape_mdv2(text: str) -> str:
    """Escapa caracteres reservados do Telegram MarkdownV2."""
    return text.translate(_MDV2_TRANS)


def _escape_mdv2_url(url: str) -> str:
    """Escapa URL para uso dentro de (...) em inline links do MarkdownV2.

    Conforme a spec do Telegram, dentro de () apenas ')' e '\\' precisam escape.
    Na prática, o parser também quebra com outros chars — escapamos tudo que é reservado
    exceto ':', '/', '?' e demais estruturais de URL.
    """
    return url.translate(_MDV2_URL_TRANS)


# Emojis por faixa de desconto
DISCOUNT_EMOJIS = {
    80: "🔥🔥🔥",
    60: "🔥🔥",
    40: "🔥",
    20: "⚡",
    0:  "💡",
}

# Templates de mensagem
# Nota: em MarkdownV2, chars como . - ! ( ) devem ser escapados no texto estático.
# Dentro de [text](url), o text segue regras normais e a url só precisa escapar ) e \.
TELEGRAM_TEMPLATE = """{discount_emoji} *{title}*

💰 De ~~R$ {original_price}~~ por *R$ {price}*
📉 *{discount_pct}% OFF*{free_shipping_line}

{description}

{hashtags}

🛒 [Comprar agora\!]({link})
━━━━━━━━━━━━━━━
_Sempre Black — Todo dia é Black Friday_ 🖤"""

WHATSAPP_TEMPLATE = """{discount_emoji} *{title}*

💰 De R$ {original_price} por *R$ {price}*
📉 *{discount_pct:.0f}% OFF*{free_shipping_line}

{description}

{hashtags}

🛒 {link}

_Sempre Black — Todo dia é Black Friday_ 🖤"""

TELEGRAM_TEMPLATE_NO_ORIGINAL = """{discount_emoji} *{title}*

💰 *R$ {price}*{free_shipping_line}

{description}

{hashtags}

🛒 [Comprar agora\!]({link})
━━━━━━━━━━━━━━━
_Sempre Black — Todo dia é Black Friday_ 🖤"""


@dataclass
class FormattedMessage:
    """Mensagem pronta para envio."""
    telegram_text: str
    whatsapp_text: str
    image_url: Optional[str]
    short_link: str
    product_ml_id: str


class MessageFormatter:
    """
    Formata ofertas para publicação nos grupos.

    Uso:
        formatter = MessageFormatter()
        msg = formatter.format(product, short_link="https://s.black/abc123")
    """

    def format(
        self,
        product: ScrapedProduct,
        short_link: str,
        custom_title: Optional[str] = None,
        custom_description: Optional[str] = None,
        hashtags: Optional[list[str]] = None,
        enhanced_image_url: Optional[str] = None,
    ) -> FormattedMessage:
        """Gera mensagem formatada para Telegram e WhatsApp."""

        title = custom_title or self._truncate(product.title, 60)
        description = custom_description or self._generate_description(product)
        tags = self._build_hashtags(product, hashtags)
        discount_emoji = self._get_discount_emoji(product.discount_pct)
        free_shipping_line = "\n✅ *Frete Grátis*" if product.free_shipping else ""

        # Formata preços
        price_str = self._format_price(product.price)
        original_str = (
            self._format_price(product.original_price)
            if product.original_price
            else None
        )

        # Telegram — escapa conteúdo dinâmico para MarkdownV2
        esc_title = _escape_mdv2(title)
        esc_price = _escape_mdv2(price_str)
        esc_original = _escape_mdv2(original_str) if original_str else None
        esc_discount = _escape_mdv2(f"{product.discount_pct:.0f}")
        esc_desc = _escape_mdv2(description)
        esc_tags = _escape_mdv2(tags)
        esc_shipping = (
            "\n✅ *Frete Grátis*" if product.free_shipping else ""
        )
        esc_link = _escape_mdv2_url(short_link)

        if esc_original:
            telegram_text = TELEGRAM_TEMPLATE.format(
                discount_emoji=discount_emoji,
                title=esc_title,
                original_price=esc_original,
                price=esc_price,
                discount_pct=esc_discount,
                free_shipping_line=esc_shipping,
                description=esc_desc,
                hashtags=esc_tags,
                link=esc_link,
            )
        else:
            telegram_text = TELEGRAM_TEMPLATE_NO_ORIGINAL.format(
                discount_emoji=discount_emoji,
                title=esc_title,
                price=esc_price,
                free_shipping_line=esc_shipping,
                description=esc_desc,
                hashtags=esc_tags,
                link=esc_link,
            )

        # WhatsApp (sem markdown de link inline, sem tachado com ~~)
        whatsapp_text = WHATSAPP_TEMPLATE.format(
            discount_emoji=discount_emoji,
            title=title,
            original_price=original_str or price_str,
            price=price_str,
            discount_pct=product.discount_pct,
            free_shipping_line=free_shipping_line,
            description=description,
            hashtags=tags,
            link=short_link,
        )

        # Usa imagem aprimorada se disponível, senão a thumbnail original
        image_url = enhanced_image_url or product.image_url or None

        return FormattedMessage(
            telegram_text=telegram_text,
            whatsapp_text=whatsapp_text,
            image_url=image_url,
            short_link=short_link,
            product_ml_id=product.ml_id,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _format_price(self, value: float) -> str:
        """Formata número para preço brasileiro. Ex: 1299.9 → '1.299,90'"""
        return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    def _get_discount_emoji(self, pct: float) -> str:
        for threshold in sorted(DISCOUNT_EMOJIS.keys(), reverse=True):
            if pct >= threshold:
                return DISCOUNT_EMOJIS[threshold]
        return "💡"

    def _truncate(self, text: str, max_len: int) -> str:
        if len(text) <= max_len:
            return text
        return text[:max_len - 3] + "..."

    def _generate_description(self, product: ScrapedProduct) -> str:
        """Gera descrição automática baseada nos atributos do produto."""
        parts = []

        if product.rating >= 4.5:
            parts.append(f"⭐ {product.rating}/5 ({product.review_count} avaliações)")
        elif product.rating >= 4.0:
            parts.append(f"⭐ Bem avaliado: {product.rating}/5")

        return " · ".join(parts) if parts else "Oferta selecionada pelo DealHunter"

    def _build_hashtags(
        self, product: ScrapedProduct, custom: Optional[list[str]] = None
    ) -> str:
        """Constrói linha de hashtags."""
        base_tags = ["#SempreBlack", "#BlackFriday", "#Oferta"]

        if product.category:
            category_tag = "#" + product.category.replace(" ", "").replace("&", "e")
            base_tags.append(category_tag)

        if product.free_shipping:
            base_tags.append("#FreteGrátis")

        tags = custom if custom else base_tags
        return " ".join(tags[:6])  # Máx 6 hashtags
