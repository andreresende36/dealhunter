"""
Testes para o Message Formatter e Affiliate Links.
"""

import pytest
from unittest.mock import patch

from src.scraper.base_scraper import ScrapedProduct
from src.distributor.message_formatter import MessageFormatter
from src.distributor.affiliate_links import AffiliateLinkBuilder


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def formatter() -> MessageFormatter:
    return MessageFormatter()


@pytest.fixture
def sample_product() -> ScrapedProduct:
    return ScrapedProduct(
        ml_id="MLB123456789",
        url="https://www.mercadolivre.com.br/tenis/p/MLB123456789",
        title="Tênis Nike Air Max 270 React Masculino Preto e Branco",
        price=299.90,
        original_price=599.90,
        rating=4.8,
        review_count=1500,
        category="Calçados",
        free_shipping=True,
        image_url="https://http2.mlstatic.com/image.jpg",
    )


# ---------------------------------------------------------------------------
# MessageFormatter
# ---------------------------------------------------------------------------


class TestMessageFormatter:
    def test_format_returns_formatted_message(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert msg.telegram_text
        assert msg.whatsapp_text
        assert msg.product_ml_id == "MLB123456789"

    def test_telegram_contains_price(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "299" in msg.telegram_text
        assert "599" in msg.telegram_text

    def test_telegram_contains_discount(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "50%" in msg.telegram_text

    def test_telegram_contains_free_shipping(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "Frete" in msg.telegram_text or "grátis" in msg.telegram_text.lower()

    def test_no_free_shipping_not_shown(self, formatter, sample_product):
        sample_product.free_shipping = False
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "Frete Grátis" not in msg.telegram_text

    def test_whatsapp_has_no_inline_link_markdown(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        # WhatsApp não suporta [texto](link) — deve ter a URL direta
        assert "[Comprar agora]" not in msg.whatsapp_text
        assert "https://s.black/abc" in msg.whatsapp_text

    def test_format_price_br_style(self, formatter):
        assert formatter._format_price(1299.90) == "1.299,90"
        assert formatter._format_price(99.0) == "99,00"

    def test_discount_emoji_high_discount(self, formatter):
        assert "🔥🔥🔥" == formatter._get_discount_emoji(85)

    def test_discount_emoji_medium(self, formatter):
        emoji = formatter._get_discount_emoji(35)
        assert emoji in ["⚡", "🔥"]

    def test_truncate(self, formatter):
        long_title = "A" * 70
        assert len(formatter._truncate(long_title, 60)) <= 60
        assert formatter._truncate("curto", 60) == "curto"

    def test_hashtags_include_sempreblack(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert (
            "#SempreBlack" in msg.telegram_text or "#SempreBlack" in msg.whatsapp_text
        )

    def test_custom_title_used(self, formatter, sample_product):
        custom = "Oferta Incrível de Tênis"
        msg = formatter.format(
            sample_product,
            short_link="https://s.black/abc",
            custom_title=custom,
        )
        assert custom in msg.telegram_text

    def test_product_without_original_price(self, formatter, sample_product):
        sample_product.original_price = None
        sample_product.discount_pct = 0.0
        # Não deve lançar exceção
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert msg.telegram_text


# ---------------------------------------------------------------------------
# AffiliateLinkBuilder
# ---------------------------------------------------------------------------


class TestAffiliateLinkBuilder:
    def setup_method(self):
        with patch("src.distributor.affiliate_links.settings") as mock_cfg:
            mock_cfg.mercado_livre.affiliate_id = "test_affiliate"
            mock_cfg.mercado_livre.affiliate_tag = "sempreblack"
            self.builder = AffiliateLinkBuilder.__new__(AffiliateLinkBuilder)
            self.builder.cfg = mock_cfg.mercado_livre

    def test_build_adds_params(self):
        url = "https://www.mercadolivre.com.br/tenis/p/MLB123"
        result = self.builder.build(url)
        assert "matt_tool" in result
        assert "matt_campaign" in result

    def test_non_ml_url_returned_unchanged(self):
        url = "https://www.amazon.com.br/produto"
        result = self.builder.build(url)
        assert result == url

    def test_empty_url_returned_unchanged(self):
        assert self.builder.build("") == ""

    def test_extract_ml_id(self):
        url = "https://www.mercadolivre.com.br/tenis/p/MLB987654321"
        assert self.builder.extract_ml_id(url) == "MLB987654321"

    def test_is_ml_url(self):
        assert self.builder._is_ml_url("https://www.mercadolivre.com.br/")
        assert self.builder._is_ml_url("https://mercadolibre.com/")
        assert not self.builder._is_ml_url("https://amazon.com.br/")
