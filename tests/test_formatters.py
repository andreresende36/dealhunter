"""
Testes para o Message Formatter e Affiliate Links.
Atualizado para Style Guide v3.
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
# MessageFormatter (Style Guide v3)
# ---------------------------------------------------------------------------


class TestMessageFormatter:
    def test_format_returns_formatted_message(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert msg.telegram_text
        assert msg.whatsapp_text
        assert msg.product_ml_id == "MLB123456789"

    def test_contains_prices(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "299" in msg.whatsapp_text
        assert "599" in msg.whatsapp_text

    def test_contains_discount_pct(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "📉" in msg.whatsapp_text
        assert "% OFF" in msg.whatsapp_text

    def test_contains_rock_on_emoji(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "🤘🏻" in msg.whatsapp_text

    def test_contains_free_shipping(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "✅ Frete Grátis" in msg.whatsapp_text

    def test_no_free_shipping_not_shown(self, formatter, sample_product):
        sample_product.free_shipping = False
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "Frete Grátis" not in msg.whatsapp_text

    def test_rating_shown_when_good(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "⭐ 4.8/5" in msg.whatsapp_text
        assert "avaliações" in msg.whatsapp_text

    def test_rating_hidden_when_low(self, formatter, sample_product):
        sample_product.rating = 3.5
        sample_product.review_count = 20
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "⭐" not in msg.whatsapp_text

    def test_rating_hidden_when_few_reviews(self, formatter, sample_product):
        sample_product.rating = 4.5
        sample_product.review_count = 30
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "⭐" not in msg.whatsapp_text

    def test_whatsapp_has_no_inline_link_markdown(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "[Comprar agora" not in msg.whatsapp_text
        assert "https://s.black/abc" in msg.whatsapp_text

    def test_telegram_has_inline_link(self, formatter, sample_product):
        msg = formatter.format(
            sample_product, short_link="https://s.black/abc"
        )
        assert "[Comprar agora\\!]" in msg.telegram_text

    def test_footer_present(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "━━━" in msg.whatsapp_text
        assert "Sempre Black" in msg.whatsapp_text
        assert "Aqui todo dia é Black Friday" in msg.whatsapp_text
        assert "🖤" in msg.whatsapp_text

    def test_no_hashtags(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "#" not in msg.whatsapp_text

    def test_no_fire_emoji(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "🔥" not in msg.whatsapp_text

    def test_catchy_title_used(self, formatter, sample_product):
        msg = formatter.format(
            sample_product,
            short_link="https://s.black/abc",
            catchy_title="NIKE CLÁSSICO COM PREÇÃO",
        )
        assert "*NIKE CLÁSSICO COM PREÇÃO*" in msg.whatsapp_text

    def test_catchy_title_fallback(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        first_line = msg.whatsapp_text.split("\n")[0]
        assert first_line.startswith("*")
        assert first_line.endswith("*")
        title = first_line.strip("*")
        assert title == title.upper()

    def test_product_name_full(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert sample_product.title in msg.whatsapp_text

    def test_pix_suffix(self, formatter, sample_product):
        sample_product.pix_price = 249.90
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "no pix" in msg.whatsapp_text

    def test_no_pix_no_suffix(self, formatter, sample_product):
        sample_product.pix_price = None
        sample_product.installments_without_interest = False
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "no pix" not in msg.whatsapp_text

    def test_format_price_br_style(self, formatter):
        assert formatter._format_price(1299.90) == "1.299,90"
        assert formatter._format_price(99.0) == "99,00"

    def test_cta_present(self, formatter, sample_product):
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert "🛒 Comprar agora!" in msg.whatsapp_text

    def test_product_without_original_price(self, formatter, sample_product):
        sample_product.original_price = None
        sample_product.discount_pct = 0.0
        msg = formatter.format(sample_product, short_link="https://s.black/abc")
        assert msg.whatsapp_text


# ---------------------------------------------------------------------------
# AffiliateLinkBuilder
# ---------------------------------------------------------------------------


class TestAffiliateLinkBuilder:
    def setup_method(self):
        with patch("src.distributor.affiliate_links.settings") as mock_cfg:
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
