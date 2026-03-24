"""
Testes para os scrapers do Crivo.
Usa mocks para evitar chamadas reais ao Mercado Livre.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.scraper.base_scraper import BaseScraper, ScrapedProduct, USER_AGENTS
from bs4 import Tag
from src.scraper.ml_scraper import MLScraper, ScrapeSource


# ---------------------------------------------------------------------------
# Fixtures — HTML mockado com seletores poly- (estrutura atual do ML)
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_product() -> ScrapedProduct:
    return ScrapedProduct(
        ml_id="MLB123456789",
        url="https://www.mercadolivre.com.br/p/MLB123456789",
        title="Tênis Nike Air Max 270 Masculino",
        price=299.90,
        original_price=599.90,
        rating=4.7,
        review_count=1234,
        category="Calçados",
        free_shipping=True,
        source="ofertas_do_dia",
    )


@pytest.fixture
def html_poly_cards() -> str:
    """HTML mockado com componentes poly- (estrutura atual do ML)."""
    return """
    <html><body>
    <div class="items-container">

      <!-- Produto 1: card completo com todos os campos -->
      <div class="poly-card">
        <div class="poly-card__portada">
          <img data-src="https://http2.mlstatic.com/tenis-nike.webp"
               src="data:image/gif;base64,placeholder" />
        </div>
        <a class="poly-component__title"
           href="https://www.mercadolivre.com.br/tenis-nike-air-max/p/MLB111222333">
          Tênis Nike Air Max 270 Masculino
        </a>
        <div class="poly-price__current">
          <span class="andes-money-amount">
            <span class="andes-money-amount__currency-symbol">R$</span>
            <span class="andes-money-amount__fraction">299</span>
            <span class="andes-money-amount__cents">,90</span>
          </span>
        </div>
        <s class="poly-price__original">
          <span class="andes-money-amount">
            <span class="andes-money-amount__fraction">599</span>
            <span class="andes-money-amount__cents">,90</span>
          </span>
        </s>
        <span class="poly-discount">50% OFF</span>
        <div class="poly-component__shipping">Frete grátis</div>
        <span class="poly-component__highlight">Oferta do dia</span>
      </div>

      <!-- Produto 2: preço com milhar, sem centavos, sem shipping/badge -->
      <div class="poly-card">
        <a class="poly-component__title"
           href="/bolsa-feminina-couro/p/MLB444555666">
          Bolsa Feminina Couro Legítimo Premium
        </a>
        <div class="poly-price__current">
          <span class="andes-money-amount">
            <span class="andes-money-amount__fraction">1.299</span>
          </span>
        </div>
        <s class="poly-price__original">
          <span class="andes-money-amount">
            <span class="andes-money-amount__fraction">2.599</span>
          </span>
        </s>
        <span class="poly-discount">50% OFF</span>
        <div class="poly-card__portada">
          <img data-src="https://http2.mlstatic.com/bolsa.webp" />
        </div>
      </div>

      <!-- Produto 3: badge "Mais vendido", URL com traço no ML ID -->
      <div class="poly-card">
        <a class="poly-component__title"
           href="https://produto.mercadolivre.com.br/MLB-777888999-relogio-casio-_JM">
          Relógio Casio Digital Vintage
        </a>
        <div class="poly-price__current">
          <span class="andes-money-amount">
            <span class="andes-money-amount__fraction">189</span>
            <span class="andes-money-amount__cents">,99</span>
          </span>
        </div>
        <s class="poly-price__original">
          <span class="andes-money-amount">
            <span class="andes-money-amount__fraction">349</span>
          </span>
        </s>
        <div class="poly-component__shipping">Frete grátis</div>
        <span class="poly-component__highlight">Mais vendido</span>
      </div>

      <!-- Produto inválido: sem link — deve ser ignorado -->
      <div class="poly-card">
        <p class="poly-component__title">Produto Sem Link</p>
        <div class="poly-price__current">
          <span class="andes-money-amount">
            <span class="andes-money-amount__fraction">99</span>
          </span>
        </div>
      </div>

      <!-- Produto inválido: sem título — deve ser ignorado -->
      <div class="poly-card">
        <a class="poly-component__title" href="/p/MLB000111222"></a>
        <div class="poly-price__current">
          <span class="andes-money-amount">
            <span class="andes-money-amount__fraction">50</span>
          </span>
        </div>
      </div>

    </div>
    </body></html>
    """


@pytest.fixture
def html_legacy_selectors() -> str:
    """HTML mockado com seletores legados (promotion-item)."""
    return """
    <html><body>
    <ul>
      <li class="promotion-item">
        <a href="https://www.mercadolivre.com.br/tenis/p/MLB999000111">
          <p class="promotion-item__title">Tênis Adidas Superstar Branco</p>
        </a>
        <span class="andes-money-amount__fraction">249</span>
        <del>
          <span class="andes-money-amount__fraction">499</span>
        </del>
        <span class="andes-money-amount__discount">50% OFF</span>
        <p class="promotion-item__free-shipping">Frete grátis</p>
      </li>
    </ul>
    </body></html>
    """


@pytest.fixture
def html_no_products() -> str:
    """HTML de página sem produtos."""
    return """
    <html><body>
    <div class="empty-state">
      <p>Nenhuma oferta disponível no momento.</p>
    </div>
    </body></html>
    """


@pytest.fixture
def html_with_pagination() -> str:
    """HTML com botão de paginação."""
    return """
    <html><body>
    <div class="poly-card">
      <a class="poly-component__title"
         href="/produto-teste/p/MLB222333444">
        Produto Teste Paginação
      </a>
      <div class="poly-price__current">
        <span class="andes-money-amount">
          <span class="andes-money-amount__fraction">99</span>
        </span>
      </div>
    </div>
    <nav>
      <a class="andes-pagination__link andes-pagination__link--next"
         href="/ofertas?page=2">Seguinte</a>
    </nav>
    </body></html>
    """


_DEFAULT_SOURCE = ScrapeSource(
    name="ofertas_do_dia",
    url="https://www.mercadolivre.com.br/ofertas",
    max_pages=2,
)


def _make_scraper() -> MLScraper:
    """Cria scraper sem chamar __init__ (para testes de parsing puro)."""
    scraper = MLScraper.__new__(MLScraper)
    scraper._storage = None
    scraper._request_count = 0
    scraper._last_user_agent = ""
    return scraper


# ===========================================================================
# ScrapedProduct — testes unitários do dataclass
# ===========================================================================


class TestScrapedProduct:
    def test_discount_pct_calculated(self):
        p = ScrapedProduct(
            ml_id="MLB1",
            url="http://ml.com",
            title="Test",
            price=100.0,
            original_price=200.0,
        )
        assert p.discount_pct == pytest.approx(50.0)

    def test_discount_pct_zero_when_no_original(self):
        p = ScrapedProduct(
            ml_id="MLB1",
            url="http://ml.com",
            title="Test",
            price=100.0,
            original_price=None,
        )
        assert p.discount_pct == pytest.approx(0.0)

    def test_discount_pct_zero_when_original_lower(self):
        p = ScrapedProduct(
            ml_id="MLB1",
            url="http://ml.com",
            title="Test",
            price=200.0,
            original_price=100.0,
        )
        assert p.discount_pct == pytest.approx(0.0)

    def test_to_dict_has_all_fields(self, sample_product):
        d = sample_product.to_dict()
        assert "ml_id" in d
        assert "price" in d
        assert "discount_pct" in d
        assert d["ml_id"] == "MLB123456789"
        assert d["free_shipping"] is True
        assert d["source"] == "ofertas_do_dia"

    def test_discount_pct_precision(self):
        p = ScrapedProduct(
            ml_id="MLB1",
            url="http://ml.com",
            title="Test",
            price=299.90,
            original_price=599.90,
        )
        assert p.discount_pct == pytest.approx(50.0)


# ===========================================================================
# OfertasDoDiaScraper — extração de ML ID
# ===========================================================================


class TestExtractMlId:
    def setup_method(self):
        self.scraper = _make_scraper()

    def test_standard_url(self):
        url = "https://www.mercadolivre.com.br/tenis/p/MLB111222333?param=1"
        assert self.scraper._extract_ml_id(url) == "MLB111222333"

    def test_url_with_dash(self):
        url = "https://produto.mercadolivre.com.br/MLB-777888999-relogio-_JM"
        assert self.scraper._extract_ml_id(url) == "MLB777888999"

    def test_relative_url(self):
        url = "/produto-teste/p/MLB555666777"
        assert self.scraper._extract_ml_id(url) == "MLB555666777"

    def test_no_ml_id_returns_none(self):
        assert self.scraper._extract_ml_id("https://google.com") is None

    def test_empty_url(self):
        assert self.scraper._extract_ml_id("") is None

    def test_case_insensitive(self):
        url = "https://ml.com/p/mlb999888777"
        assert self.scraper._extract_ml_id(url) == "mlb999888777"


# ===========================================================================
# OfertasDoDiaScraper — clean_price
# ===========================================================================


class TestCleanPrice:
    def setup_method(self):
        self.scraper = _make_scraper()

    def test_integer(self):
        assert self.scraper._clean_price("299") == pytest.approx(299.0)

    def test_with_thousands_separator(self):
        assert self.scraper._clean_price("1.299") == pytest.approx(1299.0)

    def test_with_decimal(self):
        assert self.scraper._clean_price("99,90") == pytest.approx(99.90)

    def test_full_br_format(self):
        assert self.scraper._clean_price("1.299,90") == pytest.approx(1299.90)

    def test_large_number(self):
        assert self.scraper._clean_price("12.999,99") == pytest.approx(12999.99)

    def test_none_on_invalid(self):
        assert self.scraper._clean_price("abc") is None

    def test_none_on_empty(self):
        assert self.scraper._clean_price("") is None

    def test_strips_currency(self):
        assert self.scraper._clean_price("R$ 299,90") == pytest.approx(299.90)


# ===========================================================================
# OfertasDoDiaScraper — parse_discount_pct
# ===========================================================================


class TestParseDiscountPct:
    def setup_method(self):
        self.scraper = _make_scraper()

    def test_standard_format(self):
        assert self.scraper._parse_discount_pct("50% OFF") == pytest.approx(50.0)

    def test_just_percent(self):
        assert self.scraper._parse_discount_pct("30%") == pytest.approx(30.0)

    def test_with_spaces(self):
        assert self.scraper._parse_discount_pct("  25 % de desconto") == pytest.approx(25.0)

    def test_no_discount(self):
        assert self.scraper._parse_discount_pct("sem desconto") == pytest.approx(0.0)

    def test_empty(self):
        assert self.scraper._parse_discount_pct("") == pytest.approx(0.0)


# ===========================================================================
# OfertasDoDiaScraper — price_from_andes
# ===========================================================================


class TestPriceFromAndes:
    def setup_method(self):
        self.scraper = _make_scraper()

    def _make_container(self, fraction: str, cents: str | None = None) -> "Tag":
        from bs4 import BeautifulSoup

        cents_html = ""
        if cents is not None:
            cents_html = f'<span class="andes-money-amount__cents">{cents}</span>'
        html = f"""
        <span class="andes-money-amount">
          <span class="andes-money-amount__fraction">{fraction}</span>
          {cents_html}
        </span>
        """
        soup = BeautifulSoup(html, "lxml")
        return soup.select_one(".andes-money-amount")

    def test_fraction_only(self):
        container = self._make_container("299")
        assert self.scraper._price_from_andes(container) == pytest.approx(299.0)

    def test_fraction_with_cents(self):
        container = self._make_container("299", ",90")
        assert self.scraper._price_from_andes(container) == pytest.approx(299.90)

    def test_fraction_with_thousands(self):
        container = self._make_container("1.299")
        assert self.scraper._price_from_andes(container) == pytest.approx(1299.0)

    def test_fraction_with_thousands_and_cents(self):
        container = self._make_container("1.299", ",90")
        assert self.scraper._price_from_andes(container) == pytest.approx(1299.90)

    def test_cents_without_comma(self):
        container = self._make_container("99", "50")
        assert self.scraper._price_from_andes(container) == pytest.approx(99.50)

    def test_no_fraction_returns_none(self):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup('<span class="andes-money-amount"></span>', "lxml")
        container = soup.select_one(".andes-money-amount")
        assert self.scraper._price_from_andes(container) is None


# ===========================================================================
# OfertasDoDiaScraper — parsing de página completa (poly- selectors)
# ===========================================================================


class TestParsePagePoly:
    def setup_method(self):
        self.scraper = _make_scraper()

    def test_parse_extracts_valid_products(self, html_poly_cards):
        products = self.scraper._parse_page(html_poly_cards, _DEFAULT_SOURCE)
        # 3 produtos válidos (2 inválidos são ignorados)
        assert len(products) == 3

    def test_first_product_fields(self, html_poly_cards):
        products = self.scraper._parse_page(html_poly_cards, _DEFAULT_SOURCE)
        p = products[0]
        assert p.ml_id == "MLB111222333"
        assert p.title == "Tênis Nike Air Max 270 Masculino"
        assert p.price == pytest.approx(299.90)
        assert p.original_price == pytest.approx(599.90)
        assert p.discount_pct == pytest.approx(50.0)
        assert p.free_shipping is True
        assert p.image_url == "https://http2.mlstatic.com/tenis-nike.webp"
        assert p.source == "ofertas_do_dia"

    def test_second_product_thousands(self, html_poly_cards):
        products = self.scraper._parse_page(html_poly_cards, _DEFAULT_SOURCE)
        p = products[1]
        assert p.ml_id == "MLB444555666"
        assert p.price == pytest.approx(1299.0)
        assert p.original_price == pytest.approx(2599.0)
        assert p.free_shipping is False
        assert p.url.startswith("https://")  # URL relativa convertida

    def test_third_product_dash_in_id(self, html_poly_cards):
        products = self.scraper._parse_page(html_poly_cards, _DEFAULT_SOURCE)
        p = products[2]
        # MLB-777888999 normalizado para MLB777888999
        assert p.ml_id == "MLB777888999"
        assert p.price == pytest.approx(189.99)
        assert p.original_price == pytest.approx(349.0)
        assert p.free_shipping is True

    def test_invalid_items_excluded(self, html_poly_cards):
        products = self.scraper._parse_page(html_poly_cards, _DEFAULT_SOURCE)
        ml_ids = [p.ml_id for p in products]
        # Produto sem link e produto sem título não devem aparecer
        assert "MLB000111222" not in ml_ids
        assert len(products) == 3

    def test_empty_page(self, html_no_products):
        products = self.scraper._parse_page(html_no_products, _DEFAULT_SOURCE)
        assert products == []


# ===========================================================================
# OfertasDoDiaScraper — parsing com seletores legados (fallback)
# ===========================================================================


class TestParsePageLegacy:
    def setup_method(self):
        self.scraper = _make_scraper()

    def test_legacy_selectors_work(self, html_legacy_selectors):
        products = self.scraper._parse_page(html_legacy_selectors, _DEFAULT_SOURCE)
        assert len(products) == 1
        p = products[0]
        assert p.ml_id == "MLB999000111"
        assert p.title == "Tênis Adidas Superstar Branco"
        assert p.price == pytest.approx(249.0)
        assert p.original_price == pytest.approx(499.0)
        assert p.free_shipping is True


# ===========================================================================
# OfertasDoDiaScraper — deduplicação com mock de StorageManager
# ===========================================================================


class TestDeduplication:
    @pytest.mark.asyncio
    async def test_skips_known_products(self, html_poly_cards):
        mock_storage = AsyncMock()
        # MLB111222333 já existe, os outros não
        mock_storage.check_duplicates_batch = AsyncMock(return_value={"MLB111222333"})
        mock_storage.upsert_products_batch = AsyncMock(
            return_value={
                "MLB444555666": "uuid-2",
                "MLB777888999": "uuid-3",
            }
        )
        mock_storage.add_price_history_batch = AsyncMock(return_value=True)
        mock_storage.log_event = AsyncMock(return_value=True)

        scraper = MLScraper(storage=mock_storage)

        mock_page = AsyncMock()
        mock_page.content = AsyncMock(return_value=html_poly_cards)
        mock_page.wait_for_selector = AsyncMock()
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        mock_page.close = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value=0)

        with (
            patch.object(scraper, "_start_browser", new_callable=AsyncMock),
            patch.object(scraper, "_close_browser", new_callable=AsyncMock),
            patch.object(
                scraper,
                "_new_page",
                new_callable=AsyncMock,
                return_value=mock_page,
            ),
            patch.object(
                scraper,
                "_goto",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch.object(
                scraper,
                "_human_scroll",
                new_callable=AsyncMock,
            ),
            patch.object(
                scraper,
                "_random_delay",
                new_callable=AsyncMock,
            ),
            patch.object(
                scraper,
                "_is_blocked",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            products = await scraper.scrape()

        # MLB111222333 filtrado (dupe), restam 2
        assert len(products) == 2
        assert all(p.ml_id != "MLB111222333" for p in products)

        # check_duplicates_batch chamado 1 vez com todos os 3
        mock_storage.check_duplicates_batch.assert_called_once()
        # upsert_products_batch chamado 1 vez com os 2 novos
        mock_storage.upsert_products_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_storage_returns_all(self, html_poly_cards):
        """Sem storage, retorna todos os produtos sem dedup."""
        scraper = MLScraper()

        mock_page = AsyncMock()
        mock_page.content = AsyncMock(return_value=html_poly_cards)
        mock_page.wait_for_selector = AsyncMock()
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        mock_page.close = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value=0)

        with (
            patch.object(scraper, "_start_browser", new_callable=AsyncMock),
            patch.object(scraper, "_close_browser", new_callable=AsyncMock),
            patch.object(
                scraper, "_new_page", new_callable=AsyncMock, return_value=mock_page
            ),
            patch.object(scraper, "_goto", new_callable=AsyncMock, return_value=True),
            patch.object(scraper, "_human_scroll", new_callable=AsyncMock),
            patch.object(scraper, "_random_delay", new_callable=AsyncMock),
            patch.object(
                scraper, "_is_blocked", new_callable=AsyncMock, return_value=False
            ),
        ):
            products = await scraper.scrape()

        assert len(products) == 3


# ===========================================================================
# OfertasDoDiaScraper — retry e error handling
# ===========================================================================


class TestRetryAndErrors:
    @pytest.mark.asyncio
    async def test_captcha_stops_scraping(self):
        """CAPTCHA detectado deve parar o scraping e logar o erro."""
        mock_storage = AsyncMock()
        mock_storage.log_event = AsyncMock(return_value=True)

        scraper = MLScraper(storage=mock_storage)

        mock_page = AsyncMock()
        mock_page.close = AsyncMock()

        with (
            patch.object(scraper, "_start_browser", new_callable=AsyncMock),
            patch.object(scraper, "_close_browser", new_callable=AsyncMock),
            patch.object(
                scraper, "_new_page", new_callable=AsyncMock, return_value=mock_page
            ),
            patch.object(scraper, "_goto", new_callable=AsyncMock, return_value=True),
            patch.object(
                scraper, "_is_blocked", new_callable=AsyncMock, return_value=True
            ),
        ):
            products = await scraper.scrape()

        assert products == []
        # Deve logar o evento de erro
        mock_storage.log_event.assert_any_call(
            "scrape_error",
            {"reason": "captcha", "source": "ofertas_do_dia"},
        )

    @pytest.mark.asyncio
    async def test_goto_failure_stops_page(self):
        """Falha no _goto deve parar a iteração sem crashar."""
        scraper = MLScraper()

        mock_page = AsyncMock()
        mock_page.close = AsyncMock()

        with (
            patch.object(scraper, "_start_browser", new_callable=AsyncMock),
            patch.object(scraper, "_close_browser", new_callable=AsyncMock),
            patch.object(
                scraper, "_new_page", new_callable=AsyncMock, return_value=mock_page
            ),
            patch.object(scraper, "_goto", new_callable=AsyncMock, return_value=False),
            patch.object(
                scraper, "_is_blocked", new_callable=AsyncMock, return_value=False
            ),
        ):
            products = await scraper.scrape()

        assert products == []

    @pytest.mark.asyncio
    async def test_no_cards_stops_page(self):
        """Timeout ao esperar cards deve parar a iteração."""
        scraper = MLScraper()

        mock_page = AsyncMock()
        mock_page.wait_for_selector = AsyncMock(
            side_effect=Exception("Timeout waiting for selector")
        )
        mock_page.close = AsyncMock()

        with (
            patch.object(scraper, "_start_browser", new_callable=AsyncMock),
            patch.object(scraper, "_close_browser", new_callable=AsyncMock),
            patch.object(
                scraper, "_new_page", new_callable=AsyncMock, return_value=mock_page
            ),
            patch.object(scraper, "_goto", new_callable=AsyncMock, return_value=True),
            patch.object(
                scraper, "_is_blocked", new_callable=AsyncMock, return_value=False
            ),
        ):
            products = await scraper.scrape()

        assert products == []


# ===========================================================================
# OfertasDoDiaScraper — integração completa (scrape com mocks)
# ===========================================================================


class TestScrapeIntegration:
    @pytest.mark.asyncio
    async def test_full_scrape_single_page(self, html_poly_cards):
        """Testa o fluxo completo: navegar → parsear → retornar produtos."""
        scraper = MLScraper()

        mock_page = AsyncMock()
        mock_page.content = AsyncMock(return_value=html_poly_cards)
        mock_page.wait_for_selector = AsyncMock()
        mock_page.query_selector = AsyncMock(return_value=None)  # sem next
        mock_page.query_selector_all = AsyncMock(return_value=[])
        mock_page.close = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value=0)

        with (
            patch.object(scraper, "_start_browser", new_callable=AsyncMock),
            patch.object(scraper, "_close_browser", new_callable=AsyncMock),
            patch.object(
                scraper, "_new_page", new_callable=AsyncMock, return_value=mock_page
            ),
            patch.object(scraper, "_goto", new_callable=AsyncMock, return_value=True),
            patch.object(scraper, "_human_scroll", new_callable=AsyncMock),
            patch.object(scraper, "_random_delay", new_callable=AsyncMock),
            patch.object(
                scraper, "_is_blocked", new_callable=AsyncMock, return_value=False
            ),
        ):
            products = await scraper.scrape()

        assert len(products) == 3
        assert products[0].ml_id == "MLB111222333"
        assert products[0].price == pytest.approx(299.90)
        assert products[1].ml_id == "MLB444555666"
        assert products[1].price == pytest.approx(1299.0)
        assert products[2].ml_id == "MLB777888999"

    @pytest.mark.asyncio
    async def test_multi_page_scrape(self, html_poly_cards, html_no_products):
        """Testa navegação multi-página: para quando não há mais produtos."""
        scraper = MLScraper()

        mock_page = AsyncMock()
        # Página 1: tem produtos; Página 2: vazia (para)
        mock_page.content = AsyncMock(side_effect=[html_poly_cards, html_no_products])
        mock_page.wait_for_selector = AsyncMock()
        mock_page.close = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value=0)

        # Simula "next page" link existindo na primeira chamada
        mock_next_el = AsyncMock()
        mock_next_el.get_attribute = AsyncMock(return_value="/ofertas?page=2")

        call_count = 0

        async def mock_query_selector(selector):
            nonlocal call_count
            # Na primeira vez (page 1), retorna link; depois None
            if "next" in selector and call_count == 0:
                call_count += 1
                return mock_next_el
            return None

        mock_page.query_selector = mock_query_selector
        mock_page.query_selector_all = AsyncMock(return_value=[])

        with (
            patch.object(scraper, "_start_browser", new_callable=AsyncMock),
            patch.object(scraper, "_close_browser", new_callable=AsyncMock),
            patch.object(
                scraper, "_new_page", new_callable=AsyncMock, return_value=mock_page
            ),
            patch.object(scraper, "_goto", new_callable=AsyncMock, return_value=True),
            patch.object(scraper, "_human_scroll", new_callable=AsyncMock),
            patch.object(scraper, "_random_delay", new_callable=AsyncMock),
            patch.object(
                scraper, "_is_blocked", new_callable=AsyncMock, return_value=False
            ),
        ):
            products = await scraper.scrape()

        # Apenas os 3 da página 1 (página 2 vazia)
        assert len(products) == 3

    @pytest.mark.asyncio
    async def test_persistence_saves_all_products(self, html_poly_cards):
        """Verifica que cada produto novo é salvo via storage."""
        mock_storage = AsyncMock()
        mock_storage.check_duplicates_batch = AsyncMock(return_value=set())
        mock_storage.upsert_products_batch = AsyncMock(
            return_value={
                "MLB111222333": "uuid-1",
                "MLB444555666": "uuid-2",
                "MLB777888999": "uuid-3",
            }
        )
        mock_storage.add_price_history_batch = AsyncMock(return_value=True)
        mock_storage.log_event = AsyncMock(return_value=True)

        scraper = MLScraper(storage=mock_storage)

        mock_page = AsyncMock()
        mock_page.content = AsyncMock(return_value=html_poly_cards)
        mock_page.wait_for_selector = AsyncMock()
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        mock_page.close = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value=0)

        with (
            patch.object(scraper, "_start_browser", new_callable=AsyncMock),
            patch.object(scraper, "_close_browser", new_callable=AsyncMock),
            patch.object(
                scraper,
                "_new_page",
                new_callable=AsyncMock,
                return_value=mock_page,
            ),
            patch.object(
                scraper,
                "_goto",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch.object(
                scraper,
                "_human_scroll",
                new_callable=AsyncMock,
            ),
            patch.object(
                scraper,
                "_random_delay",
                new_callable=AsyncMock,
            ),
            patch.object(
                scraper,
                "_is_blocked",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            products = await scraper.scrape()

        assert len(products) == 3
        mock_storage.upsert_products_batch.assert_called_once()
        mock_storage.add_price_history_batch.assert_called_once()
        # Log de sucesso ao final
        mock_storage.log_event.assert_any_call(
            "scrape_success",
            {
                "total": 3,
                "sources": 1,
                "dupes_skipped": 0,
                "elapsed_seconds": pytest.approx(0, abs=10),
            },
        )


# ===========================================================================
# Anti-bloqueio (BaseScraper)
# ===========================================================================


class TestAntiBlocking:
    def test_user_agent_rotation(self):
        """Verifica que o scraper gera UAs diferentes a cada chamada."""

        class ConcreteScraper(BaseScraper):
            async def scrape(self):
                return []

        scraper = ConcreteScraper.__new__(ConcreteScraper)
        scraper._last_user_agent = ""
        scraper.cfg = MagicMock()

        ua1 = scraper._pick_user_agent()
        assert ua1
        assert "Mozilla" in ua1

    def test_user_agents_fallback_list_exists(self):
        assert len(USER_AGENTS) >= 5, "Deve ter pelo menos 5 User-Agents de fallback"


# ===========================================================================
# OfertasDoDiaScraper — testes de preço (current vs original)
# ===========================================================================


class TestPriceExtraction:
    def setup_method(self):
        self.scraper = _make_scraper()

    def test_current_price_from_poly_container(self):
        from bs4 import BeautifulSoup

        html = """
        <div class="poly-card">
          <div class="poly-price__current">
            <span class="andes-money-amount__fraction">499</span>
            <span class="andes-money-amount__cents">,90</span>
          </div>
          <s class="poly-price__original">
            <span class="andes-money-amount__fraction">999</span>
          </s>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        card = soup.select_one(".poly-card")

        card_price, _pix = self.scraper._get_prices(card)
        assert card_price == pytest.approx(499.90)
        assert self.scraper._get_original_price(card) == pytest.approx(999.0)

    def test_prices_from_del_tag(self):
        from bs4 import BeautifulSoup

        html = """
        <li class="promotion-item">
          <span class="andes-money-amount__fraction">249</span>
          <del>
            <span class="andes-money-amount__fraction">499</span>
          </del>
        </li>
        """
        soup = BeautifulSoup(html, "lxml")
        card = soup.select_one(".promotion-item")

        card_price, _pix = self.scraper._get_prices(card)
        assert card_price == pytest.approx(249.0)
        assert self.scraper._get_original_price(card) == pytest.approx(499.0)

    def test_no_original_price(self):
        from bs4 import BeautifulSoup

        html = """
        <div class="poly-card">
          <div class="poly-price__current">
            <span class="andes-money-amount__fraction">99</span>
          </div>
        </div>
        """
        soup = BeautifulSoup(html, "lxml")
        card = soup.select_one(".poly-card")

        card_price, _pix = self.scraper._get_prices(card)
        assert card_price == pytest.approx(99.0)
        assert self.scraper._get_original_price(card) is None
