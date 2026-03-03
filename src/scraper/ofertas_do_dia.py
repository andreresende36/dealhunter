"""
DealHunter — Scraper 1: Ofertas do Dia
Coleta as ofertas em destaque da página principal de promoções do ML.
URL alvo: https://www.mercadolivre.com.br/ofertas

Seletores CSS validados via ml_selectors_report.md (componentes poly- e andes-).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from src.database.storage_manager import StorageManager

import structlog
from bs4 import BeautifulSoup, Tag
from playwright.async_api import Page

from .base_scraper import BaseScraper, CaptchaError, RateLimitError, ScrapedProduct

logger = structlog.get_logger(__name__)

OFERTAS_URL = "https://www.mercadolivre.com.br/ofertas"

# ---------------------------------------------------------------------------
# CSS Selectors — baseados no ml_selectors_report.md
# Primários: componentes poly- (estrutura atual do ML)
# Fallback: seletores legados (promotion-item, ui-search)
# ---------------------------------------------------------------------------

SELECTORS = {
    # Container do card de produto
    "card": "div.poly-card, li.promotion-item, li.ui-search-layout__item",
    # Título do produto (poly- tem href embutido no <a>)
    "title": (
        "a.poly-component__title, "
        "h2.poly-box.poly-component__title, "
        "p.promotion-item__title, "
        "h2.ui-search-item__title"
    ),
    # Link do produto
    "link": "a.poly-component__title, a[href*='mercadolivre'], a.ui-search-link",
    # Container de preço atual
    "price_current_container": ".poly-price__current",
    # Container de preço original (riscado)
    "price_original_container": "s.poly-price__original, .poly-price__comparison",
    # Elementos de preço dentro dos containers
    "fraction": ".andes-money-amount__fraction",
    "cents": ".andes-money-amount__cents",
    # Desconto
    "discount": (
        "span.poly-discount, "
        ".poly-price__percentage, "
        "span.andes-money-amount__discount, "
        "span[class*='discount']"
    ),
    # Frete grátis
    "shipping": (
        "div.poly-component__shipping, "
        "p.promotion-item__free-shipping, "
        "span[class*='free-shipping']"
    ),
    # Badges (Oferta do dia, Mais vendido, etc.)
    "badge": "span.poly-component__highlight",
    # Imagem / thumbnail
    "image": (
        "div.poly-card__portada img, " ".poly-component__picture img, " "img[data-src]"
    ),
    # Paginação
    "next_page": (
        "a.andes-pagination__link--next, " "li.andes-pagination__button--next a"
    ),
    "pagination_links": "a.andes-pagination__link",
}


# ---------------------------------------------------------------------------
# Scraper principal
# ---------------------------------------------------------------------------


class OfertasDoDiaScraper(BaseScraper):
    """
    Scraper para a página 'Ofertas do Dia' do Mercado Livre.

    Coleta:
    - Produtos em destaque com desconto
    - Preço original vs preço com desconto
    - Percentual de desconto
    - Badges (Oferta do dia, Mais vendido, Oferta relâmpago, Oferta imperdível)
    - Dados de frete grátis e imagem

    Uso:
        # Standalone (sem persistência):
        scraper = OfertasDoDiaScraper(max_pages=3)
        products = await scraper.scrape()

        # Com persistência e deduplicação:
        async with StorageManager() as storage:
            scraper = OfertasDoDiaScraper(max_pages=3, storage=storage)
            products = await scraper.scrape()
    """

    def __init__(self, max_pages: int = 5, storage: Optional["StorageManager"] = None):
        super().__init__()
        self.max_pages = max_pages
        self._storage = storage

    async def _new_page(self) -> Page:
        """Cria nova página com playwright-stealth aplicado (se disponível)."""
        page = await super()._new_page()
        try:
            from playwright_stealth import stealth_async  # type: ignore[import-untyped]

            await stealth_async(page)
        except ImportError:
            logger.debug("playwright_stealth_not_installed")
        return page

    # ------------------------------------------------------------------
    # Método principal
    # ------------------------------------------------------------------

    async def scrape(self) -> list[ScrapedProduct]:
        """
        Coleta ofertas do dia em até `max_pages` páginas.

        Retorna lista de ScrapedProduct novos (já filtrados por dedup se
        storage foi fornecido). Cada produto é salvo automaticamente no banco.
        """
        start = time.monotonic()
        products: list[ScrapedProduct] = []
        dupes_skipped = 0
        parse_errors = 0

        async with self:
            page = await self._new_page()
            url = OFERTAS_URL

            try:
                for page_num in range(1, self.max_pages + 1):
                    logger.info(
                        "scraping_page",
                        source="ofertas_do_dia",
                        page=page_num,
                        url=url,
                    )

                    success = await self._goto(page, url)
                    if not success:
                        parse_errors += 1
                        break

                    # Detecta bloqueio específico
                    if await self._is_blocked(page):
                        raise CaptchaError("CAPTCHA detectado na página de ofertas")

                    # Aguarda os cards de produto carregarem
                    try:
                        await page.wait_for_selector(
                            "div.poly-card, li.promotion-item",
                            timeout=15_000,
                        )
                    except Exception:
                        logger.warning("no_cards_found", page=page_num)
                        break

                    # Scroll para carregar lazy-loaded images
                    await self._human_scroll(page)

                    html = await page.content()
                    page_products = self._parse_page(html)

                    logger.info(
                        "page_parsed",
                        page=page_num,
                        raw_count=len(page_products),
                    )

                    # Deduplicação + persistência
                    for p in page_products:
                        if self._storage:
                            try:
                                is_dupe = await self._storage.check_duplicate(p.ml_id)
                                if is_dupe:
                                    dupes_skipped += 1
                                    continue
                                # Salva produto novo
                                product_id = await self._storage.upsert_product(p)
                                await self._storage.add_price_history(
                                    product_id,
                                    p.price,
                                    p.original_price,
                                )
                            except Exception as exc:
                                logger.warning(
                                    "storage_error",
                                    ml_id=p.ml_id,
                                    error=str(exc),
                                )
                        products.append(p)

                    # Sem produtos = fim das ofertas
                    if not page_products:
                        break

                    # Próxima página
                    next_url = await self._get_next_page_url(page)
                    if not next_url:
                        logger.info("no_more_pages", stopped_at=page_num)
                        break

                    url = next_url
                    await self._random_delay()

            except CaptchaError:
                logger.error("captcha_blocked", source="ofertas_do_dia")
                if self._storage:
                    await self._storage.log_event(
                        "scrape_error",
                        {"reason": "captcha", "source": "ofertas_do_dia"},
                    )

            except RateLimitError:
                logger.error("rate_limited", source="ofertas_do_dia")
                if self._storage:
                    await self._storage.log_event(
                        "scrape_error",
                        {"reason": "rate_limit", "source": "ofertas_do_dia"},
                    )

            finally:
                await page.close()

        elapsed = round(time.monotonic() - start, 1)
        logger.info(
            "scraping_done",
            source="ofertas_do_dia",
            total=len(products),
            dupes_skipped=dupes_skipped,
            parse_errors=parse_errors,
            elapsed_seconds=elapsed,
        )

        if self._storage:
            await self._storage.log_event(
                "scrape_success",
                {
                    "source": "ofertas_do_dia",
                    "total": len(products),
                    "dupes_skipped": dupes_skipped,
                    "elapsed_seconds": elapsed,
                },
            )

        return products

    # ------------------------------------------------------------------
    # Paginação
    # ------------------------------------------------------------------

    async def _get_next_page_url(self, page: Page) -> str | None:
        """Detecta e retorna a URL da próxima página de ofertas."""
        try:
            # Tenta seletores diretos de "próxima página"
            for selector in SELECTORS["next_page"].split(", "):
                el = await page.query_selector(selector)
                if el:
                    href = await el.get_attribute("href")
                    if href:
                        return self.full_url(href) if href.startswith("/") else href

            # Fallback: procura link com texto "Seguinte"
            links = await page.query_selector_all(SELECTORS["pagination_links"])
            for link in links:
                text = (await link.inner_text()).strip().lower()
                if text in ("seguinte", "siguiente", "next", "próxima"):
                    href = await link.get_attribute("href")
                    if href:
                        return self.full_url(href) if href.startswith("/") else href

        except Exception as exc:
            logger.debug("pagination_check_error", error=str(exc))

        return None

    # ------------------------------------------------------------------
    # Parsing de HTML com BeautifulSoup
    # ------------------------------------------------------------------

    def _parse_page(self, html: str) -> list[ScrapedProduct]:
        """Extrai todos os produtos do HTML da página de ofertas."""
        soup = BeautifulSoup(html, "lxml")
        products: list[ScrapedProduct] = []

        items = soup.select(SELECTORS["card"])
        for item in items:
            product = self._parse_item(item)
            if product:
                products.append(product)

        return products

    def _parse_item(self, item: Tag) -> Optional[ScrapedProduct]:
        """Extrai dados de um único card de produto."""
        try:
            # --- URL e ML ID ---
            link_tag = item.select_one(SELECTORS["link"])
            if not link_tag:
                return None
            url = str(link_tag.get("href", ""))
            if not url:
                return None

            ml_id = self._extract_ml_id(url)
            if not ml_id:
                return None

            # URL absoluta
            if url.startswith("/"):
                url = self.full_url(url)

            # --- Título ---
            title_tag = item.select_one(SELECTORS["title"])
            title = title_tag.get_text(strip=True) if title_tag else ""
            if not title:
                return None

            # --- Preço atual ---
            price = self._get_current_price(item)
            if price is None or price <= 0:
                return None

            # --- Preço original (riscado) ---
            original_price = self._get_original_price(item)

            # --- Desconto explícito (texto na página) ---
            discount_tag = item.select_one(SELECTORS["discount"])
            discount_text = discount_tag.get_text(strip=True) if discount_tag else ""
            explicit_discount = self._parse_discount_pct(discount_text)

            # --- Imagem ---
            img_tag = item.select_one(SELECTORS["image"])
            image_url = ""
            if img_tag:
                image_url = str(img_tag.get("data-src") or img_tag.get("src") or "")

            # --- Frete grátis ---
            shipping_tag = item.select_one(SELECTORS["shipping"])
            free_shipping = False
            if shipping_tag:
                text = shipping_tag.get_text(strip=True).lower()
                free_shipping = "grátis" in text or "gratis" in text

            # --- Badges ---
            badge_tag = item.select_one(SELECTORS["badge"])
            if badge_tag:
                pass  # currently unused, but can be recorded if needed in the future

            # Monta o produto
            product = ScrapedProduct(
                ml_id=ml_id,
                url=url,
                title=title,
                price=price,
                original_price=original_price,
                image_url=image_url,
                free_shipping=free_shipping,
                source="ofertas_do_dia",
            )

            # Usa desconto explícito se preço original ausente
            if explicit_discount and not original_price:
                product.discount_pct = explicit_discount

            return product

        except Exception as exc:
            logger.debug("parse_item_error", error=str(exc))
            return None

    # ------------------------------------------------------------------
    # Extração de preço (com suporte a fraction + cents separados)
    # ------------------------------------------------------------------

    def _get_current_price(self, card: Tag) -> float | None:
        """Extrai o preço atual (com desconto) de um card de produto."""
        # Estratégia 1: container .poly-price__current
        container = card.select_one(SELECTORS["price_current_container"])
        if container:
            price = self._price_from_andes(container)
            if price:
                return price

        # Estratégia 2: primeiro andes-money-amount que NÃO esteja
        # dentro de <s> ou <del> (preço riscado)
        for fraction in card.select(SELECTORS["fraction"]):
            if not fraction.find_parent(["s", "del"]):
                return self._clean_price(fraction.get_text(strip=True))

        return None

    def _get_original_price(self, card: Tag) -> float | None:
        """Extrai o preço original (riscado / antes do desconto)."""
        # Estratégia 1: container s.poly-price__original
        for selector in SELECTORS["price_original_container"].split(", "):
            container = card.select_one(selector)
            if container:
                price = self._price_from_andes(container)
                if price:
                    return price

        # Estratégia 2: fraction dentro de <s> ou <del>
        for tag_name in ("s", "del"):
            parent = card.select_one(tag_name)
            if parent:
                fraction = parent.select_one(SELECTORS["fraction"])
                if fraction:
                    return self._clean_price(fraction.get_text(strip=True))

        return None

    def _price_from_andes(self, container: Tag) -> float | None:
        """
        Extrai preço de um container andes-money-amount.
        Combina fraction (parte inteira) com cents (centavos) se presente.

        Exemplos:
            fraction="1.299", cents=",90" → 1299.90
            fraction="299", cents=None → 299.0
        """
        fraction_el = container.select_one(SELECTORS["fraction"])
        if not fraction_el:
            return None

        fraction_text = fraction_el.get_text(strip=True)
        # Remove separador de milhar (ponto no formato brasileiro)
        fraction_clean = fraction_text.replace(".", "")

        try:
            base = int(fraction_clean)
        except ValueError:
            return self._clean_price(fraction_text)

        cents_el = container.select_one(SELECTORS["cents"])
        if cents_el:
            cents_text = cents_el.get_text(strip=True).lstrip(",").strip()
            try:
                return float(base) + int(cents_text) / 100
            except ValueError:
                pass

        return float(base)

    # ------------------------------------------------------------------
    # Helpers de parsing
    # ------------------------------------------------------------------
