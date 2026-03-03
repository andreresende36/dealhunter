"""
DealHunter — Scraper Unificado do Mercado Livre
Coleta ofertas de múltiplas fontes (Ofertas do Dia, Categorias) com
extração padronizada de todos os campos.

Uso:
    # Fontes padrão (ofertas + categorias configuradas):
    scraper = MLScraper()
    products = await scraper.scrape()

    # Fontes customizadas:
    sources = [ScrapeSource(name="ofertas", url="https://...")]
    scraper = MLScraper(sources=sources)
    products = await scraper.scrape()
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlencode

if TYPE_CHECKING:
    from src.database.storage_manager import StorageManager

import structlog
from bs4 import BeautifulSoup, Tag
from playwright.async_api import Page

from src.config import settings
from .base_scraper import BaseScraper, CaptchaError, RateLimitError, ScrapedProduct

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Configuração de fonte de scraping
# ---------------------------------------------------------------------------

# Subcategorias de Moda com seus IDs no ML
SUBCATEGORIES = {
    "MLB1574": "Calçados",
    "MLB1577": "Roupas Masculinas",
    "MLB1578": "Roupas Femininas",
    "MLB1579": "Acessórios de Moda",
    "MLB1580": "Bolsas e Mochilas",
    "MLB1581": "Óculos e Lunetas",
    "MLB1582": "Relógios",
    "MLB1583": "Joias e Bijuterias",
}

OFERTAS_URL = "https://www.mercadolivre.com.br/ofertas"


@dataclass
class ScrapeSource:
    """Configuração de uma fonte de scraping."""

    name: str  # Ex: "ofertas_do_dia", "Calçados"
    url: str  # URL base da fonte
    max_pages: int = 3
    pagination: str = "link"  # "link" | "offset"
    category: str = ""  # Categoria para o ScrapedProduct
    search_params: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Seletores CSS unificados (poly- + ui-search- + fallbacks)
# ---------------------------------------------------------------------------

SELECTORS = {
    # Container do card de produto
    "card": (
        "div.poly-card, "
        "li.promotion-item, "
        "li.ui-search-layout__item, "
        "div.ui-search-result__wrapper"
    ),
    # Título do produto
    "title": (
        "a.poly-component__title, "
        "h2.poly-box.poly-component__title, "
        "p.promotion-item__title, "
        "h2.ui-search-item__title, "
        "span.ui-search-item__title"
    ),
    # Link do produto
    "link": (
        "a.poly-component__title, "
        "a.ui-search-link, "
        "a[href*='mercadolivre']"
    ),
    # Preço atual — container andes (fraction + cents)
    "price_current_container": ".poly-price__current",
    "fraction": ".andes-money-amount__fraction",
    "cents": ".andes-money-amount__cents",
    # Preço original (riscado) — poly- e ui-search-
    "price_original_container": (
        "s.poly-price__original, "
        ".poly-price__comparison"
    ),
    "price_original_search": (
        "del.ui-search-price__original-value "
        "span.andes-money-amount__fraction, "
        "s span.andes-money-amount__fraction, "
        "span.ui-search-price__original-value "
        "span.price-tag-fraction"
    ),
    # Desconto explícito
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
        "span.ui-search-item__shipping.ui-search-item__shipping--free, "
        "span[class*='free-shipping']"
    ),
    # Imagem / thumbnail
    "image": (
        "div.poly-card__portada img, "
        ".poly-component__picture img, "
        "img.ui-search-result-image__element, "
        "img[data-src]"
    ),
    # Avaliação e reviews
    "rating": "span.ui-search-reviews__rating-number",
    "review_count": "span.ui-search-reviews__amount",
    # Vendedor
    "seller_name": "span.ui-search-item__seller-name",
    "official_store": (
        "span.ui-search-official-store-label, "
        "span[class*='official-store']"
    ),
    # Badges
    "badge": "span.poly-component__highlight",
    # Paginação (link-based)
    "next_page": (
        "a.andes-pagination__link--next, "
        "li.andes-pagination__button--next a"
    ),
    "pagination_links": "a.andes-pagination__link",
}


# ---------------------------------------------------------------------------
# Scraper Unificado
# ---------------------------------------------------------------------------


class MLScraper(BaseScraper):
    """
    Scraper unificado do Mercado Livre.

    Coleta ofertas de múltiplas fontes (Ofertas do Dia, Categorias)
    com extração padronizada de todos os campos disponíveis nos cards.

    Campos extraídos de cada card:
    - ml_id, url, title, price, original_price, discount_pct
    - rating, review_count, seller, is_official_store
    - free_shipping, image_url, category
    """

    # Items por página nas buscas por categoria do ML
    ITEMS_PER_PAGE = 48

    def __init__(
        self,
        sources: Optional[list[ScrapeSource]] = None,
        storage: Optional["StorageManager"] = None,
    ):
        super().__init__()
        self._storage = storage
        self.sources = sources or self._default_sources()

    def _default_sources(self) -> list[ScrapeSource]:
        """Gera fontes padrão: Ofertas do Dia + categorias configuradas."""
        sources: list[ScrapeSource] = []

        # Ofertas do Dia
        sources.append(
            ScrapeSource(
                name="ofertas_do_dia",
                url=OFERTAS_URL,
                max_pages=2,
                pagination="link",
            )
        )

        # Categorias (subcategorias de Moda)
        for category_id in settings.mercado_livre.category_ids:
            category_name = SUBCATEGORIES.get(category_id, category_id)
            base_url = f"{self.ML_BASE_URL}/c/{category_id.upper()}"
            sources.append(
                ScrapeSource(
                    name=category_name,
                    url=base_url,
                    max_pages=1,
                    pagination="offset",
                    category=category_name,
                    search_params={
                        "sort": "relevance",
                        "discount": "10-100",
                    },
                )
            )

        return sources

    async def _new_page(self) -> Page:
        """Cria nova página com playwright-stealth aplicado."""
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
        Coleta ofertas de todas as fontes configuradas.

        Retorna lista de ScrapedProduct com todos os campos extraídos.
        Se storage foi fornecido, faz dedup e persistência inline.
        """
        start = time.monotonic()
        all_products: list[ScrapedProduct] = []
        total_dupes = 0
        total_errors = 0

        async with self:
            page = await self._new_page()

            try:
                for source in self.sources:
                    logger.info(
                        "scraping_source",
                        source=source.name,
                        pagination=source.pagination,
                        max_pages=source.max_pages,
                    )

                    try:
                        products, dupes, errors = await self._scrape_source(
                            page, source
                        )
                        all_products.extend(products)
                        total_dupes += dupes
                        total_errors += errors

                        logger.info(
                            "source_done",
                            source=source.name,
                            count=len(products),
                        )
                    except CaptchaError:
                        logger.error(
                            "captcha_blocked", source=source.name
                        )
                        if self._storage:
                            await self._storage.log_event(
                                "scrape_error",
                                {
                                    "reason": "captcha",
                                    "source": source.name,
                                },
                            )
                        total_errors += 1
                    except RateLimitError:
                        logger.error(
                            "rate_limited", source=source.name
                        )
                        if self._storage:
                            await self._storage.log_event(
                                "scrape_error",
                                {
                                    "reason": "rate_limit",
                                    "source": source.name,
                                },
                            )
                        total_errors += 1
                    except Exception as exc:
                        logger.error(
                            "source_failed",
                            source=source.name,
                            error=str(exc),
                        )
                        total_errors += 1

                    # Delay entre fontes
                    await self._random_delay(extra_min=1.0, extra_max=2.0)

            finally:
                await page.close()

        elapsed = round(time.monotonic() - start, 1)
        logger.info(
            "scraping_done",
            total=len(all_products),
            dupes_skipped=total_dupes,
            errors=total_errors,
            elapsed_seconds=elapsed,
        )

        if self._storage:
            await self._storage.log_event(
                "scrape_success",
                {
                    "total": len(all_products),
                    "sources": len(self.sources),
                    "dupes_skipped": total_dupes,
                    "elapsed_seconds": elapsed,
                },
            )

        return all_products

    # ------------------------------------------------------------------
    # Scraping por fonte
    # ------------------------------------------------------------------

    async def _scrape_source(
        self, page: Page, source: ScrapeSource
    ) -> tuple[list[ScrapedProduct], int, int]:
        """
        Coleta produtos de uma única fonte em até max_pages páginas.

        Retorna (products, dupes_skipped, parse_errors).
        """
        products: list[ScrapedProduct] = []
        dupes_skipped = 0
        parse_errors = 0
        url = self._build_url(source, page_num=1)

        for page_num in range(1, source.max_pages + 1):
            logger.info(
                "scraping_page",
                source=source.name,
                page=page_num,
                url=url,
            )

            success = await self._goto(page, url)
            if not success:
                parse_errors += 1
                break

            if await self._is_blocked(page):
                raise CaptchaError(
                    f"CAPTCHA detectado em {source.name}"
                )

            # Aguarda os cards carregarem
            try:
                await page.wait_for_selector(
                    "div.poly-card, li.promotion-item, "
                    "li.ui-search-layout__item",
                    timeout=15_000,
                )
            except Exception:
                logger.warning(
                    "no_cards_found",
                    source=source.name,
                    page=page_num,
                )
                break

            await self._human_scroll(page)

            html = await page.content()
            page_products = self._parse_page(html, source)

            logger.info(
                "page_parsed",
                source=source.name,
                page=page_num,
                raw_count=len(page_products),
            )

            # Dedup + persistência inline (se storage fornecido)
            for p in page_products:
                if self._storage:
                    try:
                        is_dupe = await self._storage.check_duplicate(
                            p.ml_id
                        )
                        if is_dupe:
                            dupes_skipped += 1
                            continue
                        product_id = await self._storage.upsert_product(p)
                        await self._storage.add_price_history(
                            product_id, p.price, p.original_price
                        )
                    except Exception as exc:
                        logger.warning(
                            "storage_error",
                            ml_id=p.ml_id,
                            error=str(exc),
                        )
                products.append(p)

            if not page_products:
                break

            # Próxima página
            next_url = await self._resolve_next_page(
                page, source, page_num
            )
            if not next_url:
                logger.info(
                    "no_more_pages",
                    source=source.name,
                    stopped_at=page_num,
                )
                break

            url = next_url
            await self._random_delay()

        return products, dupes_skipped, parse_errors

    # ------------------------------------------------------------------
    # Paginação
    # ------------------------------------------------------------------

    def _build_url(self, source: ScrapeSource, page_num: int) -> str:
        """Constrói URL para a página solicitada."""
        if source.pagination == "offset" and page_num > 1:
            offset = (page_num - 1) * self.ITEMS_PER_PAGE
            params = {**source.search_params, "_from": str(offset)}
            query = urlencode(params)
            return f"{source.url}?{query}"

        if source.search_params:
            query = urlencode(source.search_params)
            return f"{source.url}?{query}"

        return source.url

    async def _resolve_next_page(
        self,
        page: Page,
        source: ScrapeSource,
        current_page: int,
    ) -> str | None:
        """Resolve URL da próxima página conforme tipo de paginação."""
        if source.pagination == "offset":
            if current_page < source.max_pages:
                return self._build_url(source, current_page + 1)
            return None

        # Paginação por link (ofertas do dia)
        return await self._get_next_page_url(page)

    async def _get_next_page_url(self, page: Page) -> str | None:
        """Detecta e retorna a URL da próxima página via link."""
        try:
            for selector in SELECTORS["next_page"].split(", "):
                el = await page.query_selector(selector)
                if el:
                    href = await el.get_attribute("href")
                    if href:
                        return (
                            self.full_url(href)
                            if href.startswith("/")
                            else href
                        )

            # Fallback: procura link com texto "Seguinte"
            links = await page.query_selector_all(
                SELECTORS["pagination_links"]
            )
            for link in links:
                text = (await link.inner_text()).strip().lower()
                if text in (
                    "seguinte",
                    "siguiente",
                    "next",
                    "próxima",
                ):
                    href = await link.get_attribute("href")
                    if href:
                        return (
                            self.full_url(href)
                            if href.startswith("/")
                            else href
                        )

        except Exception as exc:
            logger.debug("pagination_check_error", error=str(exc))

        return None

    # ------------------------------------------------------------------
    # Parsing unificado
    # ------------------------------------------------------------------

    def _parse_page(
        self, html: str, source: ScrapeSource
    ) -> list[ScrapedProduct]:
        """Extrai todos os produtos do HTML com seletores unificados."""
        soup = BeautifulSoup(html, "lxml")
        products: list[ScrapedProduct] = []

        items = soup.select(SELECTORS["card"])
        for item in items:
            product = self._parse_item(item, source)
            if product:
                products.append(product)

        return products

    def _parse_item(
        self, item: Tag, source: ScrapeSource
    ) -> Optional[ScrapedProduct]:
        """
        Extrai TODOS os campos de um card de produto.

        Extração padronizada independente da fonte:
        ml_id, url, title, price, original_price, discount_pct,
        rating, review_count, seller, is_official_store,
        free_shipping, image_url, category.
        """
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

            # --- Desconto explícito ---
            discount_tag = item.select_one(SELECTORS["discount"])
            discount_text = (
                discount_tag.get_text(strip=True) if discount_tag else ""
            )
            explicit_discount = self._parse_discount_pct(discount_text)

            # --- Avaliação ---
            rating = self._parse_rating(item)

            # --- Reviews ---
            review_count = self._parse_review_count(item)

            # --- Vendedor ---
            seller_name_tag = item.select_one(SELECTORS["seller_name"])
            seller = (
                seller_name_tag.get_text(strip=True)
                if seller_name_tag
                else ""
            )

            # --- Loja oficial ---
            official_tag = item.select_one(SELECTORS["official_store"])
            is_official = official_tag is not None

            # --- Frete grátis ---
            shipping_tag = item.select_one(SELECTORS["shipping"])
            free_shipping = False
            if shipping_tag:
                text = shipping_tag.get_text(strip=True).lower()
                free_shipping = "grátis" in text or "gratis" in text

            # --- Imagem ---
            img_tag = item.select_one(SELECTORS["image"])
            image_url = ""
            if img_tag:
                image_url = str(
                    img_tag.get("data-src")
                    or img_tag.get("src")
                    or ""
                )

            # --- Montar produto ---
            product = ScrapedProduct(
                ml_id=ml_id,
                url=url,
                title=title,
                price=price,
                original_price=original_price,
                rating=rating,
                review_count=review_count,
                seller=seller,
                is_official_store=is_official,
                category=source.category,
                image_url=image_url,
                free_shipping=free_shipping,
                source=source.name,
            )

            # Usa desconto explícito se preço original ausente
            if explicit_discount and not original_price:
                product.discount_pct = explicit_discount

            return product

        except Exception as exc:
            logger.debug("parse_item_error", error=str(exc))
            return None

    # ------------------------------------------------------------------
    # Extração de preço (robusto: fraction + cents)
    # ------------------------------------------------------------------

    def _get_current_price(self, card: Tag) -> float | None:
        """Extrai o preço atual de um card (fraction + cents se disponível)."""
        # Estratégia 1: container .poly-price__current
        container = card.select_one(
            SELECTORS["price_current_container"]
        )
        if container:
            price = self._price_from_andes(container)
            if price:
                return price

        # Estratégia 2: primeiro fraction que NÃO esteja em <s>/<del>
        for fraction in card.select(SELECTORS["fraction"]):
            if not fraction.find_parent(["s", "del"]):
                return self._clean_price(
                    fraction.get_text(strip=True)
                )

        return None

    def _get_original_price(self, card: Tag) -> float | None:
        """Extrai o preço original (riscado / antes do desconto)."""
        # Estratégia 1: container poly-price__original
        for selector in SELECTORS["price_original_container"].split(
            ", "
        ):
            container = card.select_one(selector)
            if container:
                price = self._price_from_andes(container)
                if price:
                    return price

        # Estratégia 2: seletores de busca (del, s)
        for selector in SELECTORS["price_original_search"].split(", "):
            tag = card.select_one(selector)
            if tag:
                return self._clean_price(tag.get_text(strip=True))

        # Estratégia 3: fraction dentro de <s> ou <del>
        for tag_name in ("s", "del"):
            parent = card.select_one(tag_name)
            if parent:
                fraction = parent.select_one(SELECTORS["fraction"])
                if fraction:
                    return self._clean_price(
                        fraction.get_text(strip=True)
                    )

        return None

    def _price_from_andes(self, container: Tag) -> float | None:
        """
        Extrai preço de um container andes-money-amount.
        Combina fraction (parte inteira) com cents (centavos).

        Exemplos:
            fraction="1.299", cents=",90" → 1299.90
            fraction="299", cents=None → 299.0
        """
        fraction_el = container.select_one(SELECTORS["fraction"])
        if not fraction_el:
            return None

        fraction_text = fraction_el.get_text(strip=True)
        fraction_clean = fraction_text.replace(".", "")

        try:
            base = int(fraction_clean)
        except ValueError:
            return self._clean_price(fraction_text)

        cents_el = container.select_one(SELECTORS["cents"])
        if cents_el:
            cents_text = (
                cents_el.get_text(strip=True).lstrip(",").strip()
            )
            try:
                return float(base) + int(cents_text) / 100
            except ValueError:
                pass

        return float(base)

    # ------------------------------------------------------------------
    # Extração padronizada de rating, reviews, seller
    # ------------------------------------------------------------------

    def _parse_rating(self, item: Tag) -> float:
        """Extrai avaliação média (0-5 estrelas) do card."""
        tag = item.select_one(SELECTORS["rating"])
        if not tag:
            return 0.0
        try:
            text = tag.get_text(strip=True).replace(",", ".")
            rating = float(text)
            return rating if 0 <= rating <= 5 else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _parse_review_count(self, item: Tag) -> int:
        """Extrai número de reviews do card."""
        tag = item.select_one(SELECTORS["review_count"])
        if not tag:
            return 0
        text = re.sub(r"[^\d]", "", tag.get_text())
        try:
            return int(text)
        except (ValueError, TypeError):
            return 0
