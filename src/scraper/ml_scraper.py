"""
Crivo — Scraper Unificado do Mercado Livre
Coleta ofertas do Mercado Livre (Ofertas do Dia) com extração
padronizada de todos os campos dos cards de listagem.

Uso:
    scraper = MLScraper()
    products = await scraper.scrape()
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from src.database.storage_manager import StorageManager

import structlog
import asyncio
from bs4 import BeautifulSoup, Tag
from playwright.async_api import Page

from src.config import settings
from .base_scraper import BaseScraper, CaptchaError, RateLimitError, ScrapedProduct
from .ml_classifier import (
    get_product_category,
    classify_with_ai,
    get_product_gender,
    classify_gender_with_ai,
    GENDER_RELEVANT_CATEGORIES,
)

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Configuração de fonte de scraping
# ---------------------------------------------------------------------------


OFERTAS_URL = "https://www.mercadolivre.com.br/ofertas"


@dataclass
class ScrapeSource:
    """Configuração de uma fonte de scraping."""

    name: str  # Ex: "ofertas_do_dia"
    url: str  # URL base da fonte
    max_pages: int = 10


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
        "a.poly-component__title, a.ui-search-link, a[href*='mercadolivre']"
    ),
    # Preço atual — container andes (fraction + cents)
    "price_current_container": ".poly-price__current",
    "fraction": ".andes-money-amount__fraction",
    "cents": ".andes-money-amount__cents",
    # Preço original (riscado) — poly- e ui-search-
    "price_original_container": (
        "s.poly-price__original, "
        "s.andes-money-amount--previous"
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
    # Avaliação e reviews (poly- + ui-search- fallbacks)
    "rating": ".poly-reviews__rating, span.ui-search-reviews__rating-number",
    "review_count": ".poly-reviews__total, span.ui-search-reviews__amount",
    # Parcelamento
    "installments": ".poly-price__installments",
    # Badges
    "badge": "span.poly-component__highlight",
    # Paginação (link-based)
    "next_page": (
        "a.andes-pagination__link--next, li.andes-pagination__button--next a"
    ),
    "pagination_links": "a.andes-pagination__link",
}


# ---------------------------------------------------------------------------
# Scraper Unificado
# ---------------------------------------------------------------------------


class MLScraper(BaseScraper):
    """
    Scraper unificado do Mercado Livre.

    Coleta ofertas do dia com extração padronizada de todos os campos
    disponíveis nos cards de listagem.

    Campos extraídos de cada card:
    - ml_id, url, title, price, original_price, discount_pct
    - rating, review_count, free_shipping, image_url
    """

    def __init__(
        self,
        sources: Optional[list[ScrapeSource]] = None,
        storage: Optional["StorageManager"] = None,
    ):
        super().__init__()
        self._storage = storage
        self.sources = sources or self._default_sources()
        # ID único da execução (usado para nomear o diretório de debug)
        self.run_id: str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # Screenshots dos cards: {ml_id: bytes_png} — populado se debug_screenshots=True
        self.card_screenshots: dict[str, bytes] = {}

    def _default_sources(self) -> list[ScrapeSource]:
        """Carrega as fontes habilitadas do ml_categories.json via settings.sources."""
        import json
        from src.config import ROOT_DIR

        categories_file = ROOT_DIR / "ml_categories.json"
        with categories_file.open() as f:
            data = json.load(f)

        sources = []
        for section in data.values():
            for entry in section:
                if getattr(settings.sources, entry["source"], False):
                    sources.append(
                        ScrapeSource(
                            name=entry["source"],
                            url=entry["url"],
                            max_pages=settings.scraper.max_pages,
                        )
                    )

        if not sources:
            logger.warning(
                "no_sources_enabled",
                hint="Habilite ao menos uma fonte no .env (ex: SOURCE_OFERTA_DO_DIA=true)",
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
        Se storage foi fornecido, classifica apenas produtos novos via AI.
        """
        start = time.monotonic()
        all_products: list[ScrapedProduct] = []
        total_existing = 0
        total_errors = 0

        async with self:
            sem = asyncio.Semaphore(settings.scraper.max_concurrent)

            async def scrape_source_sem(source: ScrapeSource):
                async with sem:
                    page = await self._new_page()
                    logger.info(
                        "scraping_source",
                        source=source.name,
                        max_pages=source.max_pages,
                    )
                    try:
                        products, existing, errors = await self._scrape_source(page, source)
                        logger.info("source_done", source=source.name, count=len(products))
                        return products, existing, errors
                    except CaptchaError:
                        logger.error("captcha_blocked", source=source.name)
                        if self._storage:
                            await self._storage.log_event(
                                "scrape_error", {"reason": "captcha", "source": source.name}
                            )
                        return [], 0, 1
                    except RateLimitError:
                        logger.error("rate_limited", source=source.name)
                        if self._storage:
                            await self._storage.log_event(
                                "scrape_error", {"reason": "rate_limit", "source": source.name}
                            )
                        return [], 0, 1
                    except Exception as exc:
                        logger.error("source_failed", source=source.name, error=str(exc))
                        return [], 0, 1
                    finally:
                        await page.close()

            results = await asyncio.gather(*[scrape_source_sem(s) for s in self.sources])
            for products, existing, errors in results:
                all_products.extend(products)
                total_existing += existing
                total_errors += errors

        # Dedup cross-page: mesmo ml_id pode aparecer em várias fontes
        seen: dict[str, ScrapedProduct] = {}
        for p in all_products:
            seen[p.ml_id] = p  # mantém última ocorrência (dados mais frescos)
        all_products = list(seen.values())

        elapsed = round(time.monotonic() - start, 1)
        logger.info(
            "scraping_done",
            total=len(all_products),
            existing_in_db=total_existing,
            ai_classification_skipped=total_existing,
            errors=total_errors,
            elapsed_seconds=elapsed,
        )

        if self._storage:
            await self._storage.log_event(
                "scrape_success",
                {
                    "total": len(all_products),
                    "sources": len(self.sources),
                    "existing_in_db": total_existing,
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

        Retorna (products, existing_in_db, parse_errors).
        """
        products: list[ScrapedProduct] = []
        existing_in_db = 0
        parse_errors = 0
        url = self._build_url(source, 1)

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
                raise CaptchaError(f"CAPTCHA detectado em {source.name}")

            # Aguarda os cards carregarem
            try:
                await page.wait_for_selector(
                    "div.poly-card, li.promotion-item, li.ui-search-layout__item",
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

            # Debug: screenshot de cada card enquanto a página ainda está aberta
            if settings.scraper.debug_screenshots and page_products:
                await self._screenshot_cards(page, page_products)

            logger.info(
                "page_parsed",
                source=source.name,
                page=page_num,
                raw_count=len(page_products),
            )

            # Verifica quais ml_ids já existem (para pular classificação AI)
            existing_ids: set[str] = set()
            if self._storage:
                try:
                    existing_ids = await self._storage.check_duplicates_batch(
                        [p.ml_id for p in page_products]
                    )
                except Exception as exc:
                    logger.warning("check_duplicates_failed", error=str(exc))

            new_page = [p for p in page_products if p.ml_id not in existing_ids]
            existing_page = [p for p in page_products if p.ml_id in existing_ids]

            if existing_page:
                logger.info(
                    "ai_classification_skipped",
                    reason="products_already_in_db",
                    count=len(existing_page),
                )

            # AI enrichment apenas para produtos NOVOS
            new_page = await self._enrich_categories_with_ai(new_page)
            new_page = await self._enrich_gender(new_page)

            # Retorna TODOS para o pipeline (novos + existentes)
            products.extend(new_page)
            products.extend(existing_page)
            existing_in_db += len(existing_page)

            if not page_products:
                break

            # Próxima página
            next_url = await self._resolve_next_page(page, source, page_num)
            if not next_url:
                logger.info(
                    "no_more_pages",
                    source=source.name,
                    stopped_at=page_num,
                )
                break

            url = next_url
            await self._random_delay()

        return products, existing_in_db, parse_errors

    async def _save_page_products_batch(
        self, page_products: list[ScrapedProduct]
    ) -> tuple[list[ScrapedProduct], int]:
        """
        Deduplica e persiste produtos de uma página em batch.
        Retorna (new_products, dupes_skipped).
        """
        if not self._storage or not page_products:
            return page_products, 0

        try:
            existing = await self._storage.check_duplicates_batch(
                [p.ml_id for p in page_products]
            )
            dupes = len(existing)
            new_products = [p for p in page_products if p.ml_id not in existing]

            if new_products:
                ids = await self._storage.upsert_products_batch(new_products)
                entries = [
                    {
                        "product_id": ids[p.ml_id],
                        "price": p.price,
                        "original_price": p.original_price,
                    }
                    for p in new_products
                    if p.ml_id in ids
                ]
                await self._storage.add_price_history_batch(entries)

            return new_products, dupes
        except Exception as exc:
            logger.warning("storage_batch_error", error=str(exc))
            return page_products, 0

    async def _enrich_categories_with_ai(
        self, products: list[ScrapedProduct]
    ) -> list[ScrapedProduct]:
        """Runs the LLM classifier concurrently for products categorized as 'Outros'."""
        outros_products = [p for p in products if p.category == "Outros"]
        if not outros_products:
            return products

        logger.info("ai_enrichment_start", count=len(outros_products))

        async def _classify_and_update(p: ScrapedProduct):
            try:
                new_cat = await classify_with_ai(p.title)
                p.category = new_cat
            except Exception as e:
                logger.warning("ai_enrichment_failed", title=p.title, error=str(e))

        # Run classifications concurrently with a small concurrency limit if needed,
        # but asyncio.gather is fine for a single page (usually ~50 products max)
        await asyncio.gather(*[_classify_and_update(p) for p in outros_products])

        logger.info("ai_enrichment_done")
        return products

    async def _enrich_gender(
        self, products: list[ScrapedProduct]
    ) -> list[ScrapedProduct]:
        """
        Classifica o gênero de produtos em categorias relevantes.

        1. Tenta keyword rápida para todos.
        2. Produtos incertos (keyword retornou None) → chamada IA em batch.
        Produtos fora das categorias relevantes ficam com "Sem gênero".
        """
        needs_ai: list[ScrapedProduct] = []

        for p in products:
            result = get_product_gender(p.title, p.category)
            if result is None:
                needs_ai.append(p)
            else:
                p.gender = result

        if not needs_ai:
            return products

        logger.info("ai_gender_enrichment_start", count=len(needs_ai))

        async def _classify_gender_and_update(p: ScrapedProduct):
            try:
                p.gender = await classify_gender_with_ai(p.title)
            except Exception as e:
                logger.warning("ai_gender_enrichment_failed", title=p.title, error=str(e))
                p.gender = "Unissex"

        await asyncio.gather(*[_classify_gender_and_update(p) for p in needs_ai])

        logger.info(
            "ai_gender_enrichment_done",
            relevant_categories=list(GENDER_RELEVANT_CATEGORIES),
        )
        return products

    # ------------------------------------------------------------------
    # Paginação
    # ------------------------------------------------------------------

    def _build_url(self, source: ScrapeSource, _page_num: int) -> str:
        """Constrói URL para a página solicitada."""
        return source.url

    async def _resolve_next_page(
        self,
        page: Page,
        _source: ScrapeSource,
        _current_page: int,
    ) -> str | None:
        """Resolve URL da próxima página via link."""
        return await self._get_next_page_url(page)

    async def _get_next_page_url(self, page: Page) -> str | None:
        """Detecta e retorna a URL da próxima página via link."""
        try:
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
                if text in (
                    "seguinte",
                    "siguiente",
                    "next",
                    "próxima",
                ):
                    href = await link.get_attribute("href")
                    if href:
                        return self.full_url(href) if href.startswith("/") else href

        except Exception as exc:
            logger.debug("pagination_check_error", error=str(exc))

        return None

    # ------------------------------------------------------------------
    # Debug: screenshot de cards (Playwright)
    # ------------------------------------------------------------------

    async def _screenshot_cards(
        self, page: Page, products: list[ScrapedProduct]
    ) -> None:
        """
        Tira screenshot de cada card de produto enquanto a página ainda está aberta.

        Usa matching por posição DOM: re-parseia o HTML atual para descobrir o
        índice de cada produto, depois screenshot pelo índice no Playwright.
        Isso é robusto contra tracking URLs que não contêm o ml_id no href.
        """
        from bs4 import BeautifulSoup

        # Re-parseia o HTML atual para construir ml_id → índice DOM
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        items = soup.select(SELECTORS["card"])

        index_map: dict[str, int] = {}
        for idx, item in enumerate(items):
            link_tag = item.select_one(SELECTORS["link"])
            if not link_tag:
                continue
            raw_url = str(link_tag.get("href", ""))
            mid = self._extract_ml_id(raw_url)
            if mid:
                index_map[mid] = idx

        # Obtém todos os card elements do DOM via Playwright (mesma ordem)
        card_locator = page.locator(SELECTORS["card"])
        card_count = await card_locator.count()

        success = 0
        for product in products:
            if product.ml_id in self.card_screenshots:
                continue
            idx = index_map.get(product.ml_id)
            if idx is None or idx >= card_count:
                logger.debug("card_index_not_found", ml_id=product.ml_id)
                continue
            try:
                screenshot_bytes = await card_locator.nth(idx).screenshot()
                self.card_screenshots[product.ml_id] = screenshot_bytes
                success += 1
            except Exception as exc:
                logger.debug(
                    "card_screenshot_failed",
                    ml_id=product.ml_id,
                    error=str(exc),
                )

        logger.debug(
            "cards_screenshotted",
            success=success,
            total=len(products),
        )

    # ------------------------------------------------------------------
    # Parsing unificado
    # ------------------------------------------------------------------

    def _parse_page(self, html: str, source: ScrapeSource) -> list[ScrapedProduct]:
        """Extrai todos os produtos do HTML com seletores unificados."""
        soup = BeautifulSoup(html, "lxml")
        products: list[ScrapedProduct] = []

        items = soup.select(SELECTORS["card"])
        for item in items:
            product = self._parse_item(item, source)
            if product:
                products.append(product)

        return products

    def _resolve_tracking_url(self, url: str) -> str:
        """Extrai a URL real de tracking URLs do MercadoLivre (mclics/...)."""
        if "click1.mercadolivre" in url or "/mclics/" in url:
            from urllib.parse import parse_qs, urlparse, unquote

            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            real_url = params.get("url", [None])[0]
            if real_url:
                return unquote(real_url)
        return url

    def _parse_free_shipping(self, item: Tag) -> bool:
        tag = item.select_one(SELECTORS["shipping"])
        if tag:
            text = tag.get_text(strip=True).lower()
            return "grátis" in text or "gratis" in text
        return False

    def _parse_installments(self, item: Tag) -> bool:
        tag = item.select_one(SELECTORS["installments"])
        if tag:
            text = tag.get_text(strip=True).lower()
            return "sem juros" in text or "sin interés" in text
        return False

    def _parse_image_url(self, item: Tag) -> str:
        img_tag = item.select_one(SELECTORS["image"])
        if img_tag:
            return str(img_tag.get("data-src") or img_tag.get("src") or "")
        return ""

    def _parse_item(self, item: Tag, source: ScrapeSource) -> Optional[ScrapedProduct]:
        """
        Extrai TODOS os campos de um card de produto.

        Extração padronizada independente da fonte:
        ml_id, url, title, price, original_price, discount_pct,
        rating, review_count, free_shipping, image_url, category.
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

            url = self._resolve_tracking_url(url)

            if url.startswith("/"):
                url = self.full_url(url)

            # --- Título ---
            title_tag = item.select_one(SELECTORS["title"])
            title = title_tag.get_text(strip=True) if title_tag else ""
            if not title:
                return None

            # --- Preços (atual + Pix se aplicável) ---
            price, pix_price = self._get_prices(item)
            if price is None or price <= 0:
                return None

            # --- Preço original (riscado) ---
            original_price = self._get_original_price(item)

            # --- Desconto explícito ---
            discount_tag = item.select_one(SELECTORS["discount"])
            discount_text = discount_tag.get_text(strip=True) if discount_tag else ""
            explicit_discount = self._parse_discount_pct(discount_text)

            # --- Avaliação ---
            rating = self._parse_rating(item)

            # --- Reviews ---
            review_count = self._parse_review_count(item)

            # --- Frete grátis ---
            free_shipping = self._parse_free_shipping(item)

            # --- Parcelamento sem juros ---
            installments_without_interest = self._parse_installments(item)

            # --- Imagem ---
            image_url = self._parse_image_url(item)

            # --- Badge ---
            badge_tag = item.select_one(SELECTORS["badge"])
            badge = badge_tag.get_text(strip=True) if badge_tag else ""

            # --- Montar produto ---
            product = ScrapedProduct(
                ml_id=ml_id,
                url=url,
                title=title,
                price=price,
                original_price=original_price,
                pix_price=pix_price,
                rating=rating,
                review_count=review_count,
                category=get_product_category(title),
                image_url=image_url,
                free_shipping=free_shipping,
                installments_without_interest=installments_without_interest,
                badge=badge,
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

    def _get_prices(self, card: Tag) -> tuple[float | None, float | None]:
        """Extrai preço do cartão e preço Pix de um card.

        Retorna (card_price, pix_price):
        - card_price: preço "universal" (cartão/parcelado) — sempre presente
        - pix_price: preço com desconto Pix/boleto — None se não houver

        Quando o card exibe um preço de meio de pagamento (Pix, boleto),
        o valor em .poly-price__current é o preço Pix.
        O preço listado real aparece em .poly-price__installments
        como "ou R$ X.XXX em Nx ...".
        """
        container = card.select_one(SELECTORS["price_current_container"])
        if container:
            if self._is_payment_method_price(container):
                # O preço principal exibido é o Pix
                pix_price = self._price_from_andes(container)
                # O preço "real" (cartão) está nas parcelas
                card_price = self._get_listed_price(card)
                if card_price and pix_price:
                    return card_price, pix_price
                # Fallback: se não encontrou preço de parcela, usa o Pix como preço
                if pix_price:
                    return pix_price, None

            # Preço normal (sem desconto de meio de pagamento)
            price = self._price_from_andes(container)
            if price:
                return price, None

        # Fallback: primeiro fraction que NÃO esteja em <s>/<del>
        for fraction in card.select(SELECTORS["fraction"]):
            if not fraction.find_parent(["s", "del"]):
                price = self._clean_price(fraction.get_text(strip=True))
                return price, None

        return None, None

    def _is_payment_method_price(self, container: Tag) -> bool:
        """Detecta se .poly-price__current exibe preço de meio de pagamento."""
        disc_el = container.select_one(
            ".andes-money-amount__discount, .poly-price__disc_label"
        )
        if not disc_el:
            return False
        text = disc_el.get_text(strip=True).lower()
        return any(kw in text for kw in ("pix", "boleto"))

    def _get_listed_price(self, card: Tag) -> float | None:
        """Extrai o preço listado real da seção de parcelamento.

        Em cards com preço Pix, a estrutura é:
          .poly-price__installments → "ou R$ 2.478 em 10x R$ 247,83 sem juros"
        O primeiro andes-money-amount.poly-phrase-price é o preço listado.
        """
        installments = card.select_one(SELECTORS["installments"])
        if not installments:
            return None
        amounts = installments.select("span.andes-money-amount.poly-phrase-price")
        if amounts:
            return self._price_from_andes(amounts[0])
        return None

    def _get_original_price(self, card: Tag) -> float | None:
        """Extrai o preço original (riscado / antes do desconto)."""
        # Estratégia 1: container com classe conhecida
        for selector in SELECTORS["price_original_container"].split(", "):
            container = card.select_one(selector)
            if container:
                price = self._price_from_andes(container)
                if price:
                    return price

        # Estratégia 2: <s> ou <del> com andes-money-amount (extrai fraction + cents)
        for tag_name in ("s", "del"):
            parent = card.select_one(tag_name)
            if parent:
                price = self._price_from_andes(parent)
                if price:
                    return price

        # Estratégia 3: seletores de busca legados (ui-search-)
        for selector in SELECTORS["price_original_search"].split(", "):
            tag = card.select_one(selector)
            if tag:
                return self._clean_price(tag.get_text(strip=True))

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
            cents_text = cents_el.get_text(strip=True).lstrip(",").strip()
            try:
                return float(base) + int(cents_text) / 100
            except ValueError:
                pass

        return float(base)

    # ------------------------------------------------------------------
    # Extração padronizada de rating, reviews
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
