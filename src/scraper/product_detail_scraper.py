"""
DealHunter — Product Detail Scraper
Visita páginas individuais de produto do Mercado Livre para extrair
dados detalhados não disponíveis nos cards de listagem.

Dados coletados:
  - Reputação do vendedor (MercadoLíder Platinum/Gold/Silver)
  - Nome do vendedor
  - Quantidade vendida
  - Estrelas e contagem de avaliações (mais confiável que card)
  - Loja oficial (badge)

Uso pelo worker:
    async with ProductDetailScraper() as scraper:
        data = await scraper.enrich_product(url, ml_id)
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field

import structlog
from bs4 import BeautifulSoup

from src.config import settings
from .base_scraper import BaseScraper, ScrapedProduct

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Data model para dados enriquecidos
# ---------------------------------------------------------------------------


@dataclass
class EnrichedProductData:
    """Dados extraídos da página de detalhe de um produto."""

    ml_id: str
    seller_name: str = ""
    seller_reputation: str = ""  # "platinum" | "gold" | "silver" | ""
    sold_quantity: int = 0
    rating: float = 0.0
    review_count: int = 0
    is_official_store: bool = False
    enrichment_success: bool = True
    error_message: str = ""


# ---------------------------------------------------------------------------
# Seletores CSS para página de produto do ML
# (validar com DevTools antes de deploy em produção)
# ---------------------------------------------------------------------------

DETAIL_SELECTORS = {
    "seller_reputation": (
        "p.ui-seller-data-status__title, "
        "span.ui-seller-data-header__subtitle, "
        "div.ui-seller-info__status-icon"
    ),
    "seller_name": (
        "span.ui-pdp-seller__header__title, "
        "a.ui-seller-data-header__link, "
        "span.ui-seller-info__name"
    ),
    "sold_quantity": (
        "span.ui-pdp-subtitle__text, "
        "span.ui-pdp-header__subtitle"
    ),
    "rating": (
        "span.ui-pdp-review__rating, "
        "span.ui-pdp-reviews__rating__summary__average"
    ),
    "review_count": (
        "span.ui-pdp-review__amount, "
        "span.ui-pdp-reviews__amount"
    ),
    "official_store": (
        "a.ui-pdp-merchant-header__link, "
        "span.ui-pdp-official-store-label"
    ),
}


# ---------------------------------------------------------------------------
# Scraper de detalhe
# ---------------------------------------------------------------------------


class ProductDetailScraper(BaseScraper):
    """
    Scraper para páginas individuais de produto do Mercado Livre.

    Projetado para uso como instância de longa vida dentro do worker.
    Usa o BaseScraper para herdar toda a infraestrutura anti-bot.

    Diferenças em relação aos scrapers de listagem:
      - Delays mais longos (configuráveis via DeepScrapeConfig)
      - Rotação de contexto mais frequente
      - Não bloqueia imagens (aparência mais humana)
      - Lock para rotação de contexto thread-safe
    """

    def __init__(self) -> None:
        super().__init__()
        # Sobrescrever delays para serem mais conservadores
        self.cfg = settings.scraper  # mantém referência ao config base
        self._deep_cfg = settings.deep_scrape
        self._context_lock = asyncio.Lock()

    async def scrape(self) -> list[ScrapedProduct]:
        """Não usado — este scraper processa URLs individuais."""
        raise NotImplementedError(
            "ProductDetailScraper não implementa scrape(). "
            "Use enrich_product() para processar URLs individuais."
        )

    async def _rotate_context_if_needed(
        self, every_n_requests: int = 0
    ) -> None:
        """Override com lock e frequência configurável."""
        n = every_n_requests or self._deep_cfg.context_rotation_every
        if self._request_count > 0 and self._request_count % n == 0:
            async with self._context_lock:
                logger.info(
                    "rotating_context",
                    request_count=self._request_count,
                )
                await self._context.close()
                await self._new_context()

    async def _deep_delay(self) -> None:
        """Delay mais longo para deep scraping."""
        import random

        delay = random.uniform(
            self._deep_cfg.delay_min,
            self._deep_cfg.delay_max,
        )
        logger.debug("deep_delay", seconds=round(delay, 2))
        await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # Método principal: enriquecer um produto
    # ------------------------------------------------------------------

    async def enrich_product(
        self, url: str, ml_id: str
    ) -> EnrichedProductData:
        """
        Visita a página de um produto e extrai dados detalhados.

        Args:
            url: URL completa do produto no ML
            ml_id: ID do produto (ex: "MLB123456")

        Returns:
            EnrichedProductData com enrichment_success=False se falhar.
        """
        page = await self._context.new_page()
        try:
            await self._deep_delay()

            success = await self._goto(page, url)
            if not success:
                return EnrichedProductData(
                    ml_id=ml_id,
                    enrichment_success=False,
                    error_message="page_load_failed",
                )

            # Verificar se o produto está disponível
            if await self._is_product_unavailable(page):
                return EnrichedProductData(
                    ml_id=ml_id,
                    enrichment_success=False,
                    error_message="product_unavailable",
                )

            # Simular comportamento humano
            await self._human_scroll(page)

            # Extrair dados
            html = await page.content()
            return self._parse_product_detail(html, ml_id)

        except Exception as exc:
            logger.error(
                "enrich_product_error",
                ml_id=ml_id,
                error=str(exc),
            )
            return EnrichedProductData(
                ml_id=ml_id,
                enrichment_success=False,
                error_message=str(exc)[:200],
            )
        finally:
            await page.close()

    # ------------------------------------------------------------------
    # Detecção de produto indisponível
    # ------------------------------------------------------------------

    async def _is_product_unavailable(self, page) -> bool:
        """Verifica se a página indica que o produto não existe mais."""
        try:
            title = await page.title()
            lower_title = title.lower()
            if any(
                kw in lower_title
                for kw in [
                    "não encontr",
                    "not found",
                    "publicação pausada",
                    "404",
                ]
            ):
                return True

            # Verificar conteúdo da página
            content = await page.content()
            if "publicação pausada" in content.lower():
                return True
            if "esta publicação não existe" in content.lower():
                return True

        except Exception:
            pass
        return False

    # ------------------------------------------------------------------
    # Parsing da página de detalhe
    # ------------------------------------------------------------------

    def _parse_product_detail(
        self, html: str, ml_id: str
    ) -> EnrichedProductData:
        """Extrai dados detalhados do HTML da página de produto."""
        soup = BeautifulSoup(html, "html.parser")

        seller_name = self._parse_seller_name(soup)
        seller_reputation = self._parse_seller_reputation(soup)
        sold_quantity = self._parse_sold_quantity(soup)
        rating = self._parse_rating(soup)
        review_count = self._parse_review_count(soup)
        is_official_store = self._parse_official_store(soup)

        logger.debug(
            "product_detail_parsed",
            ml_id=ml_id,
            seller_name=seller_name,
            seller_reputation=seller_reputation,
            sold_quantity=sold_quantity,
            rating=rating,
            review_count=review_count,
            is_official_store=is_official_store,
        )

        return EnrichedProductData(
            ml_id=ml_id,
            seller_name=seller_name,
            seller_reputation=seller_reputation,
            sold_quantity=sold_quantity,
            rating=rating,
            review_count=review_count,
            is_official_store=is_official_store,
            enrichment_success=True,
        )

    def _parse_seller_name(self, soup: BeautifulSoup) -> str:
        """Extrai nome do vendedor."""
        for selector in DETAIL_SELECTORS["seller_name"].split(", "):
            tag = soup.select_one(selector.strip())
            if tag:
                text = tag.get_text(strip=True)
                if text:
                    return text
        return ""

    def _parse_seller_reputation(self, soup: BeautifulSoup) -> str:
        """Extrai tier do vendedor: platinum, gold, silver, ou vazio."""
        for selector in DETAIL_SELECTORS["seller_reputation"].split(", "):
            tag = soup.select_one(selector.strip())
            if tag:
                text = tag.get_text(strip=True).lower()
                if "platinum" in text or "platina" in text:
                    return "platinum"
                elif "gold" in text or "ouro" in text:
                    return "gold"
                elif "silver" in text or "prata" in text:
                    return "silver"

        # Fallback: buscar no atributo de classe ou data-attributes
        rep_icons = soup.select(
            "[class*='seller-reputation'], [class*='seller_reputation']"
        )
        for icon in rep_icons:
            classes = " ".join(icon.get("class", []))
            if "platinum" in classes or "platina" in classes:
                return "platinum"
            elif "gold" in classes or "ouro" in classes:
                return "gold"
            elif "silver" in classes or "prata" in classes:
                return "silver"

        return ""

    def _parse_sold_quantity(self, soup: BeautifulSoup) -> int:
        """Extrai quantidade vendida de texto como '500+ vendidos'."""
        for selector in DETAIL_SELECTORS["sold_quantity"].split(", "):
            tag = soup.select_one(selector.strip())
            if tag:
                text = tag.get_text(strip=True)
                # Padrões: "500 vendidos", "+1000 vendidos",
                # "Mais de 500 vendidos", "5mil vendidos"
                match = re.search(
                    r"\+?\s*(\d[\d.]*)\s*(?:mil)?\s*\+?\s*vendid",
                    text,
                    re.IGNORECASE,
                )
                if match:
                    num_str = match.group(1).replace(".", "")
                    try:
                        qty = int(num_str)
                        # "5mil" → 5000
                        if "mil" in text.lower():
                            qty *= 1000
                        return qty
                    except ValueError:
                        pass
        return 0

    def _parse_rating(self, soup: BeautifulSoup) -> float:
        """Extrai nota média (estrelas) do produto."""
        for selector in DETAIL_SELECTORS["rating"].split(", "):
            tag = soup.select_one(selector.strip())
            if tag:
                text = tag.get_text(strip=True)
                # Normalizar decimal brasileiro: "4,5" → "4.5"
                text = text.replace(",", ".")
                match = re.search(r"(\d+\.?\d*)", text)
                if match:
                    try:
                        rating = float(match.group(1))
                        if 0 <= rating <= 5:
                            return rating
                    except ValueError:
                        pass
        return 0.0

    def _parse_review_count(self, soup: BeautifulSoup) -> int:
        """Extrai número de avaliações."""
        for selector in DETAIL_SELECTORS["review_count"].split(", "):
            tag = soup.select_one(selector.strip())
            if tag:
                text = tag.get_text(strip=True)
                # Padrões: "(152)", "152 opiniões", "152"
                match = re.search(r"(\d[\d.]*)", text)
                if match:
                    num_str = match.group(1).replace(".", "")
                    try:
                        return int(num_str)
                    except ValueError:
                        pass
        return 0

    def _parse_official_store(self, soup: BeautifulSoup) -> bool:
        """Verifica se é loja oficial do ML."""
        for selector in DETAIL_SELECTORS["official_store"].split(", "):
            tag = soup.select_one(selector.strip())
            if tag:
                return True
        return False
