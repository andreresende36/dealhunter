"""
DealHunter — Módulo de Scraping
Coleta ofertas do Mercado Livre com técnicas de anti-bloqueio.
"""

from .base_scraper import BaseScraper, ScrapedProduct
from .ml_scraper import MLScraper, ScrapeSource
from .product_detail_scraper import ProductDetailScraper, EnrichedProductData

__all__ = [
    "BaseScraper",
    "ScrapedProduct",
    "MLScraper",
    "ScrapeSource",
    "ProductDetailScraper",
    "EnrichedProductData",
]
