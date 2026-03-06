"""
DealHunter — Score Engine
Sistema de pontuação por regras para filtrar e priorizar ofertas.
Pontuação de 0 a 100. Ofertas abaixo do mínimo são descartadas.

Critérios e pesos (soma = 100):
  - Desconto (%)              → até 40 pts  (SCORE_WEIGHT_DISCOUNT)
  - Avaliação (estrelas)      → até 25 pts  (SCORE_WEIGHT_RATING)
  - Nº de reviews             → até 15 pts  (SCORE_WEIGHT_REVIEWS)
  - Frete grátis              → 10 pts      (SCORE_WEIGHT_FREE_SHIPPING)
  - Qualidade do título       → até 10 pts  (SCORE_WEIGHT_TITLE_QUALITY)
"""

import re
from dataclasses import dataclass
from typing import Optional

import structlog

from src.config import settings
from src.scraper.base_scraper import ScrapedProduct

logger = structlog.get_logger(__name__)


@dataclass
class ScoreBreakdown:
    """Detalhamento da pontuação por critério (pesos configuráveis via .env)."""

    discount: float = 0.0
    rating: float = 0.0
    free_shipping: float = 0.0
    reviews: float = 0.0
    title_quality: float = 0.0

    @property
    def total(self) -> float:
        return (
            self.discount
            + self.rating
            + self.free_shipping
            + self.reviews
            + self.title_quality
        )


@dataclass
class ScoredProduct:
    """Produto com pontuação calculada."""

    product: ScrapedProduct
    score: float
    breakdown: ScoreBreakdown
    passed: bool  # True se score >= min_score
    reject_reason: Optional[str] = None

    def to_dict(self) -> dict:
        d = self.product.to_dict()
        d.update(
            {
                "score": self.score,
                "score_breakdown": {
                    "discount": self.breakdown.discount,
                    "rating": self.breakdown.rating,
                    "free_shipping": self.breakdown.free_shipping,
                    "reviews": self.breakdown.reviews,
                    "title_quality": self.breakdown.title_quality,
                },
                "passed": self.passed,
                "reject_reason": self.reject_reason,
            }
        )
        return d


class ScoreEngine:
    """
    Avalia a qualidade de uma oferta por regras determinísticas.

    Uso:
        engine = ScoreEngine()
        scored = engine.evaluate(product)
        if scored.passed:
            # publicar oferta
    """

    def __init__(self):
        self.cfg = settings.score

    def evaluate(self, product: ScrapedProduct) -> ScoredProduct:
        """Calcula o score de um produto e decide se passa no filtro."""
        breakdown = ScoreBreakdown()

        # 1. Pontuação por desconto (40 pts máx)
        breakdown.discount = self._score_discount(product.discount_pct)

        # 2. Pontuação por avaliação (25 pts máx)
        breakdown.rating = self._score_rating(product.rating)

        # 3. Pontuação por número de reviews (15 pts máx)
        breakdown.reviews = self._score_reviews(product.review_count)

        # 4. Bônus frete grátis (10 pts)
        breakdown.free_shipping = (
            self.cfg.weight_free_shipping if product.free_shipping else 0.0
        )

        # 5. Qualidade do título (10 pts máx)
        breakdown.title_quality = self._score_title(product.title)

        score = round(breakdown.total, 1)

        # Verifica critérios de eliminação direta (hard filters)
        reject_reason = self._hard_reject(product)
        passed = reject_reason is None and score >= self.cfg.min_score

        if not passed and not reject_reason:
            reject_reason = f"Score {score} abaixo do mínimo {self.cfg.min_score}"

        logger.debug(
            "product_scored",
            ml_id=product.ml_id,
            score=score,
            passed=passed,
            reject_reason=reject_reason,
        )

        return ScoredProduct(
            product=product,
            score=score,
            breakdown=breakdown,
            passed=passed,
            reject_reason=reject_reason,
        )

    def evaluate_batch(self, products: list[ScrapedProduct]) -> list[ScoredProduct]:
        """Avalia uma lista de produtos e retorna apenas os aprovados, ordenados por score."""
        scored = [self.evaluate(p) for p in products]
        approved = [s for s in scored if s.passed]
        approved.sort(key=lambda s: s.score, reverse=True)

        logger.info(
            "batch_evaluated",
            total=len(products),
            approved=len(approved),
            rejected=len(products) - len(approved),
        )
        return approved

    # ------------------------------------------------------------------
    # Critérios de pontuação
    # ------------------------------------------------------------------

    def _score_discount(self, pct: float) -> float:
        """Escala linear: 0% → 0pts, 80%+ → max pts."""
        max_pts = self.cfg.weight_discount
        if pct <= 0:
            return 0.0
        if pct >= 80:
            return max_pts
        return round(min(pct / 80 * max_pts, max_pts), 1)

    def _score_rating(self, rating: float) -> float:
        """Escala linear: <3.5 → 0pts, 5.0 → max pts."""
        max_pts = self.cfg.weight_rating
        if rating < 3.5:
            return 0.0
        return round(min((rating - 3.5) / 1.5 * max_pts, max_pts), 1)

    def _score_reviews(self, count: int) -> float:
        """Escala linear: 0 → 0pts, 200+ → max pts."""
        max_pts = self.cfg.weight_reviews
        if count <= 0:
            return 0.0
        if count >= 200:
            return max_pts
        return round(min(count / 200 * max_pts, max_pts), 1)

    def _score_title(self, title: str) -> float:
        """
        Avalia qualidade do título:
        - Comprimento adequado (20-80 chars): metade dos pts
        - Contém informações úteis (marca, tamanho, cor): outra metade
        """
        max_pts = self.cfg.weight_title_quality
        half = max_pts / 2
        score = 0.0
        if not title:
            return 0.0

        # Comprimento ideal
        if 20 <= len(title) <= 80:
            score += half

        # Contém keywords úteis (marca, tamanho, especificação)
        useful_keywords = [
            r"\b\d+(cm|mm|ml|l|kg|g|GB|TB|Mb|W)\b",  # medidas
            r"\b(original|oficial|importado|novo)\b",
            r"\b(kit|conjunto|par)\b",
        ]

        for pattern in useful_keywords:
            if re.search(pattern, title, re.IGNORECASE):
                score += half / 2
                break

        return round(min(score, max_pts), 1)

    # ------------------------------------------------------------------
    # Hard filters — eliminação imediata
    # ------------------------------------------------------------------

    def _hard_reject(self, product: ScrapedProduct) -> Optional[str]:
        """
        Critérios que eliminam um produto independente da pontuação.
        Retorna mensagem de rejeição ou None se passou.
        """
        if product.discount_pct < self.cfg.min_discount_pct:
            return (
                f"Desconto {product.discount_pct:.0f}% abaixo do "
                f"mínimo {self.cfg.min_discount_pct:.0f}%"
            )

        if product.rating > 0 and product.rating < self.cfg.min_rating:
            return f"Avaliação {product.rating} abaixo do mínimo {self.cfg.min_rating}"

        if product.review_count > 0 and product.review_count < self.cfg.min_reviews:
            return f"Apenas {product.review_count} avaliações (mínimo {self.cfg.min_reviews})"

        if product.price <= 0:
            return "Preço inválido"

        if len(product.title) < 10:
            return "Título muito curto"

        return None
