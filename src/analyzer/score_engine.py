"""
DealHunter — Score Engine v2
Sistema de pontuacao por regras com redistribuicao dinamica de pesos.
Pontuacao de 0 a 100. Ofertas abaixo do minimo sao descartadas.

Criterios e pesos base (soma = 100):
  - Desconto (%)              -> ate 30 pts  (SCORE_WEIGHT_DISCOUNT)     [sigmoid]
  - Badge                     -> ate 15 pts  (SCORE_WEIGHT_BADGE)        [discreto]
  - Avaliacao (estrelas)      -> ate 15 pts  (SCORE_WEIGHT_RATING)       [linear]
  - N de reviews              -> ate 10 pts  (SCORE_WEIGHT_REVIEWS)      [logaritmico]
  - Frete gratis              -> 10 pts      (SCORE_WEIGHT_FREE_SHIPPING)[binario]
  - Parcelamento sem juros    -> 10 pts      (SCORE_WEIGHT_INSTALLMENTS) [binario]
  - Qualidade do titulo       -> ate 10 pts  (SCORE_WEIGHT_TITLE_QUALITY)[heuristicas]

Redistribuicao dinamica:
  Quando um criterio nao tem dados disponiveis (ex: rating=0, badge=""),
  seu peso e redistribuido proporcionalmente entre os criterios com dados.
  Isso garante que a escala 0-100 se mantenha mesmo com dados parciais.
"""

import math
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

import structlog

from src.config import settings
from src.scraper.base_scraper import ScrapedProduct

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class CriterionScore:
    """Detalhamento de um criterio individual de pontuacao."""

    raw_score: float = 0.0  # Pontuacao bruta (antes da redistribuicao)
    final_score: float = 0.0  # Pontuacao final (apos redistribuicao)
    max_points: float = 0.0  # Peso maximo configurado para este criterio
    available: bool = True  # Se o criterio tinha dados disponiveis


@dataclass
class ScoreBreakdown:
    """Detalhamento da pontuacao por criterio (7 criterios, pesos configuraveis)."""

    discount: CriterionScore = field(default_factory=CriterionScore)
    badge: CriterionScore = field(default_factory=CriterionScore)
    rating: CriterionScore = field(default_factory=CriterionScore)
    reviews: CriterionScore = field(default_factory=CriterionScore)
    free_shipping: CriterionScore = field(default_factory=CriterionScore)
    installments: CriterionScore = field(default_factory=CriterionScore)
    title_quality: CriterionScore = field(default_factory=CriterionScore)

    @property
    def total(self) -> float:
        return (
            self.discount.final_score
            + self.badge.final_score
            + self.rating.final_score
            + self.reviews.final_score
            + self.free_shipping.final_score
            + self.installments.final_score
            + self.title_quality.final_score
        )


@dataclass
class ScoredProduct:
    """Produto com pontuacao calculada."""

    product: ScrapedProduct
    score: float
    breakdown: ScoreBreakdown
    passed: bool  # True se score >= min_score
    reject_reason: Optional[str] = None
    low_confidence: bool = False  # True se < 3 criterios com dados
    available_criteria: int = 7
    redistribution_factor: float = 1.0

    def to_dict(self) -> dict:
        d = self.product.to_dict()
        d.update(
            {
                "score": self.score,
                "score_breakdown": {
                    "discount": self.breakdown.discount.final_score,
                    "badge": self.breakdown.badge.final_score,
                    "rating": self.breakdown.rating.final_score,
                    "reviews": self.breakdown.reviews.final_score,
                    "free_shipping": self.breakdown.free_shipping.final_score,
                    "installments": self.breakdown.installments.final_score,
                    "title_quality": self.breakdown.title_quality.final_score,
                },
                "passed": self.passed,
                "reject_reason": self.reject_reason,
                "low_confidence": self.low_confidence,
                "available_criteria": self.available_criteria,
                "redistribution_factor": self.redistribution_factor,
            }
        )
        return d


# ---------------------------------------------------------------------------
# Score Engine v2
# ---------------------------------------------------------------------------


class ScoreEngine:
    """
    Avalia a qualidade de uma oferta por regras deterministicas
    com redistribuicao dinamica de pesos.

    Uso:
        engine = ScoreEngine()
        scored = engine.evaluate(product)
        if scored.passed:
            # publicar oferta
    """

    # Marcas conhecidas populares no ML BR (lowercase)
    _KNOWN_BRANDS: set[str] = {
        "nike", "adidas", "puma", "reebok", "new balance", "asics",
        "samsung", "apple", "xiaomi", "motorola", "lg", "sony",
        "philips", "electrolux", "brastemp", "consul", "arno",
        "mondial", "tramontina", "havaianas", "reserva", "hering",
        "levis", "colcci", "dumond", "arezzo", "lupo", "olympikus",
        "mizuno", "fila", "vans", "converse", "lacoste", "calvin klein",
        "tommy hilfiger", "polo ralph lauren", "under armour",
    }

    # Proporcao do peso maximo de badge por tipo (chaves normalizadas)
    _BADGE_RATIO: dict[str, float] = {
        "oferta relampago": 1.00,   # 100% do peso
        "oferta imperdivel": 0.50,  # 50% do peso
        "oferta do dia": 0.30,      # 30% do peso
        "mais vendido": 0.10,       # 10% do peso
    }

    def __init__(self) -> None:
        self.cfg = settings.score
        self._validate_weights()

    def _validate_weights(self) -> None:
        """Valida que a soma dos 7 pesos e igual a 100."""
        total = (
            self.cfg.weight_discount
            + self.cfg.weight_badge
            + self.cfg.weight_rating
            + self.cfg.weight_reviews
            + self.cfg.weight_free_shipping
            + self.cfg.weight_installments
            + self.cfg.weight_title_quality
        )
        if abs(total - 100.0) > 0.1:
            logger.warning(
                "score_weights_not_100",
                total=total,
                expected=100.0,
                msg=f"Soma dos pesos = {total}, esperado = 100.0",
            )

    # ------------------------------------------------------------------
    # Interface publica
    # ------------------------------------------------------------------

    def evaluate(self, product: ScrapedProduct) -> ScoredProduct:
        """Calcula o score de um produto e decide se passa no filtro."""
        # 1. Hard filter ANTES do score (eficiencia)
        reject_reason = self._hard_reject(product)
        if reject_reason:
            return ScoredProduct(
                product=product,
                score=0.0,
                breakdown=ScoreBreakdown(),
                passed=False,
                reject_reason=reject_reason,
            )

        # 2. Calcula raw scores e verifica disponibilidade
        breakdown = self._compute_raw_scores(product)

        # 3. Aplica redistribuicao dinamica
        available_weight, available_count, factor = self._redistribute(breakdown)

        # 4. Calcula totais
        score = round(breakdown.total, 1)
        low_confidence = available_count < 3

        # 5. Verifica score minimo
        passed = score >= self.cfg.min_score

        if not passed:
            reject_reason = (
                f"Score {score} abaixo do minimo {self.cfg.min_score}"
            )

        logger.debug(
            "product_scored",
            ml_id=product.ml_id,
            score=score,
            passed=passed,
            available_criteria=available_count,
            redistribution_factor=round(factor, 3),
            low_confidence=low_confidence,
            reject_reason=reject_reason,
        )

        return ScoredProduct(
            product=product,
            score=score,
            breakdown=breakdown,
            passed=passed,
            reject_reason=reject_reason,
            low_confidence=low_confidence,
            available_criteria=available_count,
            redistribution_factor=round(factor, 3),
        )

    def evaluate_batch(
        self, products: list[ScrapedProduct]
    ) -> list[ScoredProduct]:
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
    # Calculo de raw scores
    # ------------------------------------------------------------------

    def _compute_raw_scores(self, product: ScrapedProduct) -> ScoreBreakdown:
        """Calcula raw scores de todos os 7 criterios e marca disponibilidade."""
        breakdown = ScoreBreakdown()

        # 1. Desconto (sigmoid)
        has_discount = product.discount_pct > 0 or (
            product.original_price is not None and product.original_price > product.price
        )
        breakdown.discount = CriterionScore(
            raw_score=self._score_discount(product.discount_pct),
            max_points=self.cfg.weight_discount,
            available=has_discount,
        )

        # 2. Badge (discreto)
        has_badge = bool(product.badge and product.badge.strip())
        breakdown.badge = CriterionScore(
            raw_score=self._score_badge(product.badge),
            max_points=self.cfg.weight_badge,
            available=has_badge,
        )

        # 3. Rating (linear)
        has_rating = product.rating > 0
        breakdown.rating = CriterionScore(
            raw_score=self._score_rating(product.rating),
            max_points=self.cfg.weight_rating,
            available=has_rating,
        )

        # 4. Reviews (logaritmico)
        has_reviews = product.review_count > 0
        breakdown.reviews = CriterionScore(
            raw_score=self._score_reviews(product.review_count),
            max_points=self.cfg.weight_reviews,
            available=has_reviews,
        )

        # 5. Frete gratis (binario — sempre disponivel)
        breakdown.free_shipping = CriterionScore(
            raw_score=(
                self.cfg.weight_free_shipping if product.free_shipping else 0.0
            ),
            max_points=self.cfg.weight_free_shipping,
            available=True,
        )

        # 6. Parcelamento sem juros (binario — sempre disponivel)
        breakdown.installments = CriterionScore(
            raw_score=(
                self.cfg.weight_installments
                if product.installments_without_interest
                else 0.0
            ),
            max_points=self.cfg.weight_installments,
            available=True,
        )

        # 7. Qualidade do titulo (heuristicas — sempre disponivel)
        breakdown.title_quality = CriterionScore(
            raw_score=self._score_title(product.title),
            max_points=self.cfg.weight_title_quality,
            available=True,
        )

        return breakdown

    # ------------------------------------------------------------------
    # Redistribuicao dinamica
    # ------------------------------------------------------------------

    def _redistribute(
        self, breakdown: ScoreBreakdown
    ) -> tuple[float, int, float]:
        """
        Redistribui pesos dos criterios indisponiveis para os disponiveis.

        Retorna (available_weight, available_count, factor).
        """
        criteria = [
            breakdown.discount,
            breakdown.badge,
            breakdown.rating,
            breakdown.reviews,
            breakdown.free_shipping,
            breakdown.installments,
            breakdown.title_quality,
        ]

        available_weight = sum(c.max_points for c in criteria if c.available)
        available_count = sum(1 for c in criteria if c.available)

        # Evita divisao por zero (todos indisponiveis — cenario teorico)
        if available_weight <= 0:
            factor = 1.0
        else:
            factor = 100.0 / available_weight

        for criterion in criteria:
            if criterion.available:
                criterion.final_score = round(criterion.raw_score * factor, 1)
            else:
                criterion.final_score = 0.0

        return available_weight, available_count, factor

    # ------------------------------------------------------------------
    # Funcoes de scoring individuais
    # ------------------------------------------------------------------

    def _score_discount(self, pct: float) -> float:
        """
        Curva sigmoid para desconto.

        Formula: W / (1 + e^(-0.12*(d-35))) - baseline
        Cap em 80% de desconto. Baseline subtrai o valor em d=0 para
        garantir que _score_discount(0) = 0.
        """
        max_pts = self.cfg.weight_discount
        if pct <= 0:
            return 0.0

        # Cap em 80%
        d = min(pct, 80.0)

        # Sigmoid centrada em 35% de desconto
        raw = max_pts / (1.0 + math.exp(-0.12 * (d - 35.0)))
        baseline = max_pts / (1.0 + math.exp(-0.12 * (0.0 - 35.0)))
        score = raw - baseline

        return round(min(max(score, 0.0), max_pts), 1)

    def _score_badge(self, badge: str) -> float:
        """Pontuacao por badge do ML com normalizacao de acentos e case."""
        if not badge or not badge.strip():
            return 0.0
        normalized = self._remove_accents(badge.strip().lower())
        ratio = self._BADGE_RATIO.get(normalized, 0.0)
        return round(ratio * self.cfg.weight_badge, 1)

    def _score_rating(self, rating: float) -> float:
        """Escala linear: <3.5 -> 0pts, 5.0 -> max pts."""
        max_pts = self.cfg.weight_rating
        if rating < 3.5:
            return 0.0
        return round(min((rating - 3.5) / 1.5 * max_pts, max_pts), 1)

    def _score_reviews(self, count: int) -> float:
        """
        Curva logaritmica para reviews.

        Formula: min(W, log10(count) / log10(5000) * W)
        Satura em 5000 reviews.
        """
        max_pts = self.cfg.weight_reviews
        if count <= 0:
            return 0.0
        if count >= 5000:
            return max_pts
        score = math.log10(count) / math.log10(5000) * max_pts
        return round(min(score, max_pts), 1)

    def _score_title(self, title: str) -> float:
        """
        Avalia qualidade do titulo com heuristicas multiplas.

        Sub-criterios (soma ate 10 pontos internos, escalado pelo peso):
          - Comprimento adequado (20-80 chars): +3 pts
          - Contem marca conhecida:             +2 pts
          - Nao e todo CAPS:                    +2 pts
          - Nao contem spam/lixo:               +2 pts
          - Contem especificacoes tecnicas:      +1 pt
        Total maximo interno: 10 pts, escalado por (weight / 10)
        """
        max_pts = self.cfg.weight_title_quality
        if not title:
            return 0.0

        internal_score = 0.0
        scale = max_pts / 10.0

        # 1. Comprimento adequado (+3)
        length = len(title)
        if 20 <= length <= 80:
            internal_score += 3.0
        elif 10 <= length < 20 or 80 < length <= 120:
            internal_score += 1.5  # Parcial para faixas proximas

        # 2. Contem marca conhecida (+2)
        title_lower = title.lower()
        if any(brand in title_lower for brand in self._KNOWN_BRANDS):
            internal_score += 2.0

        # 3. Nao e todo CAPS (+2)
        # Penaliza se >50% das letras sao maiusculas
        alpha_chars = [c for c in title if c.isalpha()]
        if alpha_chars:
            upper_ratio = sum(1 for c in alpha_chars if c.isupper()) / len(
                alpha_chars
            )
            if upper_ratio < 0.5:
                internal_score += 2.0

        # 4. Nao contem spam/lixo (+2)
        spam_patterns = [
            r"!!!+",
            r"\?\?\?+",
            r"compre\s+j[aá]",
            r"corra",
            r"[uú]ltimas?\s+unidades?",
            r"imperd[ií]vel",
            r"aproveite",
            r"\bfake\b",
        ]
        has_spam = any(
            re.search(p, title, re.IGNORECASE) for p in spam_patterns
        )
        if not has_spam:
            internal_score += 2.0

        # 5. Contem especificacoes tecnicas (+1)
        spec_patterns = [
            r"\b\d+(cm|mm|ml|l|kg|g|GB|TB|Mb|W|mAh|V)\b",
            r"\b(original|oficial|importado|novo)\b",
            r"\b(kit|conjunto|par|pacote|cx)\b",
        ]
        has_specs = any(
            re.search(p, title, re.IGNORECASE) for p in spec_patterns
        )
        if has_specs:
            internal_score += 1.0

        return round(min(internal_score * scale, max_pts), 1)

    # ------------------------------------------------------------------
    # Hard filters — eliminacao imediata
    # ------------------------------------------------------------------

    def _hard_reject(self, product: ScrapedProduct) -> Optional[str]:
        """
        Criterios que eliminam um produto independente da pontuacao.
        Executado ANTES do calculo de scores para eficiencia.
        Retorna mensagem de rejeicao ou None se passou.
        """
        if product.discount_pct < self.cfg.min_discount_pct:
            return (
                f"Desconto {product.discount_pct:.0f}% abaixo do "
                f"minimo {self.cfg.min_discount_pct:.0f}%"
            )

        if product.rating > 0 and product.rating < self.cfg.min_rating:
            return (
                f"Avaliacao {product.rating} abaixo do "
                f"minimo {self.cfg.min_rating}"
            )

        if (
            product.review_count > 0
            and product.review_count < self.cfg.min_reviews
        ):
            return (
                f"Apenas {product.review_count} avaliacoes "
                f"(minimo {self.cfg.min_reviews})"
            )

        if product.price <= 0:
            return "Preco invalido"

        if len(product.title) < 10:
            return "Titulo muito curto"

        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _remove_accents(text: str) -> str:
        """Remove acentos de uma string para normalizacao."""
        nfkd = unicodedata.normalize("NFKD", text)
        return "".join(c for c in nfkd if not unicodedata.combining(c))
