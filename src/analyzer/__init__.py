"""
DealHunter — Modulo de Analise de Ofertas
Avalia a qualidade das ofertas via regras deterministicas.
"""

from .score_engine import (
    CriterionScore,
    ScoreBreakdown,
    ScoreEngine,
    ScoredProduct,
)
from .fake_discount_detector import FakeDiscountDetector

__all__ = [
    "CriterionScore",
    "ScoreBreakdown",
    "ScoreEngine",
    "ScoredProduct",
    "FakeDiscountDetector",
]
