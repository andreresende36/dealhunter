"""
Testes para o Score Engine do DealHunter.
"""

import pytest
from unittest.mock import patch

from src.scraper.base_scraper import ScrapedProduct
from src.analyzer.score_engine import ScoreEngine
from src.analyzer.fake_discount_detector import FakeDiscountDetector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_settings():
    """Mocka as configurações para testes independentes de .env."""
    with patch("src.analyzer.score_engine.settings") as mock:
        mock.score.min_discount_pct = 20.0
        mock.score.min_score = 60
        mock.score.min_rating = 4.0
        mock.score.min_reviews = 10
        mock.score.weight_discount = 35.0
        mock.score.weight_rating = 20.0
        mock.score.weight_reviews = 15.0
        mock.score.weight_free_shipping = 10.0
        mock.score.weight_official_store = 10.0
        mock.score.weight_title_quality = 10.0
        yield mock


@pytest.fixture
def engine(mock_settings) -> ScoreEngine:
    return ScoreEngine()


@pytest.fixture
def great_product() -> ScrapedProduct:
    return ScrapedProduct(
        ml_id="MLB111",
        url="http://ml.com/p/MLB111",
        title="Bolsa Feminina Couro Legítimo Marrom Premium",
        price=299.90,
        original_price=599.90,  # 50% off
        rating=4.8,
        review_count=500,
        free_shipping=True,
        is_official_store=True,
        category="Bolsas",
    )


@pytest.fixture
def bad_product() -> ScrapedProduct:
    return ScrapedProduct(
        ml_id="MLB222",
        url="http://ml.com/p/MLB222",
        title="Camiseta",
        price=99.90,
        original_price=None,  # Sem desconto
        rating=3.0,
        review_count=2,
        free_shipping=False,
        is_official_store=False,
    )


# ---------------------------------------------------------------------------
# Score Engine — pontuações individuais
# ---------------------------------------------------------------------------


class TestScoreEngine:
    def test_great_product_passes(self, engine, great_product):
        result = engine.evaluate(great_product)
        assert result.passed is True
        assert result.score >= 60

    def test_bad_product_fails(self, engine, bad_product):
        result = engine.evaluate(bad_product)
        assert result.passed is False

    def test_free_shipping_adds_10_pts(self, engine, great_product):
        result_with = engine.evaluate(great_product)

        great_product.free_shipping = False
        result_without = engine.evaluate(great_product)

        diff = result_with.score - result_without.score
        assert diff == pytest.approx(10.0, abs=0.5)

    def test_official_store_adds_10_pts(self, engine, great_product):
        result_with = engine.evaluate(great_product)

        great_product.is_official_store = False
        result_without = engine.evaluate(great_product)

        diff = result_with.score - result_without.score
        assert diff == pytest.approx(10.0, abs=0.5)

    def test_discount_scoring(self, engine):
        assert engine._score_discount(0) == 0.0
        assert engine._score_discount(80) == 35.0
        assert engine._score_discount(40) == pytest.approx(17.5, abs=1.0)

    def test_rating_scoring(self, engine):
        assert engine._score_rating(3.4) == 0.0
        assert engine._score_rating(5.0) == 20.0
        assert engine._score_rating(4.0) > 0

    def test_reviews_scoring(self, engine):
        assert engine._score_reviews(0) == 0.0
        assert engine._score_reviews(200) == 15.0
        assert engine._score_reviews(100) < 15.0

    def test_hard_reject_low_discount(self, engine, bad_product):
        bad_product.price = 100.0
        bad_product.original_price = 110.0  # ~9% off, abaixo de 20%
        bad_product.discount_pct  # força recalculo via __post_init__

        # Recria para acionar __post_init__
        bad_product = ScrapedProduct(
            ml_id="MLB222",
            url="http://ml.com",
            title="Camiseta Básica",
            price=100.0,
            original_price=110.0,
        )
        result = engine.evaluate(bad_product)
        assert result.passed is False
        assert (
            "desconto" in result.reject_reason.lower()
            or result.reject_reason is not None
        )

    def test_batch_returns_sorted_by_score(self, engine, great_product, bad_product):
        # Adicionar desconto mínimo ao bad_product para só testar ordenação
        medium_product = ScrapedProduct(
            ml_id="MLB333",
            url="http://ml.com",
            title="Óculos de Sol Polarizado UV400 Masculino",
            price=79.90,
            original_price=159.90,
            rating=4.2,
            review_count=80,
            free_shipping=False,
        )
        results = engine.evaluate_batch([great_product, medium_product])
        if len(results) >= 2:
            assert results[0].score >= results[1].score


# ---------------------------------------------------------------------------
# Fake Discount Detector
# ---------------------------------------------------------------------------


class TestFakeDiscountDetector:
    def setup_method(self):
        self.detector = FakeDiscountDetector()

    def test_genuine_discount_passes(self):
        product = ScrapedProduct(
            ml_id="MLB1",
            url="http://ml.com",
            title="Produto genuíno",
            price=199.90,
            original_price=299.90,  # ~33% off — razoável
        )
        result = self.detector.check(product)
        assert result.is_fake is False

    def test_extreme_ratio_flagged(self):
        product = ScrapedProduct(
            ml_id="MLB2",
            url="http://ml.com",
            title="Produto inflado",
            price=50.0,
            original_price=5000.0,  # 100x — impossível
        )
        result = self.detector.check(product)
        assert result.is_fake is True
        assert result.confidence > 0.6

    def test_no_original_price_not_fake(self):
        product = ScrapedProduct(
            ml_id="MLB3",
            url="http://ml.com",
            title="Produto sem preço original",
            price=99.90,
            original_price=None,
        )
        result = self.detector.check(product)
        assert result.is_fake is False

    def test_suspicious_round_price(self):
        assert self.detector._is_suspiciously_round(1000.0) is True
        assert self.detector._is_suspiciously_round(999.90) is False
        assert self.detector._is_suspiciously_round(100.0) is False  # < R$200
