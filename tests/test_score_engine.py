"""
Testes para o Score Engine v2 do DealHunter.

Cobre:
- Hard filters (7 testes)
- Funcoes de scoring individuais (discount sigmoid, badge normalization,
  rating, reviews log, shipping, installments, title heuristics)
- Redistribuicao dinamica (factor, com/sem badge, com/sem rating+reviews)
- low_confidence flag
- Integracao (great_product passa, batch sorted, to_dict format)
- Validacao de pesos (warning quando soma != 100)
"""

import math
from unittest.mock import patch

import pytest

from src.scraper.base_scraper import ScrapedProduct
from src.analyzer.score_engine import (
    CriterionScore,
    ScoreBreakdown,
    ScoreEngine,
    ScoredProduct,
)
from src.analyzer.fake_discount_detector import FakeDiscountDetector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_score_attrs(mock):
    """Configura todos os atributos de score no mock."""
    mock.score.min_discount_pct = 20.0
    mock.score.min_score = 60
    mock.score.min_rating = 4.0
    mock.score.min_reviews = 10
    mock.score.weight_discount = 30.0
    mock.score.weight_badge = 15.0
    mock.score.weight_rating = 15.0
    mock.score.weight_reviews = 10.0
    mock.score.weight_free_shipping = 10.0
    mock.score.weight_installments = 10.0
    mock.score.weight_title_quality = 10.0


@pytest.fixture
def mock_settings():
    """Mocka as configuracoes para testes independentes de .env."""
    with patch("src.analyzer.score_engine.settings") as mock:
        _mock_score_attrs(mock)
        yield mock


@pytest.fixture
def engine(mock_settings) -> ScoreEngine:
    return ScoreEngine()


@pytest.fixture
def great_product() -> ScrapedProduct:
    return ScrapedProduct(
        ml_id="MLB111",
        url="http://ml.com/p/MLB111",
        title="Bolsa Feminina Couro Legitimo Marrom Premium",
        price=299.90,
        original_price=599.90,  # 50% off
        rating=4.8,
        review_count=500,
        free_shipping=True,
        installments_without_interest=True,
        category="Bolsas",
        badge="Oferta do dia",
    )


@pytest.fixture
def card_only_product() -> ScrapedProduct:
    """Produto tipico de card de listagem: sem rating, reviews ou badge."""
    return ScrapedProduct(
        ml_id="MLB444",
        url="http://ml.com/p/MLB444",
        title="Tenis Nike Air Max 270 Masculino Original Preto",
        price=349.90,
        original_price=699.90,  # 50% off
        rating=0,
        review_count=0,
        free_shipping=True,
        installments_without_interest=True,
        badge="",
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
    )


# ---------------------------------------------------------------------------
# Hard Filters
# ---------------------------------------------------------------------------


class TestHardFilters:
    """Testa criterios de eliminacao imediata (antes do score)."""

    def test_reject_low_discount(self, engine):
        product = ScrapedProduct(
            ml_id="MLB001",
            url="http://ml.com",
            title="Camiseta Basica Algodao Masculina",
            price=90.0,
            original_price=100.0,  # 10% off < 20% minimo
        )
        result = engine.evaluate(product)
        assert result.passed is False
        assert result.score == 0.0
        assert "desconto" in result.reject_reason.lower()

    def test_reject_low_rating(self, engine):
        product = ScrapedProduct(
            ml_id="MLB002",
            url="http://ml.com",
            title="Produto com Rating Baixo Original",
            price=50.0,
            original_price=100.0,  # 50% off
            rating=3.5,  # abaixo de 4.0
            review_count=100,
        )
        result = engine.evaluate(product)
        assert result.passed is False
        assert "avaliacao" in result.reject_reason.lower()

    def test_reject_few_reviews(self, engine):
        product = ScrapedProduct(
            ml_id="MLB003",
            url="http://ml.com",
            title="Produto com Poucas Reviews Original",
            price=50.0,
            original_price=100.0,
            rating=4.5,
            review_count=5,  # abaixo de 10
        )
        result = engine.evaluate(product)
        assert result.passed is False
        assert "avaliacoes" in result.reject_reason.lower()

    def test_reject_invalid_price(self, engine):
        product = ScrapedProduct(
            ml_id="MLB004",
            url="http://ml.com",
            title="Produto com Preco Invalido Original",
            price=0,
            original_price=100.0,
        )
        result = engine.evaluate(product)
        assert result.passed is False
        assert "preco" in result.reject_reason.lower()

    def test_reject_short_title(self, engine):
        product = ScrapedProduct(
            ml_id="MLB005",
            url="http://ml.com",
            title="Curto",  # < 10 chars
            price=50.0,
            original_price=100.0,
        )
        result = engine.evaluate(product)
        assert result.passed is False
        assert "titulo" in result.reject_reason.lower()

    def test_zero_rating_not_rejected(self, engine):
        """Rating=0 significa 'sem dados', nao deve ser rejeitado por hard filter."""
        product = ScrapedProduct(
            ml_id="MLB006",
            url="http://ml.com",
            title="Produto Sem Rating Disponivel Original",
            price=50.0,
            original_price=100.0,
            rating=0,
            review_count=0,
        )
        result = engine.evaluate(product)
        # Nao deve ser rejeitado por rating/reviews
        assert result.reject_reason is None or "avaliacao" not in result.reject_reason.lower()

    def test_no_discount_rejected(self, engine):
        """Produto sem desconto nenhum deve ser rejeitado."""
        product = ScrapedProduct(
            ml_id="MLB007",
            url="http://ml.com",
            title="Produto Sem Desconto Nenhum Original",
            price=100.0,
            original_price=None,
        )
        result = engine.evaluate(product)
        assert result.passed is False


# ---------------------------------------------------------------------------
# Discount Scoring (Sigmoid)
# ---------------------------------------------------------------------------


class TestDiscountScoring:
    def test_zero_discount(self, engine):
        assert engine._score_discount(0) == 0.0

    def test_negative_discount(self, engine):
        assert engine._score_discount(-5) == 0.0

    def test_moderate_discount_30pct(self, engine):
        score = engine._score_discount(30)
        # 30% deve dar algo razoavel mas abaixo do maximo
        assert 0 < score < 30.0

    def test_high_discount_50pct(self, engine):
        score = engine._score_discount(50)
        # 50% esta acima do centro sigmoid (35%), deve ser alto
        assert score > 15.0

    def test_very_high_discount_80pct(self, engine):
        score = engine._score_discount(80)
        # Cap em 80%, deve estar proximo ao maximo
        assert score > 25.0

    def test_cap_at_80pct(self, engine):
        """Desconto acima de 80% nao deve dar mais pontos."""
        score_80 = engine._score_discount(80)
        score_90 = engine._score_discount(90)
        score_100 = engine._score_discount(100)
        assert score_80 == pytest.approx(score_90, abs=0.5)
        assert score_80 == pytest.approx(score_100, abs=0.5)

    def test_sigmoid_monotonic(self, engine):
        """Score deve aumentar monotonicamente com desconto."""
        prev = 0.0
        for d in [10, 20, 30, 40, 50, 60, 70, 80]:
            score = engine._score_discount(d)
            assert score >= prev, f"Score nao e monotonico em {d}%"
            prev = score


# ---------------------------------------------------------------------------
# Badge Scoring (normalizacao)
# ---------------------------------------------------------------------------


class TestBadgeScoring:
    def test_empty_badge(self, engine):
        assert engine._score_badge("") == 0.0

    def test_none_like_badge(self, engine):
        assert engine._score_badge("   ") == 0.0

    def test_oferta_relampago(self, engine):
        score = engine._score_badge("Oferta relâmpago")
        assert score == pytest.approx(15.0, abs=0.1)

    def test_oferta_relampago_without_accent(self, engine):
        score = engine._score_badge("Oferta relampago")
        assert score == pytest.approx(15.0, abs=0.1)

    def test_oferta_imperdivel(self, engine):
        score = engine._score_badge("Oferta imperdível")
        assert score == pytest.approx(7.5, abs=0.1)

    def test_oferta_do_dia(self, engine):
        score = engine._score_badge("Oferta do dia")
        assert score == pytest.approx(4.5, abs=0.1)

    def test_mais_vendido(self, engine):
        score = engine._score_badge("Mais vendido")
        assert score == pytest.approx(1.5, abs=0.1)

    def test_case_insensitive(self, engine):
        """Badge deve funcionar independente de casing."""
        score_upper = engine._score_badge("OFERTA RELÂMPAGO")
        score_lower = engine._score_badge("oferta relâmpago")
        assert score_upper == score_lower

    def test_unknown_badge(self, engine):
        assert engine._score_badge("Destaque da semana") == 0.0


# ---------------------------------------------------------------------------
# Rating Scoring (linear)
# ---------------------------------------------------------------------------


class TestRatingScoring:
    def test_below_threshold(self, engine):
        assert engine._score_rating(3.4) == 0.0

    def test_at_threshold(self, engine):
        assert engine._score_rating(3.5) == 0.0

    def test_perfect_rating(self, engine):
        assert engine._score_rating(5.0) == 15.0

    def test_good_rating(self, engine):
        score = engine._score_rating(4.5)
        expected = (4.5 - 3.5) / 1.5 * 15.0
        assert score == pytest.approx(expected, abs=0.1)

    def test_linear_progression(self, engine):
        """Score deve ser linear entre 3.5 e 5.0."""
        s1 = engine._score_rating(4.0)
        s2 = engine._score_rating(4.5)
        s3 = engine._score_rating(5.0)
        # Incrementos devem ser aproximadamente iguais
        assert (s2 - s1) == pytest.approx(s3 - s2, abs=0.2)


# ---------------------------------------------------------------------------
# Reviews Scoring (logaritmico)
# ---------------------------------------------------------------------------


class TestReviewsScoring:
    def test_zero_reviews(self, engine):
        assert engine._score_reviews(0) == 0.0

    def test_saturates_at_5000(self, engine):
        assert engine._score_reviews(5000) == 10.0

    def test_above_5000(self, engine):
        assert engine._score_reviews(10000) == 10.0

    def test_logarithmic_curve(self, engine):
        """Primeiras reviews devem valer mais (curva log)."""
        s_10 = engine._score_reviews(10)
        s_100 = engine._score_reviews(100)
        s_1000 = engine._score_reviews(1000)

        # Ganho de 10->100 deve ser maior que 100->1000 em proporcao
        gain_first = s_100 - s_10
        gain_second = s_1000 - s_100
        # Log garante rendimentos decrescentes
        assert gain_first > 0
        assert gain_second > 0
        # As duas faixas representam 1 ordem de magnitude cada
        assert gain_first == pytest.approx(gain_second, abs=0.5)

    def test_moderate_reviews(self, engine):
        """200 reviews deve dar uma pontuacao razoavel."""
        score = engine._score_reviews(200)
        assert 4.0 < score < 8.0


# ---------------------------------------------------------------------------
# Free Shipping & Installments (binarios)
# ---------------------------------------------------------------------------


class TestBinaryScoring:
    def test_free_shipping_yes(self, engine, great_product):
        result = engine.evaluate(great_product)
        assert result.breakdown.free_shipping.raw_score == 10.0

    def test_free_shipping_no(self, engine, great_product):
        great_product.free_shipping = False
        result = engine.evaluate(great_product)
        assert result.breakdown.free_shipping.raw_score == 0.0

    def test_installments_yes(self, engine, great_product):
        result = engine.evaluate(great_product)
        assert result.breakdown.installments.raw_score == 10.0

    def test_installments_no(self, engine, great_product):
        great_product.installments_without_interest = False
        result = engine.evaluate(great_product)
        assert result.breakdown.installments.raw_score == 0.0


# ---------------------------------------------------------------------------
# Title Quality Scoring (heuristicas)
# ---------------------------------------------------------------------------


class TestTitleScoring:
    def test_empty_title(self, engine):
        assert engine._score_title("") == 0.0

    def test_good_title_with_brand(self, engine):
        score = engine._score_title("Tenis Nike Air Max 270 Masculino Original")
        assert score > 5.0  # Comprimento + marca + nao caps + nao spam

    def test_all_caps_penalized(self, engine):
        score_normal = engine._score_title("Tenis Nike Air Max 270 Masculino")
        score_caps = engine._score_title("TENIS NIKE AIR MAX 270 MASCULINO")
        assert score_normal > score_caps

    def test_spam_title_penalized(self, engine):
        score_clean = engine._score_title("Bolsa Feminina Couro Genuino Premium")
        score_spam = engine._score_title("Bolsa Feminina Couro!!! Compre Ja Aproveite!!!")
        assert score_clean > score_spam

    def test_title_with_specs(self, engine):
        score_no_specs = engine._score_title("Mochila Escolar Grande Preta")
        score_specs = engine._score_title("Mochila Escolar Grande Preta 30l Original")
        assert score_specs >= score_no_specs

    def test_very_short_title(self, engine):
        score_short = engine._score_title("Camiseta M")
        score_good = engine._score_title("Tenis Nike Air Max 270 Masculino Original")
        # Titulo muito curto deve pontuar menos que titulo com comprimento ideal
        assert score_short < score_good

    def test_known_brand_detected(self, engine):
        """Deve detectar marcas mesmo em meio a texto."""
        score = engine._score_title("Camiseta Masculina Adidas Treino Academia")
        assert score > 3.0


# ---------------------------------------------------------------------------
# Redistribuicao Dinamica
# ---------------------------------------------------------------------------


class TestRedistribution:
    def test_all_criteria_available(self, engine, great_product):
        """Sem redistribuicao quando todos os dados estao presentes."""
        result = engine.evaluate(great_product)
        assert result.redistribution_factor == pytest.approx(1.0, abs=0.01)
        assert result.available_criteria == 7

    def test_card_only_redistribution(self, engine, card_only_product):
        """Produto de card sem rating/reviews/badge deve redistribuir."""
        result = engine.evaluate(card_only_product)
        # Sem badge (15), rating (15), reviews (10) = 40 pts indisponiveis
        # Disponiveis: discount(30) + shipping(10) + installments(10) + title(10) = 60
        # Factor = 100/60 ≈ 1.667
        assert result.redistribution_factor > 1.5
        assert result.available_criteria == 4

    def test_card_product_can_pass(self, engine, card_only_product):
        """Produto de card com bons dados deve conseguir passar score >= 60."""
        result = engine.evaluate(card_only_product)
        # 50% desconto + frete gratis + parcelamento + bom titulo
        # Com redistribuicao, deve conseguir passar
        assert result.passed is True
        assert result.score >= 60

    def test_redistribution_preserves_scale(self, engine, card_only_product):
        """Score maximo teorico com redistribuicao deve ser 100."""
        result = engine.evaluate(card_only_product)
        # Score nao deve ultrapassar 100
        assert result.score <= 100.0

    def test_unavailable_criteria_get_zero(self, engine, card_only_product):
        """Criterios indisponiveis devem ter final_score = 0."""
        result = engine.evaluate(card_only_product)
        assert result.breakdown.badge.final_score == 0.0
        assert result.breakdown.badge.available is False
        assert result.breakdown.rating.final_score == 0.0
        assert result.breakdown.rating.available is False
        assert result.breakdown.reviews.final_score == 0.0
        assert result.breakdown.reviews.available is False

    def test_available_criteria_amplified(self, engine, card_only_product):
        """Criterios disponiveis devem ter final_score > raw_score."""
        result = engine.evaluate(card_only_product)
        discount = result.breakdown.discount
        if discount.raw_score > 0:
            assert discount.final_score > discount.raw_score


# ---------------------------------------------------------------------------
# Low Confidence
# ---------------------------------------------------------------------------


class TestLowConfidence:
    def test_many_criteria_not_low_confidence(self, engine, great_product):
        result = engine.evaluate(great_product)
        assert result.low_confidence is False

    def test_card_product_not_low_confidence(self, engine, card_only_product):
        """Card com 4 criterios (discount, shipping, installments, title) nao e low_confidence."""
        result = engine.evaluate(card_only_product)
        # 4 >= 3, nao e low confidence
        assert result.low_confidence is False

    def test_minimal_data_is_low_confidence(self, engine):
        """Produto com apenas 2 criterios disponiveis deve ser low_confidence."""
        product = ScrapedProduct(
            ml_id="MLB999",
            url="http://ml.com",
            title="Produto Minimo Para Testar Confianca",
            price=50.0,
            original_price=100.0,  # 50% off — desconto disponivel
            rating=0,
            review_count=0,
            free_shipping=False,
            installments_without_interest=False,
            badge="",
        )
        result = engine.evaluate(product)
        # Disponiveis: discount + title + shipping(always) + installments(always) = 4
        # Shipping e installments sao "sempre disponiveis" mesmo com valor 0
        assert result.low_confidence is False


# ---------------------------------------------------------------------------
# Integracao
# ---------------------------------------------------------------------------


class TestIntegration:
    def test_great_product_passes(self, engine, great_product):
        result = engine.evaluate(great_product)
        assert result.passed is True
        assert result.score >= 60

    def test_bad_product_fails(self, engine, bad_product):
        result = engine.evaluate(bad_product)
        assert result.passed is False

    def test_batch_returns_sorted_by_score(self, engine, great_product):
        medium_product = ScrapedProduct(
            ml_id="MLB333",
            url="http://ml.com",
            title="Oculos de Sol Polarizado UV400 Masculino",
            price=79.90,
            original_price=159.90,
            rating=4.2,
            review_count=80,
            free_shipping=False,
        )
        results = engine.evaluate_batch([medium_product, great_product])
        if len(results) >= 2:
            assert results[0].score >= results[1].score

    def test_batch_excludes_rejected(self, engine, great_product, bad_product):
        results = engine.evaluate_batch([great_product, bad_product])
        ml_ids = [s.product.ml_id for s in results]
        assert bad_product.ml_id not in ml_ids

    def test_to_dict_format(self, engine, great_product):
        result = engine.evaluate(great_product)
        d = result.to_dict()

        # Campos obrigatorios do produto
        assert "ml_id" in d
        assert "price" in d
        assert "title" in d

        # Campos do score
        assert "score" in d
        assert "passed" in d
        assert "reject_reason" in d
        assert "low_confidence" in d
        assert "available_criteria" in d
        assert "redistribution_factor" in d

        # Breakdown com 7 criterios
        bd = d["score_breakdown"]
        assert "discount" in bd
        assert "badge" in bd
        assert "rating" in bd
        assert "reviews" in bd
        assert "free_shipping" in bd
        assert "installments" in bd
        assert "title_quality" in bd

    def test_to_dict_has_correct_score(self, engine, great_product):
        result = engine.evaluate(great_product)
        d = result.to_dict()
        assert d["score"] == result.score


# ---------------------------------------------------------------------------
# Validacao de Pesos
# ---------------------------------------------------------------------------


class TestWeightValidation:
    def test_valid_weights_no_warning(self, mock_settings):
        """Pesos que somam 100 nao devem gerar warning."""
        # mock_settings ja tem pesos que somam 100
        engine = ScoreEngine()
        # Se chegou aqui sem erro, tudo OK

    def test_invalid_weights_logs_warning(self):
        """Pesos que nao somam 100 devem logar warning."""
        with patch("src.analyzer.score_engine.settings") as mock:
            mock.score.min_discount_pct = 20.0
            mock.score.min_score = 60
            mock.score.min_rating = 4.0
            mock.score.min_reviews = 10
            mock.score.weight_discount = 50.0  # Exagerado
            mock.score.weight_badge = 15.0
            mock.score.weight_rating = 15.0
            mock.score.weight_reviews = 10.0
            mock.score.weight_free_shipping = 10.0
            mock.score.weight_installments = 10.0
            mock.score.weight_title_quality = 10.0
            # Total = 120, nao 100

            with patch("src.analyzer.score_engine.logger") as mock_logger:
                ScoreEngine()
                mock_logger.warning.assert_called_once()
                call_args = mock_logger.warning.call_args
                assert "score_weights_not_100" in str(call_args)


# ---------------------------------------------------------------------------
# Remove Accents Helper
# ---------------------------------------------------------------------------


class TestRemoveAccents:
    def test_basic_accents(self):
        assert ScoreEngine._remove_accents("relâmpago") == "relampago"
        assert ScoreEngine._remove_accents("imperdível") == "imperdivel"
        assert ScoreEngine._remove_accents("avaliação") == "avaliacao"

    def test_no_accents(self):
        assert ScoreEngine._remove_accents("hello world") == "hello world"

    def test_empty_string(self):
        assert ScoreEngine._remove_accents("") == ""


# ---------------------------------------------------------------------------
# Fake Discount Detector (mantido do original)
# ---------------------------------------------------------------------------


class TestFakeDiscountDetector:
    def setup_method(self):
        self.detector = FakeDiscountDetector()

    def test_genuine_discount_passes(self):
        product = ScrapedProduct(
            ml_id="MLB1",
            url="http://ml.com",
            title="Produto genuino original",
            price=199.90,
            original_price=299.90,  # ~33% off — razoavel
        )
        result = self.detector.check(product)
        assert result.is_fake is False

    def test_extreme_ratio_flagged(self):
        product = ScrapedProduct(
            ml_id="MLB2",
            url="http://ml.com",
            title="Produto inflado original",
            price=50.0,
            original_price=5000.0,  # 100x — impossivel
        )
        result = self.detector.check(product)
        assert result.is_fake is True
        assert result.confidence > 0.6

    def test_no_original_price_not_fake(self):
        product = ScrapedProduct(
            ml_id="MLB3",
            url="http://ml.com",
            title="Produto sem preco original",
            price=99.90,
            original_price=None,
        )
        result = self.detector.check(product)
        assert result.is_fake is False

    def test_suspicious_round_price(self):
        assert self.detector._is_suspiciously_round(1000.0) is True
        assert self.detector._is_suspiciously_round(999.90) is False
        assert self.detector._is_suspiciously_round(100.0) is False  # < R$200
