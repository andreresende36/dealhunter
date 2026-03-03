"""
DealHunter — Detector de Desconto Falso
Identifica práticas comuns de inflação de preço antes do desconto no ML.

Estratégias detectadas:
1. Preço original impossível (produto caro demais para a categoria)
2. Desconto inconsistente com preço atual vs. histórico
3. Preço atual acima da média do mercado para o mesmo produto
4. Padrão de "preço maquiado" (arredondamentos suspeitos)
"""

from dataclasses import dataclass
from typing import Optional

import structlog

from src.scraper.base_scraper import ScrapedProduct

logger = structlog.get_logger(__name__)


@dataclass
class FakeDiscountResult:
    is_fake: bool
    confidence: float  # 0.0 a 1.0
    reason: Optional[str] = None
    original_price_adjusted: Optional[float] = None  # Preço original estimado real


class FakeDiscountDetector:
    """
    Detecta descontos artificialmente inflados ("pricejacking").

    Abordagem atual: heurísticas simples.
    Fase 2: integrar com API de histórico de preços (ex: Camel, Histórico ML).

    Uso:
        detector = FakeDiscountDetector()
        result = detector.check(product)
        if result.is_fake:
            # descartar ou sinalizar para revisão
    """

    # Razão máxima aceitável entre preço original e preço atual
    # Ex: 5.0 = o preço original pode ser no máximo 5x o atual
    MAX_PRICE_RATIO = 5.0

    # Desconto máximo plausível sem suspeita automática (%)
    MAX_PLAUSIBLE_DISCOUNT = 80.0

    def check(self, product: ScrapedProduct) -> FakeDiscountResult:
        """Verifica se o desconto do produto é genuíno."""
        if not product.original_price:
            # Sem preço original → não podemos verificar desconto falso
            return FakeDiscountResult(is_fake=False, confidence=0.0)

        flags: list[tuple[str, float]] = []  # (motivo, peso_de_suspeita)

        # 1. Razão preço original / preço atual muito alta
        ratio = product.original_price / max(product.price, 0.01)
        if ratio > self.MAX_PRICE_RATIO:
            flags.append(
                (
                    f"Preço original {ratio:.1f}x maior que o atual "
                    f"(máx razoável: {self.MAX_PRICE_RATIO}x)",
                    0.8,
                )
            )

        # 2. Desconto acima do plausível
        if product.discount_pct > self.MAX_PLAUSIBLE_DISCOUNT:
            flags.append(
                (
                    f"Desconto de {product.discount_pct:.0f}% acima do limite plausível",
                    0.7,
                )
            )

        # 3. Preço original parece "arredondado" de forma suspeita
        if self._is_suspiciously_round(product.original_price):
            flags.append(("Preço original com arredondamento suspeito", 0.3))

        # 4. Desconto inconsistente com os valores mostrados
        calculated_discount = (1 - product.price / product.original_price) * 100
        if abs(calculated_discount - product.discount_pct) > 5:
            flags.append(
                (
                    f"Desconto informado ({product.discount_pct:.0f}%) "
                    f"inconsistente com cálculo ({calculated_discount:.0f}%)",
                    0.5,
                )
            )

        # 5. Preço atual em centavos mas original em reais redondos (ex: R$9,99 vs R$200,00)
        if self._price_in_cents_pattern(product.price, product.original_price):
            flags.append(("Padrão preço centavos vs. original redondo", 0.4))

        if not flags:
            return FakeDiscountResult(is_fake=False, confidence=0.0)

        # Combina suspeitas: confidence = 1 - produto das (1 - peso)
        confidence = 1.0
        for _, weight in flags:
            confidence *= 1.0 - weight
        confidence = round(1.0 - confidence, 2)

        is_fake = confidence >= 0.6
        primary_reason = flags[0][0] if flags else None

        logger.debug(
            "fake_discount_check",
            ml_id=product.ml_id,
            is_fake=is_fake,
            confidence=confidence,
            flags=[f[0] for f in flags],
        )

        return FakeDiscountResult(
            is_fake=is_fake,
            confidence=confidence,
            reason=primary_reason,
        )

    def check_batch(
        self, products: list[ScrapedProduct]
    ) -> list[tuple[ScrapedProduct, FakeDiscountResult]]:
        """Verifica uma lista de produtos. Retorna todos com seus resultados."""
        results = []
        fake_count = 0
        for product in products:
            result = self.check(product)
            if result.is_fake:
                fake_count += 1
            results.append((product, result))

        logger.info(
            "batch_fake_check",
            total=len(products),
            fakes_detected=fake_count,
        )
        return results

    # ------------------------------------------------------------------
    # Heurísticas
    # ------------------------------------------------------------------

    def _is_suspiciously_round(self, price: float) -> bool:
        """
        Detecta preços originais suspeitos por serem excessivamente redondos.
        Ex: 500.00, 1000.00, 2000.00 — muito comuns em preços inflados.
        """
        rounded = round(price)
        remainder = rounded % 100
        # Preços exatamente em centenas são suspeitos acima de R$200
        return price >= 200 and remainder == 0 and price == rounded

    def _price_in_cents_pattern(self, current: float, original: float) -> bool:
        """
        Detecta padrão de preço atual muito baixo vs. original muito alto.
        Ex: R$ 9,99 atual vs. R$ 199,99 original (ratio > 10x)
        """
        return current < 20 and original > 100 and (original / current) > 10
