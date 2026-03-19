"""
DealHunter — Message Validator (Style Guide v3)
Checklist de validação pré-envio para garantir conformidade com o template.

Validação soft: loga warnings mas não bloqueia o envio.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ValidationResult:
    """Resultado da validação de mensagem."""
    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def validate_message(
    whatsapp_text: str,
    free_shipping: bool = False,
    rating: float = 0.0,
    review_count: int = 0,
    has_image: bool = True,
) -> ValidationResult:
    """
    Valida mensagem WhatsApp contra o checklist do Style Guide v3.

    Args:
        whatsapp_text: Texto formatado da mensagem WhatsApp.
        free_shipping: Se o produto tem frete grátis.
        rating: Nota de avaliação do produto.
        review_count: Número de avaliações.
        has_image: Se há imagem anexada.

    Returns:
        ValidationResult com erros e warnings.
    """
    errors: list[str] = []
    warnings: list[str] = []
    lines = whatsapp_text.split("\n")

    # 1. Título em negrito (*...* ) e CAPS LOCK
    first_line = lines[0] if lines else ""
    if not first_line.startswith("*") or not first_line.rstrip().endswith("*"):
        errors.append("Título não está em negrito (*...*)")
    else:
        titulo = first_line.strip("* ")
        if titulo != titulo.upper():
            errors.append("Título não está em CAPS LOCK")
        if len(titulo) > 45:
            warnings.append(f"Título muito longo ({len(titulo)} chars, max 45)")
        if len(titulo) < 10:
            warnings.append(f"Título muito curto ({len(titulo)} chars, min 10)")

    # 2. 📉 XX% OFF presente
    if "📉" not in whatsapp_text or "% OFF" not in whatsapp_text:
        errors.append("Falta 📉 XX% OFF")

    # 3. Bloco de preço
    if "por R$" not in whatsapp_text:
        errors.append("Falta preço final (por R$)")

    # 4. 🤘🏻 após preço
    if "🤘🏻" not in whatsapp_text:
        errors.append("Falta emoji 🤘🏻 após preço")

    # 5. CTA com 🛒
    if "🛒" not in whatsapp_text or "Comprar agora!" not in whatsapp_text:
        errors.append("Falta CTA com 🛒 Comprar agora!")

    # 6. Rodapé da marca
    if "━━━" not in whatsapp_text or "Sempre Black" not in whatsapp_text:
        errors.append("Falta rodapé da marca")

    if "Aqui todo dia é Black Friday" not in whatsapp_text:
        warnings.append("Rodapé não contém 'Aqui todo dia é Black Friday'")

    # 7. Frete grátis: presente apenas se aplicável
    has_frete = "✅ Frete Grátis" in whatsapp_text
    if free_shipping and not has_frete:
        warnings.append("Produto tem frete grátis mas linha não incluída")
    if not free_shipping and has_frete:
        errors.append("Linha de frete grátis presente mas produto não tem frete grátis")

    # 8. Avaliação: presente apenas se rating >= 4.0 e reviews >= 50
    has_rating = "⭐" in whatsapp_text
    should_show = rating >= 4.0 and review_count >= 50
    if should_show and not has_rating:
        warnings.append("Produto tem boa avaliação mas linha não incluída")
    if not should_show and has_rating:
        warnings.append("Linha de avaliação presente mas critérios não atingidos")

    # 9. Sem 🔥 (banido pelo style guide)
    if "🔥" in whatsapp_text:
        errors.append("Emoji 🔥 presente (banido pelo style guide)")

    # 10. Sem hashtags
    if "#" in whatsapp_text:
        warnings.append("Hashtags presentes (removidas no style guide v3)")

    # 11. Imagem
    if not has_image:
        warnings.append("Mensagem sem imagem anexada")

    passed = len(errors) == 0
    result = ValidationResult(passed=passed, errors=errors, warnings=warnings)

    if not passed:
        logger.warning(
            "message_validation_failed",
            errors=errors,
            warnings=warnings,
        )
    elif warnings:
        logger.info("message_validation_warnings", warnings=warnings)

    return result
