"""
DealHunter — Title Generator (Style Guide v3)
Gera títulos catchy para ofertas usando Claude Haiku via OpenRouter.

7 fórmulas em rotação com pesos do style guide:
  - Benefício direto (25%)
  - Humor e situação (15%)
  - Comparação de preço/valor (10%)
  - Chamada de gênero (10%)
  - Superlativo (10%)
  - "X DO DURO" (5%)
  - Pergunta retórica (5%)
  - Mix livre (20%)
"""

from __future__ import annotations

import asyncio
import random
import re

import httpx
import structlog

from src.config import settings
from src.prompts_loader import load_prompt
from src.utils.brands import extract_brand
from src.utils.openrouter import OPENROUTER_URL

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

HAIKU_MODEL = "anthropic/claude-haiku-4-5"

# Fórmulas com pesos para random.choices()
FORMULAS: list[tuple[str, int]] = [
    ("beneficio_direto", 25),
    ("humor", 15),
    ("comparacao", 10),
    ("chamada_genero", 10),
    ("superlativo", 10),
    ("x_do_duro", 5),
    ("pergunta_retorica", 5),
    ("mix", 20),
]

FORMULA_NAMES = [f[0] for f in FORMULAS]
FORMULA_WEIGHTS = [f[1] for f in FORMULAS]

# ---------------------------------------------------------------------------
# System prompt com fórmulas e exemplos few-shot
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = load_prompt("title_system")
_TITLE_USER_TEMPLATE = (
    "Produto: {product_title}\n"
    "Categoria: {category}\n"
    "Preço: R$ {price:.2f}{discount_info}\n"
    "\n"
    "Fórmula a usar: {formula}\n"
    "Gere UM título seguindo esta fórmula."
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------




def _fallback_title(product_title: str) -> str:
    """Gera título rule-based quando IA não está disponível."""
    brand = extract_brand(product_title)
    if brand:
        return f"{brand.upper()} COM PREÇÃO"[:35]
    return "OFERTA IMPERDÍVEL"


def _select_formula() -> str:
    """Seleciona fórmula por peso."""
    return random.choices(FORMULA_NAMES, weights=FORMULA_WEIGHTS, k=1)[0]


def _clean_title(raw: str) -> str:
    """Limpa e normaliza o título gerado pela IA.

    Se o título for muito longo (>35 chars), tenta cortar na última
    palavra completa. Se o resultado ficar muito curto (<15 chars)
    ou terminar em artigo/preposição (sinal de corte ruim), retorna
    vazio para que o caller use fallback.
    """
    # Remove asteriscos, aspas, emojis de prefixo
    title = raw.strip().strip("*").strip('"').strip("'").strip()
    # Remove emojis comuns que o Haiku às vezes adiciona
    title = re.sub(r'[\U0001F300-\U0001F9FF]', '', title).strip()
    # Garante CAPS LOCK
    title = title.upper()

    return title


# ---------------------------------------------------------------------------
# Geração via Haiku
# ---------------------------------------------------------------------------


def _generate_sync(
    product_title: str,
    category: str,
    price: float,
    original_price: float | None,
) -> str:
    """Gera título via Haiku (síncrono)."""
    api_key = settings.openrouter.api_key
    if not api_key:
        return _fallback_title(product_title)

    formula = _select_formula()

    discount_info = ""
    if original_price and original_price > price:
        pct = round((1 - price / original_price) * 100)
        discount_info = f" (desconto de {pct}%)"

    user_msg = _TITLE_USER_TEMPLATE.format(
        product_title=product_title,
        category=category,
        price=price,
        discount_info=discount_info,
        formula=formula,
    )

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer": "https://dealhunter.ai",
        "X-Title": "DealHunter",
    }

    with httpx.Client(timeout=10.0) as client:
        resp = client.post(
            OPENROUTER_URL,
            headers=headers,
            json={
                "model": HAIKU_MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                "max_tokens": 64,
                "temperature": 0.8,
            },
        )
        if not resp.is_success:
            logger.warning(
                "title_api_error",
                status=resp.status_code,
                body=resp.text[:200],
            )
            return _fallback_title(product_title)

    data = resp.json()
    raw = data["choices"][0]["message"]["content"].strip()
    title = _clean_title(raw)

    if not title:
        logger.warning(
            "title_truncated_fallback",
            raw=raw[:60],
            product=product_title[:40],
        )
        return _fallback_title(product_title)

    logger.info(
        "title_generated",
        formula=formula,
        title=title,
        length=len(title),
        product=product_title[:40],
    )
    return title


# ---------------------------------------------------------------------------
# Interface pública (async)
# ---------------------------------------------------------------------------


async def generate_catchy_title(
    product_title: str,
    category: str,
    price: float,
    original_price: float | None = None,
) -> str:
    """
    Gera título catchy usando Claude Haiku via OpenRouter.
    Fallback rule-based se API falhar.

    Args:
        product_title: Título completo do produto no ML.
        category: Categoria do produto.
        price: Preço final (ou pix).
        original_price: Preço original (para calcular desconto).

    Returns:
        Título catchy em CAPS LOCK, 20-40 chars.
    """
    loop = asyncio.get_running_loop()
    try:
        title = await loop.run_in_executor(
            None,
            _generate_sync,
            product_title,
            category,
            price,
            original_price,
        )
        return title
    except Exception as exc:
        logger.warning("title_generation_error", error=str(exc))
        return _fallback_title(product_title)
