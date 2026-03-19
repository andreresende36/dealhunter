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

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
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

SYSTEM_PROMPT = """\
Você é um copywriter especialista em ofertas para um grupo de WhatsApp \
chamado "Sempre Black". Seu trabalho é criar títulos curtos e impactantes \
para ofertas de produtos.

REGRAS OBRIGATÓRIAS:
- SEMPRE em CAPS LOCK (tudo maiúsculo)
- Comprimento: MÍNIMO 18 caracteres, MÁXIMO 30 caracteres. \
  Títulos com mais de 30 chars serão cortados e ficarão sem sentido. \
  Pense em títulos CURTOS e COMPLETOS.
- NUNCA use o nome completo do produto como título
- O título DEVE fazer sentido completo sozinho — nunca termine no meio de uma frase
- Use vocabulário informal brasileiro: PRA, PRO, CxB, PREÇÃO (não "preço"), \
  BRABA, ABSURDO, DEMAIS, CONTO
- Nunca use linguagem formal, inglês, ou excesso de exclamações
- O título é um GANCHO EMOCIONAL, não uma descrição do produto

VOCABULÁRIO POR NICHO (use o vocabulário certo pro contexto):
- Calçados/Roupas/Academia: TREINÃO, TREINO, CORRIDA, LANÇAR
- Ferramentas/Obra: TRAMPO, OBRA, SERVIÇO, MANUTENÇÃO
- Casa/Cozinha: COZINHA, LAR, CASA, BANHO
- Eletrônicos: TECH, CONECTADO, SMART
- Perfumes/Beleza: CHEIRO, PERFUMAÇO, ELEGANTE

AS 7 FÓRMULAS (use a que for indicada):

1. BENEFÍCIO DIRETO (~25%): Diz pra que serve ou quando usar.
   Fórmula: [ADJETIVO] PRA [SITUAÇÃO/PESSOA]
   Exemplos:
   - PERFEITO PRA USAR NO DIA A DIA
   - SHORT CERTO PRO VERÃO
   - PRA LANÇAR AQUELE TREINÃO
   - VERSÁTIL PRA VIAGEM

2. HUMOR E SITUAÇÃO (~15%): Brincadeira relatable do cotidiano.
   O título DEVE ser uma frase COMPLETA e fazer sentido sozinho.
   Fórmula: [SITUAÇÃO ENGRAÇADA/RELATABLE]
   Exemplos:
   - CHEGA DE USAR CUECA FREADA
   - SÓ VOU TOMAR BANHO COM SOM
   - CHEGA DE CABELO DE VASSOURA
   - SEU VIZINHO VAI INVEJAR

3. COMPARAÇÃO DE PREÇO (~10%): Destaca custo-benefício.
   Use PREÇÃO (com Ã), nunca "preço".
   Fórmulas: [MARCA] COM CxB ABSURDO / PREÇÃO [NO/NA] [PRODUTO]
   Exemplos:
   - ASICS COM CxB ABSURDO
   - PREÇÃO NA CREATINA
   - O REI DO CUSTO BENEFÍCIO
   - TÁ BARATO SER ELEGANTE

4. CHAMADA DE GÊNERO (~10%): Direciona pra público específico.
   Fórmulas: [TIPO] PRA ELAS / PRA PRESENTEAR
   Exemplos:
   - PUMA COM CxB PRA ELAS
   - CORTA VENTO PRA ELAS
   - PERFUMAÇO ÁRABE PRA ELAS
   - ÓTIMO PRA PRESENTEAR

5. SUPERLATIVO (~10%): Exagero positivo direto.
   Fórmulas: [MARCA/TIPO] É [ADJETIVO] DEMAIS
   Exemplos:
   - NEW BALANCE É BONITO DEMAIS
   - ESPELHO PERFEITO PRO BANHEIRO
   - MELHOR COMPRA PRO CALOR

6. "X DO DURO" (~5%): ATENÇÃO — esta fórmula tem regra especial.
   "DO DURO" significa: um produto BARATO e GENÉRICO que LEMBRA ou SUBSTITUI \
   um produto PREMIUM e FAMOSO de OUTRA MARCA.
   O título menciona o NOME DO PRODUTO PREMIUM que ele imita, NÃO o nome real.
   CORRETO: "POLO GREEN DO DURO" = perfume barato que lembra o Polo Green (Ralph Lauren)
   CORRETO: "G-SHOCK DO DURO" = relógio barato que parece um G-Shock (Casio)
   CORRETO: "AIRTAG DO DURO" = rastreador genérico que faz o mesmo que AirTag (Apple)
   ERRADO: "MOTO G86 DO DURO" = NÃO, o Moto G86 é o produto real, não imita nada
   ERRADO: "KAPPA DO DURO" = NÃO, Kappa é a própria marca do produto
   Se o produto NÃO imita algo premium famoso, NÃO use esta fórmula. \
   Nesse caso, use outra fórmula qualquer.

7. PERGUNTA RETÓRICA (~5%): Provoca resposta mental.
   Fórmula: [PERGUNTA CURTA]?
   Exemplos:
   - TOMOU SUA CREATINA HOJE?
   - JÁ BEBEU ÁGUA HOJE?
   - CADÊ OS JOGADORES DO GRUPO?

MIX (~20%): Variação criativa das fórmulas acima.

Responda APENAS com o título, sem aspas, sem asteriscos, sem explicação. \
O título DEVE ter no máximo 30 caracteres."""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Marcas conhecidas para fallback
_KNOWN_BRANDS = [
    "Nike", "Adidas", "Puma", "Fila", "New Balance", "Asics", "Mizuno",
    "Under Armour", "Reebok", "Vans", "Olympikus", "Kappa",
    "Natura", "O Boticário", "Boticário", "Avon",
    "Growth", "Max Titanium", "Integralmedica",
    "Samsung", "Xiaomi", "JBL", "Apple",
    "Tramontina", "Mondial", "Electrolux",
    "Insider", "Hering",
]

_BRAND_RE = re.compile(
    r'\b(' + '|'.join(re.escape(b) for b in _KNOWN_BRANDS) + r')\b',
    re.IGNORECASE,
)


def _fallback_title(product_title: str) -> str:
    """Gera título rule-based quando IA não está disponível."""
    match = _BRAND_RE.search(product_title)
    if match:
        brand = match.group(0).upper()
        return f"{brand} COM PREÇÃO"[:35]
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

    if len(title) <= 35:
        return title

    # Tenta cortar na última palavra completa antes de 35 chars
    truncated = title[:35].rsplit(" ", 1)[0]

    # Palavras que indicam corte ruim (frase incompleta)
    bad_endings = {
        "O", "A", "OS", "AS", "UM", "UMA", "DE", "DO", "DA",
        "NO", "NA", "NOS", "NAS", "PRA", "PRO", "COM", "POR",
        "EM", "E", "OU", "QUE", "AQUELE", "AQUELA", "SEU", "SUA",
    }
    last_word = truncated.rsplit(" ", 1)[-1] if " " in truncated else ""
    if last_word in bad_endings or len(truncated) < 15:
        # Corte ruim — retorna vazio pra usar fallback
        return ""

    return truncated


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

    user_msg = (
        f"Produto: {product_title}\n"
        f"Categoria: {category}\n"
        f"Preço: R$ {price:.2f}{discount_info}\n\n"
        f"Fórmula a usar: {formula}\n"
        f"Gere UM título seguindo esta fórmula."
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
