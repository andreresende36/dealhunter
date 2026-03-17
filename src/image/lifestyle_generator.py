"""
DealHunter — Lifestyle Image Generator
Pipeline de 2 passos para transformar thumbnail de produto em imagem lifestyle.
Tudo via OpenRouter.

Passo 1: Claude Haiku 4.5 analisa a imagem e gera um prompt otimizado
Passo 2: Gemini 2.5 Flash Image gera a imagem lifestyle

Ambos os passos são síncronos (httpx) — o wrapper async roda em executor.
"""

from __future__ import annotations

import asyncio
import base64
import json
from io import BytesIO

import httpx
import structlog

from src.config import settings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_HEADERS_BASE = {
    "Content-Type": "application/json",
    "HTTP-Referer": "https://dealhunter.ai",
    "X-Title": "DealHunter",
}

HAIKU_MODEL = "anthropic/claude-haiku-4-5"
GEMINI_MODEL = "google/gemini-2.5-flash-image"

ANALYSIS_SYSTEM_PROMPT = """\
Você é um especialista em fotografia de produto e marketing visual para e-commerce \
brasileiro. Sua tarefa é analisar uma foto de produto e gerar um prompt de geração \
de imagem otimizado para o Gemini 2.5 Flash Image (Nano Banana).

PROCESSO:
1. Identifique o produto: tipo, marca (se visível), cor, materiais, detalhes
2. Determine a categoria: calçado, eletrônico, roupa, acessório, brinquedo, \
   utensílio doméstico, etc.
3. Escolha o cenário ideal de uso real para esse produto
4. Gere o prompt em inglês otimizado para geração de imagem

REGRAS PARA O PROMPT:
- Escreva SEMPRE em inglês (melhor resultado no Gemini)
- Use linguagem descritiva de fotografia: mencione lente, iluminação, composição
- Descreva a cena de uso real com detalhes sensoriais (textura, luz, ambiente)
- Inclua "this exact product" para referenciar a imagem de entrada
- Use frases positivas (descreva o que QUER, não o que não quer)
- Especifique estilo fotográfico: editorial lifestyle, natural light photography
- Inclua detalhes de iluminação: golden hour, soft natural light, warm ambient
- Mencione profundidade de campo: shallow depth of field, f/2.8, bokeh
- Especifique que a imagem deve parecer uma foto real (não render 3D)

CENÁRIOS POR CATEGORIA (adapte criativamente):
- Calçado → pessoa caminhando em rua urbana, calçadão, parque
- Eletrônico → mesa de trabalho organizada, sala moderna, uso casual
- Roupa → pessoa usando em ambiente urbano, café, rua movimentada
- Acessório → close-up em uso, complementando um look
- Brinquedo → criança brincando em sala iluminada, parque, jardim
- Utensílio doméstico → cozinha moderna, bancada organizada
- Mochila/bolsa → pessoa em campus, trilha, viagem
- Produto de beleza → bancada de banheiro elegante, penteadeira

RESPONDA APENAS com um JSON válido (sem markdown, sem backticks):
{
  "product_name": "nome descritivo do produto",
  "category": "categoria identificada",
  "scene_description": "descrição breve da cena escolhida em português",
  "generation_prompt": "prompt completo em inglês para gerar a imagem"
}
"""


def _get_headers() -> dict[str, str]:
    return {
        **OPENROUTER_HEADERS_BASE,
        "Authorization": f"Bearer {settings.openrouter.api_key}",
    }


# ---------------------------------------------------------------------------
# Passo 1 — Análise com Haiku via OpenRouter
# ---------------------------------------------------------------------------
def _step1_analyze_product(image_b64: str, media_type: str) -> dict:
    """Envia imagem ao Haiku via OpenRouter e recebe prompt otimizado."""
    api_key = settings.openrouter.api_key
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY não configurada")

    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            OPENROUTER_URL,
            headers=_get_headers(),
            json={
                "model": HAIKU_MODEL,
                "messages": [
                    {"role": "system", "content": ANALYSIS_SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:{media_type};base64,{image_b64}",
                                },
                            },
                            {
                                "type": "text",
                                "text": "Analise este produto e gere o prompt de imagem lifestyle.",
                            },
                        ],
                    },
                ],
                "max_tokens": 1024,
                "temperature": 0.3,
            },
        )
        if not resp.is_success:
            logger.error("haiku_api_error", status=resp.status_code, body=resp.text[:400])
        resp.raise_for_status()

    data = resp.json()
    raw_text = data["choices"][0]["message"]["content"].strip()

    # Extrai o JSON pela posição do primeiro '{' e último '}' — robusto contra
    # texto extra antes/depois do bloco markdown ou respostas mistas
    start = raw_text.find("{")
    end = raw_text.rfind("}") + 1
    if start == -1 or end == 0:
        logger.error("haiku_no_json_found", raw=raw_text[:300])
        raise RuntimeError("Haiku não retornou JSON válido")

    cleaned = raw_text[start:end]

    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.error("haiku_invalid_json", raw=raw_text[:300], error=str(e))
        raise RuntimeError("Haiku não retornou JSON válido") from e

    logger.info(
        "haiku_analysis_done",
        product=result.get("product_name", "?"),
        category=result.get("category", "?"),
        prompt_len=len(result.get("generation_prompt", "")),
    )
    return result


# ---------------------------------------------------------------------------
# Passo 2 — Geração com Gemini via OpenRouter
# ---------------------------------------------------------------------------
def _step2_generate_image(prompt: str, image_b64: str, media_type: str) -> bytes:
    """
    Gera imagem lifestyle via Gemini 2.5 Flash Image no OpenRouter.
    Retorna JPEG bytes.
    """
    api_key = settings.openrouter.api_key
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY não configurada")

    with httpx.Client(timeout=60.0) as client:
        resp = client.post(
            OPENROUTER_URL,
            headers=_get_headers(),
            json={
                "model": GEMINI_MODEL,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": prompt,
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:{media_type};base64,{image_b64}",
                                },
                            },
                        ],
                    },
                ],
            },
        )
        if not resp.is_success:
            logger.error("gemini_api_error", status=resp.status_code, body=resp.text[:400])
        resp.raise_for_status()

    data = resp.json()
    message = data["choices"][0]["message"]

    # O OpenRouter pode retornar a imagem gerada em 3 locais distintos:
    # 1. message.images[]  — campo dedicado do OpenRouter para imagens geradas (Gemini)
    # 2. message.content[] — lista de parts multimodais (alguns modelos)
    # 3. message.content   — string com data URI ou base64 puro

    # Formato 1: message.images (Gemini 2.5 Flash Image via OpenRouter)
    images = message.get("images")
    if isinstance(images, list) and images:
        img_entry = images[0]
        url = img_entry.get("image_url", {}).get("url", "") if isinstance(img_entry, dict) else ""
        if url.startswith("data:"):
            b64_data = url.split(",", 1)[1]
            image_bytes = base64.b64decode(b64_data)
            logger.info("gemini_image_generated_from_images_field")
            return _ensure_jpeg(image_bytes)

    content = message.get("content")

    # Formato 2: content como lista de parts multimodais
    if isinstance(content, list):
        for part in content:
            if isinstance(part, dict) and part.get("type") == "image_url":
                url = part.get("image_url", {}).get("url", "")
                if url.startswith("data:"):
                    b64_data = url.split(",", 1)[1]
                    image_bytes = base64.b64decode(b64_data)
                    logger.info("gemini_image_generated_from_parts")
                    return _ensure_jpeg(image_bytes)

    # Formato 3: content como string (data URI ou base64 puro)
    if isinstance(content, str):
        if content.startswith("data:image"):
            b64_data = content.split(",", 1)[1]
            image_bytes = base64.b64decode(b64_data)
            logger.info("gemini_image_generated_from_data_uri")
            return _ensure_jpeg(image_bytes)
        try:
            image_bytes = base64.b64decode(content)
            if len(image_bytes) > 1000:
                logger.info("gemini_image_generated_from_raw_b64")
                return _ensure_jpeg(image_bytes)
        except Exception:
            pass
        logger.warning("gemini_returned_text_instead", text=content[:200])

    raise RuntimeError(
        "Gemini não retornou imagem via OpenRouter. "
        "Verifique se o modelo suporta geração de imagem nesta rota."
    )


def _ensure_jpeg(image_bytes: bytes) -> bytes:
    """Converte quaisquer bytes de imagem para JPEG com quality 85.

    JPEG suporta apenas RGB/L. Converte P (palette/GIF), RGBA, LA e outros
    modos para RGB antes de salvar.
    """
    from PIL import Image

    img = Image.open(BytesIO(image_bytes))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=85)
    logger.info("image_converted_to_jpeg", size=f"{img.width}x{img.height}")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Pipeline sync completo
# ---------------------------------------------------------------------------
def _sync_generate(image_b64: str, media_type: str) -> bytes:
    """Pipeline completo síncrono: análise → geração. Retorna JPEG bytes."""
    analysis = _step1_analyze_product(image_b64, media_type)
    prompt = analysis["generation_prompt"]
    return _step2_generate_image(prompt, image_b64, media_type)


# ---------------------------------------------------------------------------
# API async para uso no sender
# ---------------------------------------------------------------------------
async def generate_lifestyle_image(thumbnail_url: str) -> bytes | None:
    """
    Baixa thumbnail do produto, gera imagem lifestyle via Haiku + Gemini.
    Tudo via OpenRouter.

    Args:
        thumbnail_url: URL da thumbnail do produto no ML.

    Returns:
        JPEG bytes da imagem gerada, ou None em caso de erro.
    """
    from src.image.image_storage import download_image_bytes

    # Baixa thumbnail
    image_bytes = await download_image_bytes(thumbnail_url)
    if not image_bytes:
        logger.error("lifestyle_thumbnail_download_failed", url=thumbnail_url[:80])
        return None

    # Normaliza para JPEG antes de enviar ao Haiku — thumbnails do ML podem ser
    # webp, gif ou jpeg com content-type incorreto; o Anthropic rejeita mismatches.
    image_bytes = _ensure_jpeg(image_bytes)
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    media_type = "image/jpeg"

    # Roda pipeline sync em thread (httpx sync)
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None,
            _sync_generate,
            image_b64,
            media_type,
        )
        return result
    except Exception as exc:
        # Loga o corpo da resposta HTTP em erros 4xx/5xx para facilitar debug
        detail = str(exc)
        if hasattr(exc, "response"):
            try:
                detail = exc.response.text[:400]  # type: ignore[union-attr]
            except Exception:
                pass
        logger.error("lifestyle_generation_failed", error=detail)
        return None
