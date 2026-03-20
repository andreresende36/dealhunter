"""
DealHunter — Lifestyle Image Generator
Pipeline de 2 passos para transformar thumbnail de produto em imagem lifestyle.
Tudo via OpenRouter.

Passo 1: Claude Haiku 4.5 analisa a imagem e gera um prompt otimizado
Passo 2: Modelo de imagem configurável gera a imagem lifestyle

O modelo do Passo 2 é configurável via LIFESTYLE_IMAGE_MODEL no .env.
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
from src.prompts_loader import load_prompt

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

# ---------------------------------------------------------------------------
# Modelos de geração de imagem disponíveis no OpenRouter
# Chave = alias amigável (usado no .env), valor = model ID do OpenRouter
# ---------------------------------------------------------------------------
LIFESTYLE_IMAGE_MODELS: dict[str, str] = {
    "flux-flex": "black-forest-labs/flux.2-flex",  # FLUX.2 Flex
    "flux-klein": "black-forest-labs/flux.2-klein-4b",  # FLUX.2 Klein 4B
    "flux-max": "black-forest-labs/flux.2-max",  # FLUX.2 Max
    "flux-pro": "black-forest-labs/flux.2-pro",  # FLUX.2 Pro
    "gpt5-image": "openai/gpt-5-image",  # GPT-5 Image
    "gpt5-image-mini": "openai/gpt-5-image-mini",  # GPT-5 Image Mini
    "nano-banana": (
        "google/gemini-2.5-flash-image"  # Gemini 2.5 Flash Image Preview
    ),
    "nano-banana-2": "google/gemini-3.1-flash-image-preview",  # Nano Banana 2 (Gemini 3.1)
    "nano-banana-pro": (
        "google/gemini-3-pro-image-preview"  # Nano Banana Pro (Gemini 3 Pro)
    ),
    "riverflow-fast": "sourceful/riverflow-v2-fast",  # Riverflow V2 Fast
    "riverflow-fast-preview": "sourceful/riverflow-v2-fast-preview",  # Riverflow V2 Fast Preview
    "riverflow-max-preview": "sourceful/riverflow-v2-max-preview",  # Riverflow V2 Max Preview
    "riverflow-pro": "sourceful/riverflow-v2-pro",  # Riverflow V2 Pro
    "riverflow-fast-preview": (
        "sourceful/riverflow-v2-fast-preview"  # Riverflow V2 Fast Preview
    ),
    "riverflow-std-preview": (
        "sourceful/riverflow-v2-standard-preview"  # Riverflow V2 Standard Preview
    ),
    "seedream": "bytedance-seed/seedream-4.5",  # Seedream 4.5
}


def _resolve_image_model() -> str:
    """Resolve o alias do .env para o model ID do OpenRouter."""
    alias = settings.openrouter.lifestyle_image_model.strip().lower()
    # Aceita tanto o alias amigável quanto o model ID direto
    if alias in LIFESTYLE_IMAGE_MODELS:
        model_id = LIFESTYLE_IMAGE_MODELS[alias]
    elif alias in LIFESTYLE_IMAGE_MODELS.values():
        model_id = alias
    else:
        valid = ", ".join(sorted(LIFESTYLE_IMAGE_MODELS.keys()))
        raise ValueError(
            f"Modelo de imagem '{alias}' não reconhecido. " f"Opções válidas: {valid}"
        )
    logger.info("lifestyle_image_model_resolved", alias=alias, model_id=model_id)
    return model_id


ANALYSIS_SYSTEM_PROMPT = load_prompt("lifestyle_analysis_system")
_ANALYSIS_USER_MSG = "Analise este produto e gere o prompt de imagem lifestyle."


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
                                "text": _ANALYSIS_USER_MSG,
                            },
                        ],
                    },
                ],
                "max_tokens": 1024,
                "temperature": 0.3,
            },
        )
        if not resp.is_success:
            logger.error(
                "haiku_api_error", status=resp.status_code, body=resp.text[:400]
            )
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
def _step2_generate_image(
    prompt: str, image_b64: str, media_type: str, model_id: str | None = None
) -> bytes:
    """
    Gera imagem lifestyle via modelo configurado no OpenRouter.
    Retorna JPEG bytes.
    """
    api_key = settings.openrouter.api_key
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY não configurada")

    if model_id is None:
        model_id = _resolve_image_model()

    with httpx.Client(timeout=60.0) as client:
        resp = client.post(
            OPENROUTER_URL,
            headers=_get_headers(),
            json={
                "model": model_id,
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
            logger.error(
                "gemini_api_error", status=resp.status_code, body=resp.text[:400]
            )
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
        url = (
            img_entry.get("image_url", {}).get("url", "")
            if isinstance(img_entry, dict)
            else ""
        )
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
    model_id = _resolve_image_model()
    analysis = _step1_analyze_product(image_b64, media_type)
    prompt = analysis["generation_prompt"]
    return _step2_generate_image(prompt, image_b64, media_type, model_id=model_id)


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
