"""
DealHunter — Vision Evaluator
Usa Claude Haiku Vision via OpenRouter para validar e ranquear imagens candidatas.
"""

from __future__ import annotations

import base64
from io import BytesIO
from typing import TYPE_CHECKING

import httpx
import structlog

from src.config import settings

if TYPE_CHECKING:
    from PIL import Image

logger = structlog.get_logger(__name__)

VISION_PROMPT = """Você é um especialista em marketing visual para e-commerce.

Estou selecionando a melhor imagem para divulgar o produto "{title}" em um grupo de ofertas.

A IMAGEM 0 é a imagem original do anúncio (geralmente fundo branco, pouco atrativa).
As IMAGENS 1 a {n} são candidatas encontradas na internet.

Para cada candidata, avalie:
1. É o MESMO produto (ou muito similar)? Se não for o mesmo produto, elimine.
2. É mais ATRATIVA que a original para gerar cliques em um grupo de ofertas?
3. Tem boa qualidade visual (resolução, iluminação, composição)?

Responda SOMENTE com um JSON no formato:
{{"best": N, "reason": "motivo curto"}}

Onde N é o número da melhor imagem (1 a {n}), ou 0 se nenhuma candidata for melhor que a original.
Se nenhuma candidata for do mesmo produto, responda {{"best": 0, "reason": "nenhuma candidata é do mesmo produto"}}.
"""


def _image_to_base64(img: Image.Image, max_size: int = 512) -> str:
    """Converte PIL.Image para base64 JPEG, redimensionando se necessário."""
    w, h = img.size
    if w > max_size or h > max_size:
        ratio = min(max_size / w, max_size / h)
        img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)

    buf = BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=80)
    return base64.b64encode(buf.getvalue()).decode("ascii")


async def evaluate_candidates(
    original_url: str,
    product_title: str,
    candidates: list[tuple[dict, "Image.Image"]],
) -> dict | None:
    """
    Avalia candidatas usando Claude Haiku Vision via OpenRouter.

    Envia a imagem original + todas as candidatas em UMA chamada,
    pedindo para ranquear qual é a mais atrativa e do mesmo produto.

    Args:
        original_url: URL da imagem original (thumbnail ML).
        product_title: Título do produto para contexto.
        candidates: Lista de (meta_dict, PIL.Image) da filtragem local.

    Returns:
        Dict da candidata vencedora (do Serper), ou None se nenhuma for melhor.
    """
    api_key = settings.openrouter.api_key
    if not api_key:
        logger.warning("openrouter_key_missing_for_vision")
        return None

    if not candidates:
        return None

    # Monta as imagens para a chamada Vision
    content: list[dict] = []

    # Imagem original (imagem 0) — enviada como URL
    content.append({
        "type": "text",
        "text": "IMAGEM 0 (original do anúncio):",
    })
    content.append({
        "type": "image_url",
        "image_url": {"url": original_url},
    })

    # Candidatas (imagens 1..N) — enviadas como base64
    for i, (meta, img) in enumerate(candidates, start=1):
        b64 = _image_to_base64(img)
        content.append({
            "type": "text",
            "text": f"IMAGEM {i} (candidata — fonte: {meta.get('source', 'desconhecida')}):",
        })
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
        })

    # Prompt
    prompt = VISION_PROMPT.format(title=product_title, n=len(candidates))
    content.append({"type": "text", "text": prompt})

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://dealhunter.ai",
                    "X-Title": "DealHunter",
                },
                json={
                    "model": "anthropic/claude-haiku-4-5-20251001",
                    "max_tokens": 200,
                    "temperature": 0,
                    "messages": [
                        {"role": "user", "content": content},
                    ],
                },
            )
            resp.raise_for_status()
            data = resp.json()

        reply = data["choices"][0]["message"]["content"].strip()
        logger.debug("vision_raw_reply", reply=reply)

        # Parse da resposta JSON
        import json
        # Extrair JSON da resposta (pode vir com markdown code block)
        if "```" in reply:
            reply = reply.split("```")[1]
            if reply.startswith("json"):
                reply = reply[4:]
            reply = reply.strip()

        result = json.loads(reply)
        best_idx = result.get("best", 0)
        reason = result.get("reason", "")

        logger.info(
            "vision_evaluation_done",
            product=product_title[:40],
            best=best_idx,
            reason=reason,
        )

        if best_idx == 0 or best_idx > len(candidates):
            return None

        return candidates[best_idx - 1][0]  # Retorna o dict do Serper

    except httpx.HTTPStatusError as exc:
        logger.error(
            "vision_http_error",
            status=exc.response.status_code,
            detail=exc.response.text[:200],
        )
        return None
    except Exception as exc:
        logger.error("vision_evaluation_failed", error=str(exc))
        return None
