"""
DealHunter — Serper.dev Image Search Client
Busca imagens de produtos via Google Images usando a API do Serper.dev.
"""

from __future__ import annotations

import httpx
import structlog

from src.config import settings

logger = structlog.get_logger(__name__)


async def search_product_images(
    title: str,
    num_results: int | None = None,
) -> list[dict]:
    """
    Busca imagens de um produto no Google Images via Serper.dev.

    Args:
        title: Título do produto (usado como query de busca).
        num_results: Número de resultados desejados (default: config).

    Returns:
        Lista de dicts com keys: url, width, height, source, title.
        Lista vazia se a busca falhar.
    """
    api_key = settings.serper.api_key
    if not api_key:
        logger.warning("serper_key_missing")
        return []

    num = num_results or settings.serper.max_results

    # Busca lifestyle: adiciona contexto para evitar fundo branco
    query = f"{title} produto foto"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                "https://google.serper.dev/images",
                headers={
                    "X-API-KEY": api_key,
                    "Content-Type": "application/json",
                },
                json={
                    "q": query,
                    "gl": "br",
                    "hl": "pt-br",
                    "num": num,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        images = data.get("images", [])
        results = []
        for img in images[:num]:
            results.append({
                "url": img.get("imageUrl", ""),
                "width": img.get("imageWidth", 0),
                "height": img.get("imageHeight", 0),
                "source": img.get("source", ""),
                "title": img.get("title", ""),
            })

        logger.info(
            "serper_search_done",
            query=query[:60],
            results=len(results),
        )
        return results

    except httpx.HTTPStatusError as exc:
        logger.error(
            "serper_http_error",
            status=exc.response.status_code,
            detail=exc.response.text[:200],
        )
        return []
    except Exception as exc:
        logger.error("serper_search_failed", error=str(exc))
        return []
