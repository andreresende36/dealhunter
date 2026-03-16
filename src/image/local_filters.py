"""
DealHunter — Local Image Filters
Filtros rápidos e gratuitos para eliminar imagens ruins antes da validação por IA.
"""

from __future__ import annotations

from io import BytesIO

import httpx
import structlog
from PIL import Image

from src.config import settings

logger = structlog.get_logger(__name__)


async def download_image(url: str, timeout: float = 10.0) -> bytes | None:
    """
    Baixa uma imagem por URL.

    Returns:
        Bytes da imagem ou None se falhar.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, follow_redirects=True)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if "image" not in content_type:
                return None
            return resp.content
    except Exception as exc:
        logger.debug("image_download_failed", url=url[:80], error=str(exc))
        return None


def check_resolution(img: Image.Image, min_size: int | None = None) -> bool:
    """Verifica se a imagem atende à resolução mínima."""
    min_px = min_size or settings.image_worker.min_resolution
    w, h = img.size
    return w >= min_px and h >= min_px


def is_white_background(
    img: Image.Image,
    threshold: float | None = None,
) -> bool:
    """
    Detecta se a imagem tem fundo predominantemente branco.

    Analisa a proporção de pixels "quase brancos" (R,G,B > 240).
    Retorna True se a proporção exceder o threshold.
    """
    thresh = threshold or settings.image_worker.white_bg_threshold
    rgb = img.convert("RGB")
    pixels = list(rgb.getdata())
    total = len(pixels)
    if total == 0:
        return False

    white_count = sum(
        1 for r, g, b in pixels
        if r > 240 and g > 240 and b > 240
    )
    ratio = white_count / total
    return ratio >= thresh


def has_watermark_heuristic(img: Image.Image) -> bool:
    """
    Heurística simples para detectar marcas d'água.

    Verifica se há uma faixa semitransparente/clara no terço inferior
    da imagem (onde watermarks costumam ficar).
    Retorna True se suspeitar de marca d'água.
    """
    w, h = img.size
    if h < 100:
        return False

    # Analisa o terço inferior
    bottom_third = img.crop((0, int(h * 0.7), w, h)).convert("RGB")
    pixels = list(bottom_third.getdata())
    total = len(pixels)
    if total == 0:
        return False

    # Pixels muito claros mas não completamente brancos (watermark translúcido)
    semi_white = sum(
        1 for r, g, b in pixels
        if 200 < r < 250 and 200 < g < 250 and 200 < b < 250
    )
    ratio = semi_white / total
    # Threshold alto — só marca como watermark se for muito óbvio
    return ratio > 0.5


async def filter_candidates(
    candidates: list[dict],
) -> list[tuple[dict, Image.Image]]:
    """
    Pipeline de filtragem local para candidatas de imagem.

    Para cada candidata:
      1. Download
      2. Verificar resolução mínima
      3. Descartar fundo branco
      4. Descartar com marca d'água

    Args:
        candidates: Lista de dicts do Serper (url, width, height, ...).

    Returns:
        Lista de (candidata_dict, PIL.Image) que passaram em todos os filtros.
    """
    survivors: list[tuple[dict, Image.Image]] = []
    max_ai = settings.image_worker.max_candidates_for_ai

    for candidate in candidates:
        url = candidate.get("url", "")
        if not url:
            continue

        # 1. Download
        raw = await download_image(url)
        if raw is None:
            continue

        try:
            img = Image.open(BytesIO(raw))
        except Exception:
            continue

        # 2. Resolução mínima
        if not check_resolution(img):
            logger.debug("image_filtered_resolution", url=url[:60])
            continue

        # 3. Fundo branco
        if is_white_background(img):
            logger.debug("image_filtered_white_bg", url=url[:60])
            continue

        # 4. Marca d'água
        if has_watermark_heuristic(img):
            logger.debug("image_filtered_watermark", url=url[:60])
            continue

        survivors.append((candidate, img))

        # Limita candidatas para não sobrecarregar a IA
        if len(survivors) >= max_ai:
            break

    logger.info(
        "local_filter_done",
        input=len(candidates),
        survivors=len(survivors),
    )
    return survivors
