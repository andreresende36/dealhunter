"""
DealHunter — Image Storage
Upload e recuperação de imagens aprimoradas via Supabase Storage.
"""

from __future__ import annotations

import httpx
import structlog

from src.config import settings

logger = structlog.get_logger(__name__)


async def download_image_bytes(url: str, timeout: float = 15.0) -> bytes | None:
    """Baixa imagem da URL e retorna os bytes."""
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, follow_redirects=True)
            resp.raise_for_status()
            return resp.content
    except Exception as exc:
        logger.error("image_download_failed", url=url[:80], error=str(exc))
        return None


async def upload_to_supabase(
    product_id: str,
    image_bytes: bytes,
    ext: str = "jpg",
) -> str | None:
    """
    Faz upload de imagem para o Supabase Storage.

    Args:
        product_id: UUID do produto.
        image_bytes: Bytes da imagem.
        ext: Extensão do arquivo (jpg, png, webp).

    Returns:
        URL pública da imagem ou None em caso de erro.
    """
    from supabase import acreate_client

    bucket = settings.image_worker.supabase_bucket
    path = f"products/{product_id}/enhanced.{ext}"

    try:
        client = await acreate_client(
            settings.supabase.url,
            settings.supabase.service_role_key,
        )

        content_type = f"image/{ext}" if ext != "jpg" else "image/jpeg"

        await client.storage.from_(bucket).upload(
            path=path,
            file=image_bytes,
            file_options={"content-type": content_type, "upsert": "true"},
        )

        public_url = client.storage.from_(bucket).get_public_url(path)

        logger.info(
            "image_uploaded",
            product_id=product_id,
            path=path,
            size_kb=len(image_bytes) // 1024,
        )
        return public_url

    except Exception as exc:
        logger.error(
            "image_upload_failed",
            product_id=product_id,
            error=str(exc),
        )
        return None


async def get_enhanced_url(product_id: str) -> str | None:
    """
    Verifica se já existe uma imagem aprimorada para o produto no Supabase Storage.

    Returns:
        URL pública se existir, None caso contrário.
    """
    from supabase import acreate_client

    bucket = settings.image_worker.supabase_bucket

    try:
        client = await acreate_client(
            settings.supabase.url,
            settings.supabase.service_role_key,
        )

        # Tenta listar o arquivo no path do produto
        files = await client.storage.from_(bucket).list(f"products/{product_id}")

        for f in (files or []):
            name = f.get("name", "")
            if name.startswith("enhanced."):
                return client.storage.from_(bucket).get_public_url(
                    f"products/{product_id}/{name}"
                )

        return None

    except Exception as exc:
        logger.debug(
            "image_check_failed",
            product_id=product_id,
            error=str(exc),
        )
        return None
