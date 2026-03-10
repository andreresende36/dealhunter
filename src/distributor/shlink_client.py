"""
DealHunter — Shlink Client
Encurta URLs longas de afiliado usando a API do Shlink.
Documentação: https://shlink.io/documentation/api-docs/
"""

import hashlib
from typing import Optional

import httpx
import structlog

from src.config import settings

logger = structlog.get_logger(__name__)


class ShlinkClient:
    """
    Cliente para a API REST do Shlink.

    Uso:
        client = ShlinkClient()
        short_url = await client.shorten("https://www.mercadolivre.com.br/...")
    """

    API_VERSION = "v3"

    def __init__(self):
        self.cfg = settings.shlink
        self.base_url = f"{self.cfg.api_url.rstrip('/')}/rest/{self.API_VERSION}"
        self.headers = {
            "X-Api-Key": self.cfg.api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def shorten(
        self,
        long_url: str,
        custom_slug: Optional[str] = None,
        tags: Optional[list[str]] = None,
    ) -> str:
        """
        Encurta uma URL longa.

        Args:
            long_url: URL original (com parâmetros de afiliado)
            custom_slug: Slug personalizado (ex: "tenis-nike-30off")
            tags: Tags para organização (ex: ["moda", "calcados"])

        Returns:
            URL encurtada (ex: "https://s.sempreblack.com/abc123")
            Em caso de erro, retorna a URL original.
        """
        slug = custom_slug or self._generate_slug(long_url)
        payload = {
            "longUrl": long_url,
            "customSlug": slug,
            "tags": tags or ["dealhunter"],
            "findIfExists": True,  # Reutiliza se URL já foi encurtada
        }

        # Usa domínio personalizado se configurado (ex: s.sempreblack.com)
        if self.cfg.domain:
            payload["domain"] = self.cfg.domain

        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.post(
                    f"{self.base_url}/short-urls",
                    json=payload,
                    headers=self.headers,
                )
                response.raise_for_status()
                data = response.json()
                short_url = data.get("shortUrl", long_url)
                logger.info(
                    "url_shortened",
                    slug=slug,
                    short_url=short_url,
                    tags=tags,
                )
                return short_url

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "shlink_http_error",
                    status=exc.response.status_code,
                    detail=exc.response.text[:200],
                    url=long_url,
                )
                return long_url
            except Exception as exc:
                logger.error("shlink_error", error=str(exc), url=long_url)
                return long_url

    async def get_stats(self, slug: str) -> Optional[dict]:
        """Retorna estatísticas de cliques de um link encurtado."""
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.get(
                    f"{self.base_url}/short-urls/{slug}/visits",
                    headers=self.headers,
                )
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                logger.error("shlink_stats_error", slug=slug, error=str(exc))
                return None

    async def delete(self, slug: str) -> bool:
        """Remove um link encurtado."""
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.delete(
                    f"{self.base_url}/short-urls/{slug}",
                    headers=self.headers,
                )
                return response.status_code == 204
            except Exception as exc:
                logger.error("shlink_delete_error", slug=slug, error=str(exc))
                return False

    def _generate_slug(self, url: str) -> str:
        """
        Gera um slug determinístico a partir da URL.
        Garante que a mesma URL sempre gere o mesmo slug (idempotente).
        """
        hash_val = hashlib.md5(url.encode()).hexdigest()[:6]
        return hash_val
