"""
DealHunter — Affiliate Link Builder
Constrói links de afiliado do Mercado Livre.

Documentação ML Affiliates: https://www.mercadolivre.com.br/afiliados
Os parâmetros de rastreamento são adicionados à URL do produto.
"""

import re
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import structlog

from src.config import settings

logger = structlog.get_logger(__name__)


class AffiliateLinkBuilder:
    """
    Adiciona parâmetros de afiliado do ML à URL de um produto.

    Uso:
        builder = AffiliateLinkBuilder()
        affiliate_url = builder.build("https://www.mercadolivre.com.br/p/MLB123")
    """

    def __init__(self):
        self.cfg = settings.mercado_livre
        tag = self.cfg.affiliate_tag
        # Parâmetros de rastreamento do programa de afiliados ML
        # Referência: documentação do ML Partners
        self.affiliate_params = {
            "matt_tool": "sem_googleads",  # Fonte (pode ser customizada)
            "matt_word": tag,
            "matt_source": "google",
            "matt_campaign": tag,
            "matt_ad_type": "pla",
            "matt_creative_id": "sem",
        }

    def build(self, product_url: str) -> str:
        """
        Adiciona parâmetros de afiliado à URL do produto.

        Args:
            product_url: URL original do produto no ML

        Returns:
            URL com parâmetros de afiliado
        """
        if not product_url or not self._is_ml_url(product_url):
            logger.warning("invalid_ml_url", url=product_url)
            return product_url

        try:
            parsed = urlparse(product_url)
            existing_params = parse_qs(parsed.query)

            # Combina params existentes com os de afiliado
            # Parâmetros de afiliado têm prioridade
            affiliate_params = {
                **{k: [v[0]] for k, v in existing_params.items()},
                **{k: [v] for k, v in self.affiliate_params.items()},
            }

            # Adiciona ID de afiliado se configurado
            if self.cfg.affiliate_id:
                affiliate_params["matt_affiliate"] = [self.cfg.affiliate_id]

            new_query = urlencode(
                {k: v[0] for k, v in affiliate_params.items()},
                doseq=False,
            )

            affiliate_url = urlunparse(parsed._replace(query=new_query))
            logger.debug(
                "affiliate_link_built", original=product_url, result=affiliate_url
            )
            return affiliate_url

        except Exception as exc:
            logger.error("affiliate_link_error", url=product_url, error=str(exc))
            return product_url

    def _is_ml_url(self, url: str) -> bool:
        """Verifica se a URL é do Mercado Livre."""
        ml_domains = [
            "mercadolivre.com.br",
            "mercadolivre.com",
            "mercadolibre.com",
            "mlstatic.com",
        ]
        return any(domain in url for domain in ml_domains)

    def extract_ml_id(self, url: str) -> str:
        """Extrai o ID do produto da URL."""
        match = re.search(r"(MLB\d+)", url, re.IGNORECASE)
        return match.group(1) if match else ""
