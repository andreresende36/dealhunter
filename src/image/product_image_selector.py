"""
DealHunter — Product Image Selector (Style Guide v3)
Pipeline de 3 camadas para seleção de imagem real do produto.

Camada 1: Busca web no site oficial da marca
Camada 2: Seleção inteligente das imagens do anúncio ML (2ª/3ª foto)
Camada 3: 1ª imagem do ML (último recurso)

Validação opcional com Haiku 4.5 Vision nas Camadas 1 e 2.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
from io import BytesIO
from urllib.parse import quote_plus

import httpx
import structlog

from src.config import settings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
HAIKU_MODEL = "anthropic/claude-haiku-4-5"

# Domínios oficiais por marca (para Camada 1)
BRAND_DOMAINS: dict[str, list[str]] = {
    "nike": ["nike.com.br", "nike.com"],
    "adidas": ["adidas.com.br", "adidas.com"],
    "fila": ["fila.com.br"],
    "puma": ["puma.com", "puma.com.br"],
    "new balance": ["newbalance.com.br"],
    "asics": ["asics.com.br"],
    "mizuno": ["mizuno.com.br"],
    "under armour": ["underarmour.com.br"],
    "reebok": ["reebok.com.br"],
    "vans": ["vans.com.br"],
    "natura": ["natura.com.br"],
    "o boticário": ["oboticario.com.br"],
    "boticário": ["oboticario.com.br"],
    "growth": ["gsuplementos.com.br"],
    "max titanium": ["maxtitanium.com.br"],
    "insider": ["insider.com.br"],
    "hering": ["hering.com.br"],
    "samsung": ["samsung.com.br"],
    "xiaomi": ["xiaomi.com.br"],
    "jbl": ["jbl.com.br"],
    "tramontina": ["tramontina.com.br"],
    "mondial": ["mondial.com.br"],
    "electrolux": ["electrolux.com.br"],
}

# Domínios genéricos por nicho (fallback quando marca não é mapeada)
NICHE_DOMAINS: dict[str, list[str]] = {
    "calçados": ["netshoes.com.br"],
    "perfumes": ["belezanaweb.com.br", "sephora.com.br"],
    "suplementos": ["crescer.com.br"],
    "moda": ["renner.com.br"],
    "eletrônicos": ["kabum.com.br", "amazon.com.br"],
    "casa": ["magazineluiza.com.br", "amazon.com.br"],
}

# Regex para extrair marca do título
_BRAND_NAMES = sorted(BRAND_DOMAINS.keys(), key=len, reverse=True)
_BRAND_RE = re.compile(
    r'\b(' + '|'.join(re.escape(b) for b in _BRAND_NAMES) + r')\b',
    re.IGNORECASE,
)

# Regex para extrair URLs de imagem do HTML do Google Images
_IMG_URL_RE = re.compile(
    r'https?://[^\s"\'<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^\s"\'<>]*)?',
    re.IGNORECASE,
)

# User-Agents para scraping
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


# ---------------------------------------------------------------------------
# Resultado tipado
# ---------------------------------------------------------------------------


class ImageResult:
    """Resultado da seleção de imagem."""

    def __init__(
        self,
        url: str,
        image_bytes: bytes | None = None,
        source: str = "unknown",
        validation_score: int | None = None,
    ):
        self.url = url
        self.image_bytes = image_bytes
        self.source = source  # "brand_web", "ml_api", "ml_thumbnail"
        self.validation_score = validation_score


# ---------------------------------------------------------------------------
# Camada 1 — Busca web no site oficial da marca
# ---------------------------------------------------------------------------


def _extract_brand(title: str) -> str | None:
    """Extrai marca conhecida do título."""
    match = _BRAND_RE.search(title)
    return match.group(0).lower() if match else None


def _get_search_domains(brand: str | None, category: str = "") -> list[str]:
    """Retorna domínios para buscar imagens da marca."""
    domains: list[str] = []
    if brand and brand in BRAND_DOMAINS:
        domains.extend(BRAND_DOMAINS[brand])
    # Adiciona domínios do nicho como fallback
    cat_lower = category.lower()
    for niche, niche_doms in NICHE_DOMAINS.items():
        if niche in cat_lower:
            domains.extend(d for d in niche_doms if d not in domains)
    return domains


async def _search_brand_images(
    product_title: str,
    brand: str | None,
    category: str = "",
    max_results: int = 3,
) -> list[str]:
    """
    Busca imagens do produto nos sites oficiais da marca via Google Images.
    Retorna lista de URLs de imagens candidatas.
    """
    domains = _get_search_domains(brand, category)
    if not domains:
        return []

    # Monta query com site: filter
    site_filter = " OR ".join(f"site:{d}" for d in domains[:3])
    query = f"{product_title} ({site_filter})"
    search_url = (
        f"https://www.google.com/search?q={quote_plus(query)}"
        f"&tbm=isch&tbs=isz:l"  # Filtro: imagens grandes
    )

    try:
        import random
        ua = random.choice(_USER_AGENTS)
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                search_url,
                headers={
                    "User-Agent": ua,
                    "Accept-Language": "pt-BR,pt;q=0.9",
                },
                follow_redirects=True,
            )
            if not resp.is_success:
                logger.warning(
                    "brand_image_search_failed",
                    status=resp.status_code,
                )
                return []

            # Extrai URLs de imagens do HTML
            html = resp.text
            urls = _IMG_URL_RE.findall(html)

            # Filtra: preferir domínios oficiais, excluir Google assets
            filtered: list[str] = []
            for url in urls:
                if any(g in url for g in ["gstatic.com", "google.com", "googleapis"]):
                    continue
                if len(url) < 30:
                    continue
                filtered.append(url)
                if len(filtered) >= max_results:
                    break

            logger.info(
                "brand_image_search_done",
                query=product_title[:50],
                found=len(filtered),
            )
            return filtered

    except Exception as exc:
        logger.warning("brand_image_search_error", error=str(exc))
        return []


# ---------------------------------------------------------------------------
# Camada 2 — Imagens do anúncio ML via API pública
# ---------------------------------------------------------------------------


async def _get_ml_pictures(ml_id: str) -> list[dict]:
    """
    Busca array de fotos do produto via API pública do ML.
    Endpoint: GET https://api.mercadolivre.com.br/items/{ML_ID}
    Retorna lista de dicts com 'id', 'url', 'secure_url', 'size', etc.
    """
    api_url = f"https://api.mercadolivre.com.br/items/{ml_id}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(api_url, follow_redirects=True)
            resp.raise_for_status()
            data = resp.json()
            pictures = data.get("pictures", [])
            logger.info(
                "ml_pictures_fetched",
                ml_id=ml_id,
                count=len(pictures),
            )
            return pictures
    except Exception as exc:
        logger.warning("ml_pictures_fetch_failed", ml_id=ml_id, error=str(exc))
        return []


def _select_best_ml_image(pictures: list[dict]) -> list[str]:
    """
    Seleciona as melhores imagens do array de fotos do ML.
    Pula a primeira (geralmente fundo branco) e retorna 2ª e 3ª.
    """
    if not pictures:
        return []

    urls: list[str] = []
    # Pula a primeira, pega a 2ª e 3ª
    candidates = pictures[1:3] if len(pictures) > 1 else pictures[:1]
    for pic in candidates:
        # Preferir URL -O (original) ou -F (full) em vez de thumbnail
        url = pic.get("secure_url") or pic.get("url", "")
        if url:
            # Substituir sufixo de tamanho para pegar a maior resolução
            url = re.sub(r'-[A-Z]\.', '-O.', url)
            urls.append(url)

    return urls


# ---------------------------------------------------------------------------
# Validação com Haiku Vision
# ---------------------------------------------------------------------------

VALIDATION_PROMPT = """\
Você é um avaliador de imagens para um grupo de ofertas no WhatsApp.
O produto anunciado é: {product_name}

Avalie esta imagem e responda APENAS em JSON (sem markdown, sem backticks):
{{
  "produto_correto": true/false,
  "qualidade": "alta|média|baixa",
  "tipo_fundo": "ambiente|branco|colorido|outro",
  "tem_texto_overlay": true/false,
  "nota_geral": 1-10,
  "motivo": "justificativa curta"
}}

Nota alta (8-10): produto correto, alta qualidade, fundo ambiente/lifestyle, \
sem texto overlay, boa composição.
Rejeição (nota < 5): produto errado, baixa qualidade, muito texto, imagem genérica."""


def _validate_image_sync(image_b64: str, product_name: str) -> dict:
    """Valida imagem com Haiku 4.5 Vision via OpenRouter (síncrono)."""
    api_key = settings.openrouter.api_key
    if not api_key:
        return {"nota_geral": 6, "produto_correto": True, "motivo": "sem API key"}

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer": "https://dealhunter.ai",
        "X-Title": "DealHunter",
    }

    prompt = VALIDATION_PROMPT.format(product_name=product_name)

    with httpx.Client(timeout=15.0) as client:
        resp = client.post(
            OPENROUTER_URL,
            headers=headers,
            json={
                "model": HAIKU_MODEL,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{image_b64}",
                                },
                            },
                            {"type": "text", "text": prompt},
                        ],
                    },
                ],
                "max_tokens": 256,
                "temperature": 0.1,
            },
        )
        resp.raise_for_status()

    data = resp.json()
    raw_text = data["choices"][0]["message"]["content"].strip()

    # Extrai JSON
    start = raw_text.find("{")
    end = raw_text.rfind("}") + 1
    if start == -1 or end == 0:
        logger.warning("validation_no_json", raw=raw_text[:200])
        return {"nota_geral": 5, "produto_correto": True, "motivo": "parse error"}

    try:
        return json.loads(raw_text[start:end])
    except json.JSONDecodeError:
        logger.warning("validation_invalid_json", raw=raw_text[:200])
        return {"nota_geral": 5, "produto_correto": True, "motivo": "parse error"}


async def _validate_image(image_bytes: bytes, product_name: str) -> dict:
    """Wrapper async para validação Haiku."""
    from PIL import Image

    # Normaliza para JPEG e reduz tamanho para validação (economiza tokens)
    img = Image.open(BytesIO(image_bytes))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    # Reduz para max 512px no maior lado (validação não precisa de alta res)
    max_side = max(img.size)
    if max_side > 512:
        ratio = 512 / max_side
        img = img.resize(
            (int(img.width * ratio), int(img.height * ratio)),
            Image.LANCZOS,
        )
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=70)
    b64 = base64.b64encode(buf.getvalue()).decode()

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _validate_image_sync, b64, product_name
    )


# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------


async def _download_image(url: str, timeout: float = 10.0) -> bytes | None:
    """Baixa imagem da URL."""
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, follow_redirects=True)
            resp.raise_for_status()
            if len(resp.content) < 1000:
                return None  # Imagem muito pequena, provavelmente placeholder
            return resp.content
    except Exception as exc:
        logger.debug("image_download_failed", url=url[:80], error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------


async def select_best_image(
    ml_id: str,
    product_title: str,
    thumbnail_url: str = "",
    category: str = "",
) -> ImageResult:
    """
    Pipeline de 3 camadas para selecionar a melhor imagem real do produto.

    Args:
        ml_id: ID do produto no Mercado Livre (ex: MLB123456)
        product_title: Título completo do produto
        thumbnail_url: URL da thumbnail do scraper (fallback final)
        category: Categoria do produto (opcional, melhora busca)

    Returns:
        ImageResult com URL, bytes e metadados da imagem selecionada.
    """
    validation_enabled = settings.openrouter.image_validation_enabled
    brand = _extract_brand(product_title)

    # ===== CAMADA 1: Busca web no site oficial da marca =====
    if brand:
        web_urls = await _search_brand_images(
            product_title, brand, category, max_results=3
        )
        for url in web_urls:
            img_bytes = await _download_image(url)
            if not img_bytes:
                continue

            if validation_enabled:
                try:
                    result = await _validate_image(img_bytes, product_title)
                    score = result.get("nota_geral", 0)
                    correct = result.get("produto_correto", False)

                    if not correct:
                        logger.info("brand_image_wrong_product", url=url[:80])
                        continue
                    if score >= 8:
                        logger.info(
                            "brand_image_selected",
                            score=score,
                            source="brand_web",
                        )
                        return ImageResult(
                            url=url,
                            image_bytes=img_bytes,
                            source="brand_web",
                            validation_score=score,
                        )
                    if score >= 5:
                        logger.info("brand_image_mediocre", score=score)
                        # Continua para tentar ML
                except Exception as exc:
                    logger.warning("brand_validation_error", error=str(exc))
            else:
                # Sem validação, usa a primeira que baixar com sucesso
                logger.info("brand_image_selected_no_validation", url=url[:80])
                return ImageResult(
                    url=url,
                    image_bytes=img_bytes,
                    source="brand_web",
                )

    # ===== CAMADA 2: Seleção inteligente das imagens do ML =====
    pictures = await _get_ml_pictures(ml_id)
    ml_urls = _select_best_ml_image(pictures)

    for url in ml_urls:
        img_bytes = await _download_image(url)
        if not img_bytes:
            continue

        if validation_enabled:
            try:
                result = await _validate_image(img_bytes, product_title)
                score = result.get("nota_geral", 0)
                correct = result.get("produto_correto", False)

                if not correct:
                    logger.info("ml_image_wrong_product", url=url[:80])
                    continue
                if score >= 5:
                    logger.info(
                        "ml_image_selected",
                        score=score,
                        source="ml_api",
                    )
                    return ImageResult(
                        url=url,
                        image_bytes=img_bytes,
                        source="ml_api",
                        validation_score=score,
                    )
            except Exception as exc:
                logger.warning("ml_validation_error", error=str(exc))
                # Usa mesmo sem validar
                return ImageResult(
                    url=url, image_bytes=img_bytes, source="ml_api"
                )
        else:
            logger.info("ml_image_selected_no_validation", url=url[:80])
            return ImageResult(
                url=url, image_bytes=img_bytes, source="ml_api"
            )

    # ===== CAMADA 3: 1ª imagem do ML ou thumbnail (último recurso) =====
    fallback_url = ""
    if pictures:
        pic = pictures[0]
        fallback_url = pic.get("secure_url") or pic.get("url", "")
        if fallback_url:
            fallback_url = re.sub(r'-[A-Z]\.', '-O.', fallback_url)

    if not fallback_url:
        fallback_url = thumbnail_url

    if fallback_url:
        img_bytes = await _download_image(fallback_url)
        logger.info("fallback_image_used", source="ml_first" if pictures else "thumbnail")
        return ImageResult(
            url=fallback_url,
            image_bytes=img_bytes,
            source="ml_first" if pictures else "thumbnail",
        )

    # Nenhuma imagem encontrada
    logger.warning("no_image_found", ml_id=ml_id)
    return ImageResult(url="", image_bytes=None, source="none")
