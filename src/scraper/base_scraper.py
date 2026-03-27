"""
Crivo — Base Scraper
Classe abstrata com todas as técnicas de anti-bloqueio para o Mercado Livre.
Subclasses devem implementar apenas o método `scrape()`.
"""

import asyncio
import random
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

import structlog
from fake_useragent import UserAgent
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# User-Agents — gerados dinamicamente via fake-useragent (sempre atualizados)
# com fallback para lista estática caso a lib falhe
# ---------------------------------------------------------------------------
_ua_provider: UserAgent | None = None


def _get_ua_provider() -> UserAgent:
    """Inicializa o provedor de User-Agents sob demanda (singleton)."""
    global _ua_provider
    if _ua_provider is None:
        try:
            _ua_provider = UserAgent(
                browsers=["Chrome", "Firefox", "Edge", "Safari"],
                os=["Windows", "Mac OS X", "Linux"],
                min_percentage=1.0,
            )
        except Exception:
            _ua_provider = UserAgent(
                fallback="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"  # noqa: E501
            )
    return _ua_provider


# Lista estática como fallback final se fake-useragent falhar completamente
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",  # noqa: E501
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",  # noqa: E501
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",  # noqa: E501
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",  # noqa: E501
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.15",  # noqa: E501
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Mobile/15E148 Safari/604.1",  # noqa: E501
]

# Headers base que imitam um browser real
BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",  # noqa: E501
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


# ---------------------------------------------------------------------------
# Data model para produto raspado
# ---------------------------------------------------------------------------


@dataclass
class ScrapedProduct:
    """Representa um produto coletado do Mercado Livre."""

    # Identificadores
    ml_id: str  # ID único do produto no ML (ex: "MLB123456")
    url: str  # URL original do produto

    # Dados básicos
    title: str
    price: float  # Preço atual em BRL (cartão/parcelado — preço "universal")
    original_price: Optional[float]  # Preço antes do desconto (se disponível)
    pix_price: Optional[float] = None  # Preço com desconto Pix/boleto (se diferente de price)
    discount_pct: float = 0.0  # Percentual de desconto calculado

    # Metadata
    rating: float = 0.0  # Avaliação média (0-5)
    review_count: int = 0  # Número de avaliações
    category: str = ""
    image_url: str = ""
    free_shipping: bool = False
    full_shipping: bool = False  # Enviado pelo FULL (fulfilled by ML)
    installments_without_interest: bool = False
    installment_count: Optional[int] = None  # Nº máximo de parcelas (ex: 10, 12)
    installment_value: Optional[float] = None  # Valor por parcela em BRL
    badge: str = ""  # Ex: "Oferta do dia", "Mais vendido"
    brand: str = ""  # Marca extraída do card (ex: "GROWTH SUPPLEMENTS")
    variations: str = ""  # Texto de variações (ex: "Disponível em 6 cores")
    discount_type: str = ""  # "standard" | "pix" | ""
    gender: str = "Sem gênero"  # Masculino | Feminino | Unissex | Sem gênero

    # Controle interno
    scraped_at: float = field(default_factory=time.time)
    source: str = ""  # Ex: "ofertas_do_dia", "categoria_moda"
    marketplace: str = "Mercado Livre"  # Marketplace de origem

    def __post_init__(self):
        # Desconto baseado no preço Pix se disponível, senão no preço cartão
        effective_price = self.pix_price if self.pix_price else self.price
        if self.original_price and self.original_price > effective_price:
            self.discount_pct = round((1 - effective_price / self.original_price) * 100, 1)

    def to_dict(self) -> dict:
        """Serializa para dicionário (útil para salvar no banco)."""
        return {
            "ml_id": self.ml_id,
            "url": self.url,
            "title": self.title,
            "price": self.price,
            "original_price": self.original_price,
            "pix_price": self.pix_price,
            "discount_pct": self.discount_pct,
            "rating": self.rating,
            "review_count": self.review_count,
            "category": self.category,
            "image_url": self.image_url,
            "free_shipping": self.free_shipping,
            "full_shipping": self.full_shipping,
            "installments_without_interest": self.installments_without_interest,
            "installment_count": self.installment_count,
            "installment_value": self.installment_value,
            "badge": self.badge,
            "brand": self.brand,
            "variations": self.variations,
            "discount_type": self.discount_type,
            "gender": self.gender,
            "scraped_at": self.scraped_at,
            "source": self.source,
            "marketplace": self.marketplace,
        }


# ---------------------------------------------------------------------------
# Exceções de scraping
# ---------------------------------------------------------------------------


class CaptchaError(Exception):
    """Raised when CAPTCHA is detected on the page."""


class RateLimitError(Exception):
    """Raised when rate limited (HTTP 429)."""


# ---------------------------------------------------------------------------
# Classe base abstrata
# ---------------------------------------------------------------------------


class BaseScraper(ABC):
    """
    Classe base para todos os scrapers do Crivo.

    Fornece:
    - Rotação de User-Agent
    - Delays aleatórios entre requisições
    - Headers realistas
    - Gerenciamento de contexto do Playwright
    - Retry automático com backoff exponencial
    - Detecção básica de anti-bot (CAPTCHA, bloqueio)
    """

    ML_BASE_URL = "https://www.mercadolivre.com.br"

    def __init__(self):
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._request_count = 0
        self._last_user_agent = ""
        self.cfg = settings.scraper

    # ------------------------------------------------------------------
    # Gerenciamento de ciclo de vida (usar como context manager async)
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "BaseScraper":
        await self._start_browser()
        return self

    async def __aexit__(self, *_) -> None:
        await self._close_browser()

    async def _start_browser(self) -> None:
        """Inicia o Playwright e abre o browser com configurações anti-bot."""
        self._playwright = await async_playwright().start()

        launch_kwargs = {
            "headless": self.cfg.headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--no-first-run",
                "--no-zygote",
                "--disable-gpu",
            ],
        }

        if self.cfg.proxy_url and self.cfg.proxy_url.startswith(
            ("http://", "https://", "socks4://", "socks5://")
        ):
            launch_kwargs["proxy"] = {"server": self.cfg.proxy_url}

        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        await self._new_context()
        logger.info("browser_started", headless=self.cfg.headless)

    async def _new_context(self) -> None:
        """Cria um novo contexto de browser com UA e headers aleatórios."""
        user_agent = self._pick_user_agent()
        vp: dict[str, int] = random.choice(
            [
                {"width": 1920, "height": 1080},
                {"width": 1440, "height": 900},
                {"width": 1366, "height": 768},
                {"width": 1280, "height": 800},
            ]
        )

        if self._browser is None:
            raise RuntimeError("Browser not started")
        self._context = await self._browser.new_context(
            user_agent=user_agent,
            viewport={"width": vp["width"], "height": vp["height"]},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            extra_http_headers=BASE_HEADERS,
            # Bloqueia recursos desnecessários para acelerar
            # (ativado via route abaixo)
        )

        # Bloqueia imagens, fontes e mídia para acelerar o scraping
        await self._context.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,eot,mp4,mp3,avi}",
            lambda route: route.abort(),
        )

        # Injeta script para esconder sinais de automação
        await self._context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en-US', 'en'] });  # noqa: E501
            window.chrome = { runtime: {} };
        """
        )

    async def _close_browser(self) -> None:
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("browser_closed", requests_made=self._request_count)

    # ------------------------------------------------------------------
    # Utilitários de anti-bloqueio
    # ------------------------------------------------------------------

    def _pick_user_agent(self) -> str:
        """Gera um User-Agent atualizado via fake-useragent, com fallback estático."""
        try:
            ua = _get_ua_provider().random
            if ua and ua != self._last_user_agent:
                self._last_user_agent = ua
                return ua
        except Exception:
            pass
        # Fallback: lista estática
        candidates = [ua for ua in USER_AGENTS if ua != self._last_user_agent]
        ua = random.choice(candidates)
        self._last_user_agent = ua
        return ua

    async def _random_delay(
        self, extra_min: float = 0.0, extra_max: float = 0.0
    ) -> None:
        """Aguarda um delay aleatório para simular comportamento humano."""
        delay = random.uniform(
            self.cfg.delay_min + extra_min,
            self.cfg.delay_max + extra_max,
        )
        logger.debug("delay", seconds=round(delay, 2))
        await asyncio.sleep(delay)

    async def _human_scroll(self, page: Page) -> None:
        """Simula scroll na página (steps grandes + delays curtos)."""
        total_height = await page.evaluate("document.body.scrollHeight")
        current = 0
        while current < total_height:
            scroll_step = random.randint(600, 1200)
            current = min(current + scroll_step, total_height)
            await page.evaluate(f"window.scrollTo(0, {current})")
            await asyncio.sleep(random.uniform(0.05, 0.15))

    async def _rotate_context_if_needed(self, every_n_requests: int = 20) -> None:
        """Recria o contexto do browser a cada N requisições para limpar cookies."""
        if self._request_count > 0 and self._request_count % every_n_requests == 0:
            logger.info("rotating_context", request_count=self._request_count)
            if self._context is not None:
                await self._context.close()
            await self._new_context()

    # ------------------------------------------------------------------
    # Navegação com retry
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(settings.scraper.max_retries),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        reraise=True,
    )
    async def _goto(self, page: Page, url: str) -> bool:
        """
        Navega para a URL com retry automático.
        Retorna True se a página carregou com sucesso.
        """
        self._request_count += 1

        try:
            response = await page.goto(
                url,
                timeout=self.cfg.page_timeout,
                wait_until="domcontentloaded",
            )

            if response and response.status == 429:
                logger.warning("rate_limited", url=url)
                await asyncio.sleep(random.uniform(30, 60))
                raise RateLimitError("Rate limited — retrying")

            if response and response.status >= 400:
                logger.warning("http_error", status=response.status, url=url)
                return False

            # Verifica se caiu em página de CAPTCHA
            if await self._is_blocked(page):
                logger.warning("captcha_detected", url=url)
                await asyncio.sleep(random.uniform(10, 20))
                raise CaptchaError("CAPTCHA detectado — retrying")

            # Aceita banner de cookies se presente
            await self._accept_cookies(page)

            return True

        except Exception as exc:
            logger.error("goto_error", url=url, error=str(exc))
            raise

    async def _accept_cookies(self, page: Page) -> bool:
        """
        Tenta aceitar o banner de cookies se estiver presente na página.

        Testa primeiro seletores CSS específicos do ML e padrões comuns
        (OneTrust, etc.), depois faz varredura por texto em botões visíveis.
        Retorna True se algum botão foi clicado, False se nenhum banner foi
        encontrado. Nunca lança exceção — erros são silenciados.
        """
        # Seletores CSS diretos (ML + plataformas comuns de consentimento)
        _CSS_SELECTORS = [
            # Mercado Livre — botão "Concordo" / "Entendido"
            "button[data-testid='action:understood-button']",
            "#cookie-disclaimer-actions button",
            # OneTrust (amplamente utilizado)
            "#onetrust-accept-btn-handler",
            ".onetrust-accept-btn-handler",
            # Padrões genéricos por id/class
            "button[id*='accept'][id*='cookie']",
            "button[class*='accept'][class*='cookie']",
            "button[id*='cookie'][id*='accept']",
        ]

        # Textos de botão a procurar (case-insensitive, substring)
        _TEXT_PATTERNS = [
            "aceitar todos",
            "aceitar tudo",
            "aceitar cookies",
            "concordo",
            "entendi",
            "aceitar",
            "accept all",
            "accept cookies",
            "i agree",
        ]

        # 1. Tenta seletores CSS
        for selector in _CSS_SELECTORS:
            try:
                btn = await page.query_selector(selector)
                if btn and await btn.is_visible():
                    await btn.click()
                    await asyncio.sleep(0.5)
                    logger.debug("cookies_accepted", method="css", selector=selector)
                    return True
            except Exception:
                pass

        # 2. Tenta por texto — percorre botões visíveis
        for text in _TEXT_PATTERNS:
            try:
                btn = page.get_by_role("button", name=re.compile(text, re.IGNORECASE))
                if await btn.count() > 0 and await btn.first.is_visible():
                    await btn.first.click()
                    await asyncio.sleep(0.5)
                    logger.debug("cookies_accepted", method="text", pattern=text)
                    return True
            except Exception:
                pass

        return False

    async def _is_blocked(self, page: Page) -> bool:
        """Detecta se o site retornou uma página de bloqueio/CAPTCHA."""
        title = (await page.title()).lower()

        # Sinais no título da página (alta confiança)
        title_signals = [
            "captcha",
            "robot",
            "blocked",
            "access denied",
            "403 forbidden",
            "verificação de segurança",
            "security check",
        ]

        for signal in title_signals:
            if signal in title:
                return True

        # Sinais no conteúdo da página (mais específicos para evitar
        # falsos positivos com <meta name="robots"> e similares)
        content = (await page.content()).lower()
        content_signals = [
            "captcha",
            "g-recaptcha",
            "h-captcha",
            "hcaptcha",
            "challenge-form",
            "verificação de segurança",
            "prove you are human",
            "não sou um robô",
            "não é um robô",
        ]

        for signal in content_signals:
            if signal in content:
                return True

        return False

    async def _new_page(self) -> Page:
        """Cria uma nova aba no contexto atual."""
        if self._context is None:
            raise RuntimeError("Browser context not created")
        page = await self._context.new_page()
        page.set_default_timeout(self.cfg.page_timeout)
        return page

    # ------------------------------------------------------------------
    # Interface pública — subclasses DEVEM implementar
    # ------------------------------------------------------------------

    @abstractmethod
    async def scrape(self) -> list[ScrapedProduct]:
        """
        Executa o scraping e retorna lista de produtos encontrados.
        Deve ser implementado por cada scraper específico.
        """
        ...

    def full_url(self, path: str) -> str:
        """Constrói URL completa a partir de um path relativo."""
        if path.startswith("http"):
            return path
        return (
            f"{self.ML_BASE_URL}{path}"
            if path.startswith("/")
            else f"{self.ML_BASE_URL}/{path}"
        )

    # ------------------------------------------------------------------
    # Helpers de parsing compartilhados
    # ------------------------------------------------------------------

    def _extract_ml_id(self, url: str) -> str | None:
        """
        Extrai o ID do produto da URL do ML.

        Ordem de prioridade:
        1. /up/MLBU... no path  → Produto Universal (produto.mercadolivre.com.br/up/MLBU...)
        2. wid=MLB...           → ID do listing vencedor (query/fragment)
        3. MLB... no path       → ID de listing no path (antes do '?')
        4. MLB... em qualquer lugar na URL (fallback)

        Normaliza removendo o traço (MLB-12345 → MLB12345).
        """
        # 1. Produto Universal: /up/MLBU...
        up_match = re.search(r"/up/(MLBU\d+)", url, re.IGNORECASE)
        if up_match:
            return up_match.group(1)

        # 2. wid= no query ou fragment (ID do listing vencedor)
        wid_match = re.search(r"[?&#]wid=(MLB-?\d+)", url, re.IGNORECASE)
        if wid_match:
            return wid_match.group(1).replace("-", "")

        # 3. MLB... no path (antes do '?')
        path = url.split("?")[0]
        path_match = re.search(r"\b(MLB-?\d+)\b", path, re.IGNORECASE)
        if path_match:
            return path_match.group(1).replace("-", "")

        # 4. Fallback: qualquer MLB... na URL completa
        match = re.search(r"(MLB-?\d+)", url, re.IGNORECASE)
        if match:
            return match.group(1).replace("-", "")

        return None

    def _clean_price(self, raw: str) -> float | None:
        """
        Converte string de preço para float no formato brasileiro.

        Exemplos:
            "1.299" → 1299.0
            "99,90" → 99.90
            "1.299,90" → 1299.90
        """
        try:
            cleaned = re.sub(r"[^\d,.]", "", raw)
            if not cleaned:
                return None
            cleaned = cleaned.replace(".", "").replace(",", ".")
            return float(cleaned)
        except (ValueError, AttributeError):
            return None

    def _parse_discount_pct(self, text: str) -> float:
        """Extrai percentual de desconto de texto como '50% OFF'."""
        match = re.search(r"(\d+)\s*%", text)
        return float(match.group(1)) if match else 0.0
