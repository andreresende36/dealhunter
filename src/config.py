"""
DealHunter - Configuração Central
Lê todas as variáveis de ambiente e expõe configurações tipadas para o sistema.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Carrega .env do diretório raiz do projeto
ROOT_DIR = Path(__file__).parent.parent
load_dotenv(ROOT_DIR / ".env")


# ---------------------------------------------------------------------------
# Supabase
# ---------------------------------------------------------------------------


@dataclass
class SupabaseConfig:
    url: str = field(default_factory=lambda: os.environ["SUPABASE_URL"])
    anon_key: str = field(default_factory=lambda: os.environ["SUPABASE_ANON_KEY"])
    service_role_key: str = field(
        default_factory=lambda: os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    )


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------


@dataclass
class TelegramConfig:
    bot_token: str = field(default_factory=lambda: os.environ["TELEGRAM_BOT_TOKEN"])
    # IDs dos grupos/canais "Sempre Black"
    group_ids: list[str] = field(
        default_factory=lambda: [
            g.strip()
            for g in os.getenv("TELEGRAM_GROUP_IDS", "").split(",")
            if g.strip()
        ]
    )
    # Chat ID do admin para alertas (usa o primeiro grupo como fallback)
    admin_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
    )
    # Delay entre mensagens para evitar flood (segundos)
    send_delay: float = float(os.getenv("TELEGRAM_SEND_DELAY", "1.5"))


# ---------------------------------------------------------------------------
# WhatsApp (Evolution API ou similar)
# ---------------------------------------------------------------------------


@dataclass
class WhatsAppConfig:
    api_url: str = field(default_factory=lambda: os.getenv("WHATSAPP_API_URL", ""))
    api_key: str = field(default_factory=lambda: os.getenv("WHATSAPP_API_KEY", ""))
    instance_name: str = field(
        default_factory=lambda: os.getenv("WHATSAPP_INSTANCE_NAME", "dealhunter")
    )
    # Números/grupos destino separados por vírgula
    group_ids: list[str] = field(
        default_factory=lambda: [
            g.strip()
            for g in os.getenv("WHATSAPP_GROUP_IDS", "").split(",")
            if g.strip()
        ]
    )
    # Delay entre mensagens para evitar flood/ban (segundos)
    send_delay: float = float(os.getenv("WHATSAPP_SEND_DELAY", "3.0"))
    # Máximo de mensagens por minuto (rate limit)
    max_messages_per_minute: int = int(os.getenv("WHATSAPP_MAX_MSG_PER_MIN", "10"))


# ---------------------------------------------------------------------------
# Mercado Livre Afiliados
# ---------------------------------------------------------------------------


@dataclass
class MercadoLivreConfig:
    # Tag de rastreamento para os links
    affiliate_tag: str = field(
        default_factory=lambda: os.getenv("ML_AFFILIATE_TAG", "sempreblack")
    )
    # Cookies de sessão do ML para chamar a API createLink (JSON string)
    # Exportar do navegador: _csrf, ssid, nsa_rotok, orguseridp, orgnickp
    session_cookies: str = field(
        default_factory=lambda: os.getenv("ML_SESSION_COOKIES", "")
    )
    # CSRF token (header x-csrf-token, necessário junto com cookie _csrf)
    csrf_token: str = field(
        default_factory=lambda: os.getenv("ML_CSRF_TOKEN", "")
    )
    # Credenciais do usuário DealHunter (temporário — via .env)
    user_name: str = field(
        default_factory=lambda: os.getenv("ML_USER_NAME", "")
    )
    user_email: str = field(
        default_factory=lambda: os.getenv("ML_USER_EMAIL", "")
    )
    user_password: str = field(
        default_factory=lambda: os.getenv("ML_USER_PASSWORD", "")
    )


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------


@dataclass
class ScraperConfig:
    # Delays em segundos (min, max)
    delay_min: float = float(os.getenv("SCRAPER_DELAY_MIN", "2.0"))
    delay_max: float = float(os.getenv("SCRAPER_DELAY_MAX", "5.0"))
    # Timeout de página em ms
    page_timeout: int = int(os.getenv("SCRAPER_PAGE_TIMEOUT", "30000"))
    # Headless mode para Playwright
    headless: bool = os.getenv("SCRAPER_HEADLESS", "true").lower() == "true"
    # Número máximo de retentativas por página
    max_retries: int = int(os.getenv("SCRAPER_MAX_RETRIES", "3"))
    # Número máximo de páginas por fonte
    max_pages: int = int(os.getenv("SCRAPER_MAX_PAGES", "10"))
    # Proxy (opcional): "http://user:pass@host:port"
    proxy_url: Optional[str] = field(
        default_factory=lambda: os.getenv("SCRAPER_PROXY_URL")
    )
    # Debug: salva screenshots dos cards rejeitados pelo score engine
    debug_screenshots: bool = os.getenv("SCRAPER_DEBUG_SCREENSHOTS", "false").lower() == "true"


# ---------------------------------------------------------------------------
# Score Engine (filtros de qualidade de oferta)
# ---------------------------------------------------------------------------


@dataclass
class ScoreConfig:
    # Desconto mínimo para considerar a oferta (%)
    min_discount_pct: float = float(os.getenv("SCORE_MIN_DISCOUNT_PCT", "20.0"))
    # Pontuação mínima para publicar
    min_score: int = int(os.getenv("SCORE_MIN_SCORE", "60"))
    # Avaliação mínima do produto (estrelas)
    min_rating: float = float(os.getenv("SCORE_MIN_RATING", "4.0"))
    # Número mínimo de avaliações
    min_reviews: int = int(os.getenv("SCORE_MIN_REVIEWS", "10"))
    # Pesos por critério (soma = 100)
    weight_discount: float = float(os.getenv("SCORE_WEIGHT_DISCOUNT", "30.0"))
    weight_badge: float = float(os.getenv("SCORE_WEIGHT_BADGE", "15.0"))
    weight_rating: float = float(os.getenv("SCORE_WEIGHT_RATING", "15.0"))
    weight_reviews: float = float(os.getenv("SCORE_WEIGHT_REVIEWS", "10.0"))
    weight_free_shipping: float = float(os.getenv("SCORE_WEIGHT_FREE_SHIPPING", "10.0"))
    weight_installments: float = float(os.getenv("SCORE_WEIGHT_INSTALLMENTS", "10.0"))
    weight_title_quality: float = float(os.getenv("SCORE_WEIGHT_TITLE_QUALITY", "10.0"))


# ---------------------------------------------------------------------------
# OpenRouter (classificação por LLM)
# ---------------------------------------------------------------------------


@dataclass
class OpenRouterConfig:
    api_key: str = field(default_factory=lambda: os.getenv("OPENROUTER_API_KEY", ""))


# ---------------------------------------------------------------------------
# Serper.dev (busca de imagens)
# ---------------------------------------------------------------------------


@dataclass
class SerperConfig:
    api_key: str = field(default_factory=lambda: os.getenv("SERPER_API_KEY", ""))
    max_results: int = int(os.getenv("SERPER_MAX_RESULTS", "8"))


# ---------------------------------------------------------------------------
# Image Worker
# ---------------------------------------------------------------------------


@dataclass
class ImageWorkerConfig:
    enabled: bool = os.getenv("IMAGE_WORKER_ENABLED", "true").lower() == "true"
    poll_interval: int = int(os.getenv("IMAGE_WORKER_POLL_INTERVAL", "30"))
    batch_size: int = int(os.getenv("IMAGE_WORKER_BATCH_SIZE", "5"))
    min_resolution: int = int(os.getenv("IMAGE_WORKER_MIN_RESOLUTION", "400"))
    white_bg_threshold: float = float(os.getenv("IMAGE_WORKER_WHITE_BG_THRESHOLD", "0.70"))
    max_candidates_for_ai: int = int(os.getenv("IMAGE_WORKER_MAX_CANDIDATES_AI", "5"))
    supabase_bucket: str = os.getenv("IMAGE_WORKER_BUCKET", "images")


# ---------------------------------------------------------------------------
# n8n
# ---------------------------------------------------------------------------


@dataclass
class N8nConfig:
    webhook_url: str = field(default_factory=lambda: os.getenv("N8N_WEBHOOK_URL", ""))
    api_key: str = field(default_factory=lambda: os.getenv("N8N_API_KEY", ""))


# ---------------------------------------------------------------------------
# SQLite Fallback
# ---------------------------------------------------------------------------


@dataclass
class SQLiteConfig:
    db_path: Path = field(
        default_factory=lambda: Path(
            os.getenv("SQLITE_DB_PATH", str(ROOT_DIR / "data" / "dealhunter.db"))
        )
    )


# ---------------------------------------------------------------------------
# Configuração Global
# ---------------------------------------------------------------------------


@dataclass
class Settings:
    supabase: SupabaseConfig = field(default_factory=SupabaseConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    whatsapp: WhatsAppConfig = field(default_factory=WhatsAppConfig)
    mercado_livre: MercadoLivreConfig = field(default_factory=MercadoLivreConfig)
    scraper: ScraperConfig = field(default_factory=ScraperConfig)
    score: ScoreConfig = field(default_factory=ScoreConfig)
    openrouter: OpenRouterConfig = field(default_factory=OpenRouterConfig)
    serper: SerperConfig = field(default_factory=SerperConfig)
    image_worker: ImageWorkerConfig = field(default_factory=ImageWorkerConfig)
    n8n: N8nConfig = field(default_factory=N8nConfig)
    sqlite: SQLiteConfig = field(default_factory=SQLiteConfig)

    # Ambiente de execução
    env: str = field(default_factory=lambda: os.getenv("APP_ENV", "development"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    @property
    def is_production(self) -> bool:
        return self.env == "production"


# Instância singleton — importar de qualquer lugar com:
#   from src.config import settings
settings = Settings()
