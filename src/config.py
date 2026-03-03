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
# Claude API (Anthropic)
# ---------------------------------------------------------------------------


@dataclass
class ClaudeConfig:
    api_key: str = field(default_factory=lambda: os.environ["ANTHROPIC_API_KEY"])
    model: str = field(
        default_factory=lambda: os.getenv("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
    )
    max_tokens: int = int(os.getenv("CLAUDE_MAX_TOKENS", "1024"))
    # Temperatura para análise de ofertas (0 = determinístico)
    temperature: float = float(os.getenv("CLAUDE_TEMPERATURE", "0.2"))


# ---------------------------------------------------------------------------
# Shlink (encurtador de links)
# ---------------------------------------------------------------------------


@dataclass
class ShlinkConfig:
    api_url: str = field(default_factory=lambda: os.environ["SHLINK_API_URL"])
    api_key: str = field(default_factory=lambda: os.environ["SHLINK_API_KEY"])
    domain: str = field(default_factory=lambda: os.getenv("SHLINK_DOMAIN", ""))


# ---------------------------------------------------------------------------
# Mercado Livre Afiliados
# ---------------------------------------------------------------------------


@dataclass
class MercadoLivreConfig:
    affiliate_id: str = field(default_factory=lambda: os.getenv("ML_AFFILIATE_ID", ""))
    # Tag de rastreamento para os links
    affiliate_tag: str = field(
        default_factory=lambda: os.getenv("ML_AFFILIATE_TAG", "sempreblack")
    )
    # Categorias monitoradas (IDs do ML separados por vírgula)
    category_ids: list[str] = field(
        default_factory=lambda: [
            c.strip()
            for c in os.getenv("ML_CATEGORY_IDS", "MLB1430").split(",")
            if c.strip()
        ]
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
    # Proxy (opcional): "http://user:pass@host:port"
    proxy_url: Optional[str] = field(
        default_factory=lambda: os.getenv("SCRAPER_PROXY_URL")
    )


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
    # Pesos por critério (soma deve dar ~100)
    weight_discount: float = float(os.getenv("SCORE_WEIGHT_DISCOUNT", "30.0"))
    weight_rating: float = float(os.getenv("SCORE_WEIGHT_RATING", "15.0"))
    weight_reviews: float = float(os.getenv("SCORE_WEIGHT_REVIEWS", "10.0"))
    weight_free_shipping: float = float(os.getenv("SCORE_WEIGHT_FREE_SHIPPING", "10.0"))
    weight_official_store: float = float(
        os.getenv("SCORE_WEIGHT_OFFICIAL_STORE", "5.0")
    )
    weight_title_quality: float = float(os.getenv("SCORE_WEIGHT_TITLE_QUALITY", "5.0"))
    weight_seller_reputation: float = float(
        os.getenv("SCORE_WEIGHT_SELLER_REPUTATION", "15.0")
    )
    weight_sold_quantity: float = float(
        os.getenv("SCORE_WEIGHT_SOLD_QUANTITY", "10.0")
    )


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
# Deep Scrape Worker
# ---------------------------------------------------------------------------


@dataclass
class DeepScrapeConfig:
    # Delays mais conservadores que o scraper de listagem (anti-bot)
    delay_min: float = float(os.getenv("DEEP_SCRAPE_DELAY_MIN", "3.0"))
    delay_max: float = float(os.getenv("DEEP_SCRAPE_DELAY_MAX", "8.0"))
    # Intervalo entre polls ao DB quando a fila está vazia (segundos)
    poll_interval: int = int(os.getenv("DEEP_SCRAPE_POLL_INTERVAL", "30"))
    # Produtos por batch
    batch_size: int = int(os.getenv("DEEP_SCRAPE_BATCH_SIZE", "10"))
    # Concorrência máxima (abas simultâneas no browser)
    max_concurrent: int = int(os.getenv("DEEP_SCRAPE_MAX_CONCURRENT", "3"))
    # Retry de produtos com falha
    max_attempts: int = int(os.getenv("DEEP_SCRAPE_MAX_ATTEMPTS", "3"))
    backoff_base_minutes: int = int(os.getenv("DEEP_SCRAPE_BACKOFF_BASE_MIN", "5"))
    # Rotação de contexto mais agressiva (a cada N requests)
    context_rotation_every: int = int(
        os.getenv("DEEP_SCRAPE_CONTEXT_ROTATION", "10")
    )
    # Pausa ao detectar CAPTCHAs consecutivos
    captcha_pause_seconds: int = int(os.getenv("DEEP_SCRAPE_CAPTCHA_PAUSE", "300"))
    max_consecutive_captchas: int = int(os.getenv("DEEP_SCRAPE_MAX_CAPTCHAS", "3"))


# ---------------------------------------------------------------------------
# Configuração Global
# ---------------------------------------------------------------------------


@dataclass
class Settings:
    supabase: SupabaseConfig = field(default_factory=SupabaseConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    whatsapp: WhatsAppConfig = field(default_factory=WhatsAppConfig)
    claude: ClaudeConfig = field(default_factory=ClaudeConfig)
    shlink: ShlinkConfig = field(default_factory=ShlinkConfig)
    mercado_livre: MercadoLivreConfig = field(default_factory=MercadoLivreConfig)
    scraper: ScraperConfig = field(default_factory=ScraperConfig)
    score: ScoreConfig = field(default_factory=ScoreConfig)
    n8n: N8nConfig = field(default_factory=N8nConfig)
    sqlite: SQLiteConfig = field(default_factory=SQLiteConfig)
    deep_scrape: DeepScrapeConfig = field(default_factory=DeepScrapeConfig)

    # Ambiente de execução
    env: str = field(default_factory=lambda: os.getenv("APP_ENV", "development"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    @property
    def is_production(self) -> bool:
        return self.env == "production"


# Instância singleton — importar de qualquer lugar com:
#   from src.config import settings
settings = Settings()
