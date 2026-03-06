"""
DealHunter — Configuração de Logging
Setup centralizado do structlog com redação de dados sensíveis
e formatação visual rica para o console (desenvolvimento).

Uso:
    from src.logging_config import setup_logging
    setup_logging()
"""

import logging
import re
import sys
from datetime import datetime

import structlog

from src.config import settings

# ---------------------------------------------------------------------------
# Redação de dados sensíveis nos logs
# ---------------------------------------------------------------------------
_SENSITIVE_PATTERNS = [
    (re.compile(r"(sk-ant-api\w{2}-)[\w-]+"), r"\1****"),  # Anthropic API keys
    (re.compile(r"(eyJ[\w-]+\.eyJ[\w-]+)\.[\w-]+"), r"\1.****"),  # JWTs (Supabase keys)
    (re.compile(r"(Bearer\s+)[\w.-]+"), r"\1****"),  # Bearer tokens
    (re.compile(r"(apikey[=:\s]+)[\w-]+", re.I), r"\1****"),  # API keys genéricos
    (re.compile(r"(\d{6,}:[\w-]{30,})"), "****:****"),  # Telegram bot tokens
]


def _redact_sensitive_data(logger, method_name, event_dict):
    """Processador structlog que mascara dados sensíveis nos valores dos logs."""
    for key, value in event_dict.items():
        if not isinstance(value, str):
            continue
        for pattern, replacement in _SENSITIVE_PATTERNS:
            value = pattern.sub(replacement, value)
        event_dict[key] = value
    return event_dict


# ---------------------------------------------------------------------------
# Cores ANSI
# ---------------------------------------------------------------------------
class _C:
    """Códigos ANSI para colorir o terminal."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    # Cores
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    MAGENTA = "\033[35m"
    BLUE = "\033[34m"
    WHITE = "\033[97m"
    GRAY = "\033[90m"
    # Negrito + cor
    B_CYAN = "\033[1;36m"
    B_GREEN = "\033[1;32m"
    B_YELLOW = "\033[1;33m"
    B_RED = "\033[1;31m"
    B_MAGENTA = "\033[1;35m"
    B_BLUE = "\033[1;34m"
    B_WHITE = "\033[1;97m"


# ---------------------------------------------------------------------------
# Mapeamento de eventos para formatação rica
# ---------------------------------------------------------------------------
_LEVEL_STYLE = {
    "debug": (_C.GRAY, "🔍"),
    "info": (_C.CYAN, "ℹ️ "),
    "warning": (_C.YELLOW, "⚠️ "),
    "error": (_C.RED, "❌"),
    "critical": (_C.B_RED, "🔥"),
}

# Eventos especiais com formatação customizada
_EVENT_CONFIG = {
    # ── Pipeline ──────────────────────────────────────────────────────
    "pipeline_start": ("🚀", _C.B_CYAN, "PIPELINE INICIADO"),
    "pipeline_done": ("✅", _C.B_GREEN, "PIPELINE CONCLUÍDO"),
    "execution_complete": ("🏁", _C.B_GREEN, "EXECUÇÃO FINALIZADA"),
    # ── Scraper ───────────────────────────────────────────────────────
    "scraping_source": ("🌐", _C.B_BLUE, "INICIANDO SCRAPING"),
    "scraping_page": ("📄", _C.BLUE, None),  # None = formato custom inline
    "page_parsed": ("📦", _C.GREEN, None),
    "source_done": ("✔️ ", _C.B_GREEN, "FONTE CONCLUÍDA"),
    "scraping_done": ("📊", _C.B_CYAN, "SCRAPING FINALIZADO"),
    "no_more_pages": ("🛑", _C.YELLOW, None),
    "no_cards_found": ("⚠️ ", _C.YELLOW, None),
    # ── Dedup & Filtros ───────────────────────────────────────────────
    "dedup_done": ("🔎", _C.CYAN, None),
    "fake_filter_done": ("🛡️ ", _C.CYAN, None),
    "scraper_done": ("📋", _C.B_GREEN, "COLETA FINALIZADA"),
    "batch_fake_check": ("🛡️ ", _C.CYAN, None),
    # ── Banco de Dados ────────────────────────────────────────────────
    "save_failed": ("💾", _C.RED, None),
    "storage_error": ("💾", _C.YELLOW, None),
    # ── Health Check ──────────────────────────────────────────────────
    "health_check_done": ("🏥", _C.CYAN, None),
    # ── Erros ─────────────────────────────────────────────────────────
    "captcha_blocked": ("🤖", _C.B_RED, "CAPTCHA DETECTADO"),
    "rate_limited": ("🚫", _C.B_RED, "RATE LIMITED"),
    "source_failed": ("💥", _C.RED, None),
    "scraper_failed": ("💥", _C.B_RED, "SCRAPER FALHOU"),
    "no_products_scraped": ("😔", _C.YELLOW, "NENHUM PRODUTO COLETADO"),
    # ── Anti-bloqueio ────────────────────────────────────────────────
    "delay": ("⏳", _C.DIM, None),
    "rotating_context": ("🔄", _C.DIM, None),
    "unhealthy_services": ("⚠️ ", _C.B_YELLOW, "SERVIÇOS COM PROBLEMA"),
}


def _format_value(key: str, value) -> str:
    """Formata um valor de log para exibição rica."""
    if key == "elapsed_seconds":
        return f"{_C.B_WHITE}{value}s{_C.RESET}"
    if key == "url":
        # Trunca URLs longas
        s = str(value)
        if len(s) > 60:
            s = s[:57] + "..."
        return f"{_C.DIM}{s}{_C.RESET}"
    if key in ("count", "total", "raw_count", "scraped", "saved", "new"):
        return f"{_C.B_WHITE}{value}{_C.RESET}"
    if key in ("genuine",):
        return f"{_C.B_GREEN}{value}{_C.RESET}"
    if key in ("fake", "dupes_skipped", "errors"):
        color = _C.B_RED if value and int(value) > 0 else _C.DIM
        return f"{color}{value}{_C.RESET}"
    if key in ("error",):
        return f"{_C.RED}{value}{_C.RESET}"
    if key in ("page", "max_pages", "sources"):
        return f"{_C.WHITE}{value}{_C.RESET}"
    if key in ("seconds",):
        return f"{_C.DIM}{value}s{_C.RESET}"
    return str(value)


def _render_banner(emoji: str, color: str, title: str, kv: dict) -> str:
    """Renderiza um evento como um banner destacado."""
    line = f"\n{color}{'─' * 60}{_C.RESET}"
    header = f"{emoji}  {color}{title}{_C.RESET}"

    parts = [line, header]
    if kv:
        details = []
        for k, v in kv.items():
            label = k.replace("_", " ").title()
            details.append(f"   {_C.DIM}▸{_C.RESET} {label}: {_format_value(k, v)}")
        parts.extend(details)
    parts.append(f"{color}{'─' * 60}{_C.RESET}")
    return "\n".join(parts)


def _render_inline(emoji: str, color: str, event: str, kv: dict) -> str:
    """Renderiza um evento como uma linha inline formatada."""
    label = event.replace("_", " ").title()
    parts = [f"{emoji}  {color}{label}{_C.RESET}"]

    if kv:
        pairs = []
        for k, v in kv.items():
            pairs.append(f"{_C.DIM}{k}={_C.RESET}{_format_value(k, v)}")
        parts.append("  ".join(pairs))

    return "  ".join(parts)


def _dealhunter_renderer(logger, method_name, event_dict):
    """
    Renderer customizado do DealHunter para o console.
    Formata eventos com emojis, cores e banners visuais.
    """
    # Extrai campos de controle do structlog
    timestamp = event_dict.pop("timestamp", "")
    level = event_dict.pop("level", "info")
    event = event_dict.pop("event", "")

    # Timestamp formatado
    try:
        dt = datetime.fromisoformat(timestamp)
        ts = f"{_C.DIM}{dt.strftime('%H:%M:%S')}{_C.RESET}"
    except (ValueError, TypeError):
        ts = f"{_C.DIM}{timestamp}{_C.RESET}"

    # Nível de log
    level_color, level_emoji = _LEVEL_STYLE.get(level, (_C.RESET, ""))

    # Verifica se é um evento especial
    config = _EVENT_CONFIG.get(event)

    if config:
        emoji, color, title = config
        kv = {k: v for k, v in event_dict.items() if k != "logger"}
        if title:
            # Banner destacado
            formatted = _render_banner(emoji, color, title, kv)
            return f"{ts}  {formatted}"
        else:
            # Inline formatado
            formatted = _render_inline(emoji, color, event, kv)
            return f"{ts}  {formatted}"

    # Eventos genéricos — fallback para formato padrão com cores
    kv_parts = []
    for k, v in event_dict.items():
        if k == "logger":
            continue
        kv_parts.append(f"{_C.DIM}{k}={_C.RESET}{_format_value(k, v)}")

    kv_str = f"  {' '.join(kv_parts)}" if kv_parts else ""
    return f"{ts}  {level_emoji}  {level_color}{event}{_C.RESET}{kv_str}"


def setup_logging() -> None:
    """Configura logging estruturado com formatação visual rica."""
    is_tty = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    if is_tty:
        # Terminal interativo → sempre usa o renderer visual rico
        renderer = _dealhunter_renderer
    elif not settings.is_production:
        renderer = structlog.dev.ConsoleRenderer()
    else:
        # Ambiente não-interativo (Docker, pipes, CI) → JSON
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            _redact_sensitive_data,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
    )
