"""
DealHunter — Runner (Style Guide v3)
Processo long-running com 2 coroutines:
  - scraper_loop: roda o pipeline de scraping a cada 1h (24/7)
  - sender_loop: envia ofertas da fila com distribuição temporal (8h-23h BRT)

Distribuição por janela de horário (style guide v3):
  08h-10h: 28% do volume (~36 ofertas)
  10h-13h: 14% (~18 ofertas)
  13h-16h: 30% (~39 ofertas)
  16h-18h: 13% (~17 ofertas)
  18h-23h: 15% (~20 ofertas)

Multiplicadores por dia da semana:
  Sexta: +20%  |  Terça: -15%  |  Demais: normal
"""

import asyncio
import random
import signal
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import structlog

from src.logging_config import setup_logging
from src.config import settings
from src.database.storage_manager import StorageManager
from src.main import run_pipeline
from src.sender import send_next_offer
from src.monitoring.alert_bot import AlertBot
from src.monitoring.health_check import HealthCheck
from src.monitoring.state import MonitorState
import uvicorn

setup_logging()
logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Distribuição temporal (Style Guide v3)
# ---------------------------------------------------------------------------

# Peso de cada janela de horário (proporção do volume diário)
TIME_WINDOWS: list[tuple[int, int, float]] = [
    (8, 10, 0.28),    # 28% do volume
    (10, 13, 0.14),   # 14%
    (13, 16, 0.30),   # 30%
    (16, 18, 0.13),   # 13%
    (18, 23, 0.15),   # 15%
]

# Multiplicadores por dia da semana (0=Monday ... 6=Sunday)
DAY_MULTIPLIERS: dict[int, float] = {
    0: 1.0,     # Segunda
    1: 0.85,    # Terça (-15%)
    2: 1.0,     # Quarta
    3: 1.0,     # Quinta
    4: 1.20,    # Sexta (+20%)
    5: 1.0,     # Sábado
    6: 1.0,     # Domingo
}


def _get_window_weight(hour: int) -> float:
    """Retorna o peso da janela de horário atual."""
    for start, end, weight in TIME_WINDOWS:
        if start <= hour < end:
            return weight
    return 0.14  # Fallback para janelas não mapeadas


def calculate_send_interval() -> float:
    """
    Calcula intervalo de envio em minutos baseado na janela de horário
    e dia da semana.

    Janelas de pico (peso alto) → intervalos menores.
    Janelas fracas (peso baixo) → intervalos maiores.
    """
    tz = ZoneInfo(settings.sender.timezone)
    now = datetime.now(tz)

    base_min = settings.sender.min_interval
    base_max = settings.sender.max_interval
    base_avg = (base_min + base_max) / 2

    # Peso da janela atual (0.13 a 0.30)
    window_weight = _get_window_weight(now.hour)

    # Peso médio para normalização (todos os pesos / número de janelas)
    avg_weight = sum(w for _, _, w in TIME_WINDOWS) / len(TIME_WINDOWS)

    # Fator de ajuste: inversamente proporcional ao peso
    # Janela com peso alto → fator < 1 → intervalo menor
    # Janela com peso baixo → fator > 1 → intervalo maior
    factor = avg_weight / window_weight if window_weight > 0 else 1.0

    # Multiplicador do dia da semana
    day_mult = DAY_MULTIPLIERS.get(now.weekday(), 1.0)
    # Dia com mais volume → intervalo menor (inversamente proporcional)
    factor /= day_mult

    # Calcula intervalo ajustado
    adjusted_avg = base_avg * factor

    # Adiciona variação aleatória (±30%)
    jitter = random.uniform(0.7, 1.3)
    interval = adjusted_avg * jitter

    # Clamp entre limites razoáveis
    interval = max(base_min, min(interval, base_max * 2))

    return interval


async def _interruptible_sleep(seconds: float, shutdown: asyncio.Event) -> None:
    """Dorme por `seconds` ou retorna imediatamente se shutdown for sinalizado."""
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass  # Timeout normal — continuar loop


def is_sending_hours() -> bool:
    """Verifica se está no horário de envio (BRT)."""
    tz = ZoneInfo(settings.sender.timezone)
    now = datetime.now(tz)
    return settings.sender.start_hour <= now.hour < settings.sender.end_hour


async def scraper_loop(
    storage: StorageManager,
    shutdown: asyncio.Event,
    alert_bot: AlertBot,
) -> None:
    """Executa o pipeline de scraping a cada intervalo configurado."""
    interval = settings.scraper.interval

    while not shutdown.is_set():
        try:
            stats = await run_pipeline(storage)
            logger.info(
                "scraper_cycle_done",
                approved=stats.get("approved", 0),
                saved=stats.get("saved", 0),
                timings=stats.get("timings", {}),
            )
        except Exception as exc:
            logger.error("scraper_loop_error", error=str(exc))
            try:
                await alert_bot.send_error(exc, context="scraper_loop")
            except Exception:
                pass

        MonitorState.next_scrape_time = datetime.now() + timedelta(seconds=interval)
        await _interruptible_sleep(interval, shutdown)


async def sender_loop(
    storage: StorageManager,
    shutdown: asyncio.Event,
    alert_bot: AlertBot,
) -> None:
    """Envia ofertas da fila com distribuição temporal do Style Guide v3."""

    while not shutdown.is_set():
        is_sending = is_sending_hours()
        MonitorState.is_sending_hours = is_sending
        if not is_sending:
            tz = ZoneInfo(settings.sender.timezone)
            now = datetime.now(tz)
            logger.debug(
                "sender_outside_hours",
                current_hour=now.hour,
                window=f"{settings.sender.start_hour}h-{settings.sender.end_hour}h",
            )
            MonitorState.next_send_time = datetime.now() + timedelta(seconds=60)
            await _interruptible_sleep(60, shutdown)
            continue

        try:
            sent = await send_next_offer(storage)
            if not sent:
                logger.debug("sender_queue_empty_waiting")
        except Exception as exc:
            logger.error("sender_loop_error", error=str(exc))
            try:
                await alert_bot.send_error(exc, context="sender_loop")
            except Exception:
                pass

        # Intervalo ajustado por janela de horário e dia da semana
        delay_minutes = calculate_send_interval()
        tz = ZoneInfo(settings.sender.timezone)
        now = datetime.now(tz)
        window_weight = _get_window_weight(now.hour)
        logger.debug(
            "sender_next_in",
            minutes=delay_minutes,
            hour=now.hour,
            window_weight=window_weight,
            day=now.strftime("%A"),
        )
        MonitorState.next_send_time = datetime.now() + timedelta(minutes=delay_minutes)
        await _interruptible_sleep(delay_minutes * 60, shutdown)


async def api_loop(shutdown: asyncio.Event) -> None:
    """Roda a API web usando Uvicorn."""
    config = uvicorn.Config(
        "src.api.monitor:app", host="0.0.0.0", port=8000, log_level="warning"
    )
    server = uvicorn.Server(config)

    # Faz o server encerrar junto com o shutdown event
    async def watch_shutdown():
        await shutdown.wait()
        server.should_exit = True

    asyncio.create_task(watch_shutdown())
    await server.serve()


async def main() -> None:
    """Entry point principal — inicia scraper e sender em paralelo."""
    shutdown = asyncio.Event()

    # Signal handlers para shutdown graceful
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    logger.info(
        "runner_starting",
        scrape_interval=f"{settings.scraper.interval}s",
        send_window=f"{settings.sender.start_hour}h-{settings.sender.end_hour}h",
        send_interval=f"{settings.sender.min_interval}-{settings.sender.max_interval}min",
        timezone=settings.sender.timezone,
    )

    # AlertBot instanciado uma vez e injetado nos dois loops
    alert_bot = AlertBot()

    # Health check inicial
    try:
        checker = HealthCheck()
        report = await checker.run()
        if not report.overall_healthy:
            logger.warning("unhealthy_services", summary=report.summary())
        else:
            logger.info("health_check_ok")
    except Exception as exc:
        logger.warning("health_check_failed", error=str(exc))

    async with StorageManager() as storage:
        await asyncio.gather(
            scraper_loop(storage, shutdown, alert_bot),
            sender_loop(storage, shutdown, alert_bot),
            api_loop(shutdown),
        )

    logger.info("runner_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
