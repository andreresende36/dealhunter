"""
DealHunter — Runner
Processo long-running com 2 coroutines:
  - scraper_loop: roda o pipeline de scraping a cada 1h (24/7)
  - sender_loop: envia ofertas da fila a cada 3-6 min (8h-23h BRT)
"""

import asyncio
import random
import signal
from datetime import datetime
from zoneinfo import ZoneInfo

import structlog

from src.logging_config import setup_logging
from src.config import settings
from src.database.storage_manager import StorageManager
from src.main import run_pipeline
from src.sender import send_next_offer
from src.monitoring.alert_bot import AlertBot
from src.monitoring.health_check import HealthCheck

setup_logging()
logger = structlog.get_logger(__name__)


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

        await _interruptible_sleep(interval, shutdown)


async def sender_loop(
    storage: StorageManager,
    shutdown: asyncio.Event,
    alert_bot: AlertBot,
) -> None:
    """Envia ofertas da fila com intervalo aleatório, só em horário comercial."""
    min_interval = settings.sender.min_interval
    max_interval = settings.sender.max_interval

    while not shutdown.is_set():
        if not is_sending_hours():
            tz = ZoneInfo(settings.sender.timezone)
            now = datetime.now(tz)
            logger.debug(
                "sender_outside_hours",
                current_hour=now.hour,
                window=f"{settings.sender.start_hour}h-{settings.sender.end_hour}h",
            )
            # Verifica a cada 60s se entrou no horário
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

        # Intervalo aleatório entre envios
        delay_minutes = random.randint(min_interval, max_interval)
        logger.debug("sender_next_in", minutes=delay_minutes)
        await _interruptible_sleep(delay_minutes * 60, shutdown)


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
        )

    logger.info("runner_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
