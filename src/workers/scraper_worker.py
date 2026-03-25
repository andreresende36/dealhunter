"""
Crivo — Scraper Worker
Entry point isolado para o container `scraper`.
Roda o pipeline de scraping a cada SCRAPER_INTERVAL segundos.
Escreve next_scrape_time no Redis (quando USE_REDIS_STATE=true).

Uso:
  python -m src.workers.scraper_worker
"""

import asyncio
import signal
from datetime import datetime, timedelta

import structlog

from src.logging_config import setup_logging
from src.config import settings
from src.database.storage_manager import StorageManager
from src.scraper.pipeline import run_pipeline
from src.monitoring.alert_bot import AlertBot
from src.monitoring.health_check import HealthCheck
from src.monitoring.state import set_next_scrape_time

setup_logging()
logger = structlog.get_logger(__name__)


async def _interruptible_sleep(seconds: float, shutdown: asyncio.Event) -> None:
    """Dorme por `seconds` ou retorna imediatamente se shutdown for sinalizado."""
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def scraper_loop(
    storage: StorageManager,
    shutdown: asyncio.Event,
    alert_bot: AlertBot,
) -> None:
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

        await set_next_scrape_time(datetime.now() + timedelta(seconds=interval))
        await _interruptible_sleep(interval, shutdown)


async def main() -> None:
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    logger.info(
        "scraper_worker_starting",
        interval=f"{settings.scraper.interval}s",
        use_redis_state=settings.use_redis_state,
    )

    alert_bot = AlertBot()

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
        task = asyncio.create_task(scraper_loop(storage, shutdown, alert_bot))
        await shutdown.wait()
        logger.info("scraper_worker_shutting_down")
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    logger.info("scraper_worker_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
