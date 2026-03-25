"""
Crivo — Sender Worker
Entry point isolado para o container `sender`.
Envia ofertas da fila com distribuição temporal Style Guide v3 (8h-23h BRT).
Escreve next_send_time e is_sending_hours no Redis (quando USE_REDIS_STATE=true).

Uso:
  python -m src.workers.sender_worker
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
from src.distributor.sender import send_next_offer
from src.monitoring.alert_bot import AlertBot
from src.monitoring.state import set_next_send_time, set_is_sending_hours
from src.distributor.title_review_bot import TitleReviewBot

setup_logging()
logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Distribuição temporal (Style Guide v3) — copiado do runner.py
# ---------------------------------------------------------------------------

TIME_WINDOWS: list[tuple[int, int, float]] = [
    (8, 10, 0.28),
    (10, 13, 0.14),
    (13, 16, 0.30),
    (16, 18, 0.13),
    (18, 23, 0.15),
]

DAY_MULTIPLIERS: dict[int, float] = {
    0: 1.0,   # Segunda
    1: 0.85,  # Terça (-15%)
    2: 1.0,   # Quarta
    3: 1.0,   # Quinta
    4: 1.20,  # Sexta (+20%)
    5: 1.0,   # Sábado
    6: 1.0,   # Domingo
}


def _get_window_weight(hour: int) -> float:
    for start, end, weight in TIME_WINDOWS:
        if start <= hour < end:
            return weight
    return 0.14


def calculate_send_interval() -> float:
    tz = ZoneInfo(settings.sender.timezone)
    now = datetime.now(tz)
    base_min = settings.sender.min_interval
    base_max = settings.sender.max_interval
    base_avg = (base_min + base_max) / 2
    window_weight = _get_window_weight(now.hour)
    avg_weight = sum(w for _, _, w in TIME_WINDOWS) / len(TIME_WINDOWS)
    factor = avg_weight / window_weight if window_weight > 0 else 1.0
    day_mult = DAY_MULTIPLIERS.get(now.weekday(), 1.0)
    factor /= day_mult
    adjusted_avg = base_avg * factor
    jitter = random.uniform(0.7, 1.3)
    interval = adjusted_avg * jitter
    return max(base_min, min(interval, base_max * 2))


def is_sending_hours() -> bool:
    if settings.test_mode:
        return True
    tz = ZoneInfo(settings.sender.timezone)
    now = datetime.now(tz)
    return settings.sender.start_hour <= now.hour < settings.sender.end_hour


async def _interruptible_sleep(seconds: float, shutdown: asyncio.Event) -> None:
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------


async def sender_loop(
    storage: StorageManager,
    shutdown: asyncio.Event,
    alert_bot: AlertBot,
    title_review_bot: TitleReviewBot | None = None,
) -> None:
    while not shutdown.is_set():
        is_sending = is_sending_hours()
        await set_is_sending_hours(is_sending)

        if not is_sending:
            tz = ZoneInfo(settings.sender.timezone)
            now = datetime.now(tz)
            logger.debug(
                "sender_outside_hours",
                current_hour=now.hour,
                window=f"{settings.sender.start_hour}h-{settings.sender.end_hour}h",
            )
            await set_next_send_time(datetime.now() + timedelta(seconds=60))
            await _interruptible_sleep(60, shutdown)
            continue

        try:
            sent = await send_next_offer(storage, title_review_bot=title_review_bot)
            if not sent:
                logger.debug("sender_queue_empty_waiting")
        except Exception as exc:
            logger.error("sender_loop_error", error=str(exc))
            try:
                await alert_bot.send_error(exc, context="sender_loop")
            except Exception:
                pass

        delay_minutes = calculate_send_interval()
        tz = ZoneInfo(settings.sender.timezone)
        now = datetime.now(tz)
        logger.debug(
            "sender_next_in",
            minutes=delay_minutes,
            hour=now.hour,
            window_weight=_get_window_weight(now.hour),
            day=now.strftime("%A"),
        )
        await set_next_send_time(datetime.now() + timedelta(minutes=delay_minutes))
        await _interruptible_sleep(delay_minutes * 60, shutdown)


async def main() -> None:
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    logger.info(
        "sender_worker_starting",
        window=f"{settings.sender.start_hour}h-{settings.sender.end_hour}h",
        interval=f"{settings.sender.min_interval}-{settings.sender.max_interval}min",
        use_redis_state=settings.use_redis_state,
    )

    alert_bot = AlertBot()

    title_review_bot: TitleReviewBot | None = None
    if settings.title_review.enabled:
        title_review_bot = TitleReviewBot()
        try:
            await title_review_bot.start()
            logger.info("title_review_enabled")
        except Exception as exc:
            logger.warning("title_review_bot_start_failed", error=str(exc))
            title_review_bot = None

    async with StorageManager() as storage:
        task = asyncio.create_task(
            sender_loop(storage, shutdown, alert_bot, title_review_bot)
        )
        await shutdown.wait()
        logger.info("sender_worker_shutting_down")

        if title_review_bot:
            try:
                await title_review_bot.stop()
            except Exception as exc:
                logger.warning("title_review_bot_stop_error", error=str(exc))

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    logger.info("sender_worker_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
