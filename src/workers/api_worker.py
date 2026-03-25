"""
Crivo — API Worker
Entry point isolado para o container `api`.
Serve FastAPI/Uvicorn na porta 8000. Lê estado do Redis via /api/state.

Uso:
  python -m src.workers.api_worker
"""

import asyncio
import signal

import structlog
import uvicorn

from src.logging_config import setup_logging

setup_logging()
logger = structlog.get_logger(__name__)


async def main() -> None:
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    logger.info("api_worker_starting", port=8000)

    config = uvicorn.Config(
        "src.api.monitor:app",
        host="0.0.0.0",
        port=8000,
        log_level="warning",
    )
    server = uvicorn.Server(config)

    async def _watch_shutdown() -> None:
        await shutdown.wait()
        server.should_exit = True

    watcher = asyncio.create_task(_watch_shutdown())
    await server.serve()
    watcher.cancel()
    logger.info("api_worker_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(main())
