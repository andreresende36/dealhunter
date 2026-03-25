"""
Crivo — Redis State
Camada de estado compartilhado entre containers via Redis.
Substitui as class-variables do MonitorState quando USE_REDIS_STATE=true.

Chaves:
  crivo:state:next_scrape_time  → ISO 8601 string | TTL 7200s
  crivo:state:next_send_time    → ISO 8601 string | TTL 600s
  crivo:state:is_sending_hours  → "1" ou "0"      | TTL 120s
"""

from datetime import datetime
from typing import Optional

import redis.asyncio as redis

from src.config import settings

_PREFIX = "crivo:state:"
_client: Optional[redis.Redis] = None  # type: ignore[type-arg]


async def _get_client() -> redis.Redis:  # type: ignore[type-arg]
    """Singleton lazy de conexão Redis."""
    global _client
    if _client is None:
        _client = redis.from_url(settings.redis.url, decode_responses=True)
    return _client


async def close_redis() -> None:
    """Fecha a conexão Redis. Chamar no shutdown do worker."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


# ---------------------------------------------------------------------------
# Escritores (chamados por scraper_worker e sender_worker)
# ---------------------------------------------------------------------------


async def set_next_scrape_time(dt: datetime) -> None:
    r = await _get_client()
    await r.set(_PREFIX + "next_scrape_time", dt.isoformat(), ex=7200)


async def set_next_send_time(dt: datetime) -> None:
    r = await _get_client()
    await r.set(_PREFIX + "next_send_time", dt.isoformat(), ex=600)


async def set_is_sending_hours(value: bool) -> None:
    r = await _get_client()
    await r.set(_PREFIX + "is_sending_hours", "1" if value else "0", ex=120)


# ---------------------------------------------------------------------------
# Leitura (chamada pela api_worker via /api/state)
# ---------------------------------------------------------------------------


async def get_state() -> dict:
    """Lê o estado do Redis em round-trip único (pipeline)."""
    r = await _get_client()
    pipe = r.pipeline()
    pipe.get(_PREFIX + "next_scrape_time")
    pipe.get(_PREFIX + "next_send_time")
    pipe.get(_PREFIX + "is_sending_hours")
    results = await pipe.execute()
    return {
        "next_scrape_time": results[0],  # ISO string ou None
        "next_send_time": results[1],    # ISO string ou None
        "is_sending_hours": results[2] == "1",
        "server_time": datetime.now().isoformat(),
    }
