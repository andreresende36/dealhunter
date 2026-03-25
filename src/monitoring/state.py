"""
Crivo Monitor - Shared State
Armazena variáveis em memória para exibir na interface web em tempo real.

Quando USE_REDIS_STATE=true (containers Docker), as funções abaixo despacham
para src.monitoring.redis_state, permitindo que scraper, sender e api
compartilhem estado entre processos distintos.

Quando USE_REDIS_STATE=false (dev local / runner.py monolítico), o estado
fica nas class-variables de MonitorState — sem dependência de Redis.
"""

from datetime import datetime
from typing import Optional


class MonitorState:
    """Estado global em memória para o monitor web."""

    # Timers (armazenam o momento exato em que a próxima ação vai ocorrer)
    next_scrape_time: Optional[datetime] = None
    next_send_time: Optional[datetime] = None

    # Status
    is_sending_hours: bool = False

    @classmethod
    def get_state(cls) -> dict:
        """Retorna o estado serializável para a API."""
        return {
            "next_scrape_time": cls.next_scrape_time.isoformat() if cls.next_scrape_time else None,
            "next_send_time": cls.next_send_time.isoformat() if cls.next_send_time else None,
            "is_sending_hours": cls.is_sending_hours,
            "server_time": datetime.now().isoformat(),
        }


# Instância singleton exportada
state = MonitorState()


# ---------------------------------------------------------------------------
# Funções unificadas (despacham para Redis ou in-memory conforme config)
# ---------------------------------------------------------------------------


async def set_next_scrape_time(dt: datetime) -> None:
    from src.config import settings
    if settings.use_redis_state:
        from src.monitoring.redis_state import set_next_scrape_time as _set
        await _set(dt)
    else:
        MonitorState.next_scrape_time = dt


async def set_next_send_time(dt: datetime) -> None:
    from src.config import settings
    if settings.use_redis_state:
        from src.monitoring.redis_state import set_next_send_time as _set
        await _set(dt)
    else:
        MonitorState.next_send_time = dt


async def set_is_sending_hours(value: bool) -> None:
    from src.config import settings
    if settings.use_redis_state:
        from src.monitoring.redis_state import set_is_sending_hours as _set
        await _set(value)
    else:
        MonitorState.is_sending_hours = value


async def read_state() -> dict:
    from src.config import settings
    if settings.use_redis_state:
        from src.monitoring.redis_state import get_state
        return await get_state()
    return MonitorState.get_state()
