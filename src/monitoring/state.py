"""
DealHunter Monitor - Shared State
Armazena variáveis em memória para exibir na interface web em tempo real.
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
