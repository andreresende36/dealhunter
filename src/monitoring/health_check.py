"""
DealHunter — Health Check
Verifica a saúde de todos os serviços do sistema.
Pode ser chamado pelo endpoint HTTP do n8n ou diretamente.
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import httpx
import structlog

from src.config import settings

logger = structlog.get_logger(__name__)


@dataclass
class ServiceStatus:
    name: str
    healthy: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    details: dict = field(default_factory=dict)


@dataclass
class HealthReport:
    timestamp: str
    overall_healthy: bool
    services: list[ServiceStatus] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "overall_healthy": self.overall_healthy,
            "services": [
                {
                    "name": s.name,
                    "healthy": s.healthy,
                    "latency_ms": s.latency_ms,
                    "error": s.error,
                    "details": s.details,
                }
                for s in self.services
            ],
        }

    def summary(self) -> str:
        lines = [f"Health Check — {self.timestamp}"]
        for s in self.services:
            icon = "✅" if s.healthy else "❌"
            latency = f" ({s.latency_ms:.0f}ms)" if s.latency_ms else ""
            error = f" — {s.error}" if s.error else ""
            lines.append(f"{icon} {s.name}{latency}{error}")
        return "\n".join(lines)


class HealthCheck:
    """
    Verifica a saúde dos serviços do DealHunter.

    Uso:
        checker = HealthCheck()
        report = await checker.run()
        if not report.overall_healthy:
            # disparar alerta
    """

    async def run(self) -> HealthReport:
        """Executa verificação completa de todos os serviços."""
        results = await asyncio.gather(
            self._check_supabase(),
            self._check_telegram(),
            self._check_whatsapp(),
            self._check_openrouter(),
            return_exceptions=True,
        )

        statuses = []
        for result in results:
            if isinstance(result, ServiceStatus):
                statuses.append(result)
            else:
                # Exception capturada pelo gather
                statuses.append(
                    ServiceStatus(
                        name="unknown",
                        healthy=False,
                        error=str(result),
                    )
                )

        overall = all(s.healthy for s in statuses)
        report = HealthReport(
            timestamp=datetime.now(tz=timezone.utc).isoformat(),
            overall_healthy=overall,
            services=statuses,
        )

        logger.info(
            "health_check_done",
            overall=overall,
            healthy=[s.name for s in statuses if s.healthy],
            unhealthy=[s.name for s in statuses if not s.healthy],
        )
        return report

    async def _check_supabase(self) -> ServiceStatus:
        """Verifica conexão com Supabase via health endpoint."""
        cfg = settings.supabase
        url = f"{cfg.url}/rest/v1/"
        start = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    url,
                    headers={"apikey": cfg.anon_key},
                )
            latency = (time.monotonic() - start) * 1000
            return ServiceStatus(
                name="supabase",
                healthy=response.status_code < 500,
                latency_ms=latency,
            )
        except Exception as exc:
            return ServiceStatus(name="supabase", healthy=False, error=str(exc))

    async def _check_telegram(self) -> ServiceStatus:
        """Verifica se o bot Telegram está respondendo."""
        cfg = settings.telegram
        start = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"https://api.telegram.org/bot{cfg.bot_token}/getMe"
                )
            latency = (time.monotonic() - start) * 1000
            data = response.json()
            return ServiceStatus(
                name="telegram",
                healthy=data.get("ok", False),
                latency_ms=latency,
                details={"bot_username": data.get("result", {}).get("username")},
            )
        except Exception as exc:
            return ServiceStatus(name="telegram", healthy=False, error=str(exc))

    async def _check_whatsapp(self) -> ServiceStatus:
        """Verifica conexão com Evolution API (WhatsApp)."""
        cfg = settings.whatsapp
        if not cfg.api_url:
            return ServiceStatus(
                name="whatsapp", healthy=True, details={"note": "not configured"}
            )

        start = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{cfg.api_url}/instance/fetchInstances",
                    headers={"apikey": cfg.api_key},
                )
            latency = (time.monotonic() - start) * 1000
            return ServiceStatus(
                name="whatsapp",
                healthy=response.status_code == 200,
                latency_ms=latency,
            )
        except Exception as exc:
            return ServiceStatus(name="whatsapp", healthy=False, error=str(exc))

    async def _check_openrouter(self) -> ServiceStatus:
        """Verifica se a chave do OpenRouter é válida."""
        api_key = settings.openrouter.api_key
        if not api_key:
            return ServiceStatus(
                name="openrouter", healthy=False, error="OPENROUTER_API_KEY not set"
            )
        start = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    "https://openrouter.ai/api/v1/models",
                    headers={"Authorization": f"Bearer {api_key}"},
                )
            latency = (time.monotonic() - start) * 1000
            return ServiceStatus(
                name="openrouter",
                healthy=response.status_code == 200,
                latency_ms=latency,
            )
        except Exception as exc:
            return ServiceStatus(name="openrouter", healthy=False, error=str(exc))
