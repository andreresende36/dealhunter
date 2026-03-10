"""
DealHunter — Alert Bot
Envia alertas críticos via Telegram para o admin.
Usado quando há falhas, erros inesperados ou limites atingidos.
"""

from datetime import datetime
from typing import Optional

import httpx
import structlog

from src.config import settings

logger = structlog.get_logger(__name__)

# Ícones por severidade
SEVERITY_ICONS = {
    "info": "ℹ️",
    "warning": "⚠️",
    "error": "🔴",
    "critical": "🚨",
}


class AlertBot:
    """
    Envia alertas para o admin via Telegram.

    Usa a Bot API diretamente (sem python-telegram-bot) para ser leve
    e funcionar mesmo se o módulo distributor estiver com problemas.

    Uso:
        alert = AlertBot()
        await alert.send("Scraper falhou 3 vezes consecutivas", severity="error")
    """

    def __init__(self, admin_chat_id: Optional[str] = None):
        self.cfg = settings.telegram
        self.admin_chat_id = (
            admin_chat_id
            or self.cfg.admin_chat_id
            or (self.cfg.group_ids[0] if self.cfg.group_ids else "")
        )
        self.base_url = f"https://api.telegram.org/bot{self.cfg.bot_token}"

    async def send(
        self,
        message: str,
        severity: str = "info",
        details: Optional[dict] = None,
    ) -> bool:
        """
        Envia alerta para o admin.

        Args:
            message: Mensagem principal do alerta
            severity: "info" | "warning" | "error" | "critical"
            details: Dicionário com dados adicionais para debug
        """
        if not self.admin_chat_id or not self.cfg.bot_token:
            logger.warning("alert_bot_not_configured")
            return False

        icon = SEVERITY_ICONS.get(severity, "ℹ️")
        text = f"{icon} *DealHunter — {severity.upper()}*\n\n{message}"

        if details:
            detail_lines = "\n".join(f"• `{k}`: {v}" for k, v in details.items())
            text += f"\n\n*Detalhes:*\n{detail_lines}"

        return await self._send_telegram_message(text)

    async def send_health_report(self, report_summary: str) -> bool:
        """Envia relatório de health check."""
        return await self._send_telegram_message(
            f"📊 *Health Check*\n\n```\n{report_summary}\n```"
        )

    async def _send_telegram_message(self, text: str) -> bool:
        """Envia mensagem via Bot API REST."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.base_url}/sendMessage",
                    json={
                        "chat_id": self.admin_chat_id,
                        "text": text,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": True,
                    },
                )
                data = response.json()
                if data.get("ok"):
                    return True
                logger.error("alert_send_failed", response=data)
                return False
        except Exception as exc:
            logger.error("alert_bot_error", error=str(exc))
            return False

    async def send_startup(self) -> bool:
        """Notifica que o sistema foi iniciado."""
        return await self.send(
            f"Sistema iniciado com sucesso\n"
            f"Horário: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
            severity="info",
        )

    async def send_error(self, error: Exception, context: str = "") -> bool:
        """Atalho para enviar erros de exceção."""
        return await self.send(
            f"{'[' + context + '] ' if context else ''}{type(error).__name__}: {str(error)}",
            severity="error",
        )
