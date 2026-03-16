"""
DealHunter — Telegram Bot
Publica ofertas nos grupos "Sempre Black" via Telegram Bot API.
Biblioteca: python-telegram-bot v21 (async)
"""

import asyncio
from typing import Optional

import structlog
from telegram import Bot
from telegram.error import RetryAfter, TelegramError
from telegram.constants import ParseMode

from src.config import settings
from .message_formatter import FormattedMessage

logger = structlog.get_logger(__name__)


class TelegramBot:
    """
    Publica mensagens nos grupos Telegram configurados.

    Uso:
        bot = TelegramBot()
        results = await bot.publish(formatted_message)
    """

    def __init__(self):
        self.cfg = settings.telegram
        self.bot = Bot(token=self.cfg.bot_token)

    async def publish(
        self,
        message: FormattedMessage,
        group_ids: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        Publica a oferta em todos os grupos configurados.

        Args:
            message: Mensagem formatada
            group_ids: Override da lista de grupos (usa config padrão se None)

        Returns:
            Lista de resultados por grupo (sucesso/falha e message_id)
        """
        targets = group_ids or self.cfg.group_ids
        results = []

        for group_id in targets:
            result = await self._send_to_group(group_id, message)
            results.append(result)

            # Delay entre grupos para evitar flood
            if len(targets) > 1:
                await asyncio.sleep(self.cfg.send_delay)

        return results

    async def _send_to_group(self, group_id: str, message: FormattedMessage) -> dict:
        """Envia mensagem para um grupo específico."""
        result = {"group_id": group_id, "success": False, "message_id": None}

        try:
            if message.image_url:
                sent = await self._send_photo(group_id, message)
            else:
                sent = await self._send_text(group_id, message)

            result["success"] = True
            result["message_id"] = sent.message_id
            logger.debug(
                "telegram_sent",
                group_id=group_id,
                message_id=sent.message_id,
                product_ml_id=message.product_ml_id,
            )

        except RetryAfter as exc:
            logger.warning(
                "telegram_rate_limited",
                group_id=group_id,
                retry_after=exc.retry_after,
            )
            await asyncio.sleep(exc.retry_after + 1)
            # Re-tenta uma vez após o flood wait
            return await self._send_to_group(group_id, message)

        except TelegramError as exc:
            # Fallback: tenta enviar sem parse_mode (texto puro com link visível)
            logger.warning(
                "telegram_mdv2_failed_retrying_plain",
                group_id=group_id,
                error=str(exc),
                product_ml_id=message.product_ml_id,
            )
            try:
                fallback_text = self._build_plaintext_fallback(message)
                if message.image_url:
                    sent = await self.bot.send_photo(
                        chat_id=group_id,
                        photo=message.image_url,
                        caption=fallback_text,
                    )
                else:
                    sent = await self.bot.send_message(
                        chat_id=group_id,
                        text=fallback_text,
                        disable_web_page_preview=False,
                    )
                result["success"] = True
                result["message_id"] = sent.message_id
                logger.debug(
                    "telegram_sent_plaintext_fallback",
                    group_id=group_id,
                    message_id=sent.message_id,
                    product_ml_id=message.product_ml_id,
                )
            except TelegramError as exc2:
                logger.error(
                    "telegram_error",
                    group_id=group_id,
                    error=str(exc2),
                    product_ml_id=message.product_ml_id,
                )
                result["error"] = str(exc2)

        return result

    @staticmethod
    def _build_plaintext_fallback(message: FormattedMessage) -> str:
        """Gera versão texto puro da mensagem (sem MarkdownV2) com link visível."""
        # Remove escapes de MarkdownV2 e syntax chars
        import re
        text = message.telegram_text
        # Remove escapes (\. \- \! etc.)
        text = re.sub(r'\\([_*\[\]()~`>#+=|{}.!\\-])', r'\1', text)
        # Converte [text](url) para text + url na linha seguinte
        text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'\1\n🔗 \2', text)
        # Remove ~~ (strikethrough syntax)
        text = text.replace('~~', '')
        # Remove * (bold syntax) — mantém o texto
        text = re.sub(r'\*([^*]+)\*', r'\1', text)
        # Remove _ (italic syntax)
        text = re.sub(r'_([^_]+)_', r'\1', text)
        return text

    async def _send_photo(self, group_id: str, message: FormattedMessage):
        """Envia mensagem com imagem do produto."""
        return await self.bot.send_photo(
            chat_id=group_id,
            photo=message.image_url,
            caption=message.telegram_text,
            parse_mode=ParseMode.MARKDOWN_V2,
        )

    async def _send_text(self, group_id: str, message: FormattedMessage):
        """Envia mensagem apenas em texto (sem imagem)."""
        return await self.bot.send_message(
            chat_id=group_id,
            text=message.telegram_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=False,  # Mostra preview do link
        )

    async def send_alert(self, text: str, group_id: Optional[str] = None) -> bool:
        """
        Envia alerta de sistema (erros, relatórios).
        Usa o primeiro grupo da lista ou o grupo especificado.
        """
        target = group_id or (self.cfg.group_ids[0] if self.cfg.group_ids else None)
        if not target:
            logger.warning("no_telegram_group_for_alert")
            return False

        try:
            await self.bot.send_message(
                chat_id=target,
                text=f"⚠️ *DealHunter Alert*\n\n{text}",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return True
        except TelegramError as exc:
            logger.error("telegram_alert_error", error=str(exc))
            return False

    async def test_connection(self) -> bool:
        """Verifica se o bot está conectado e configurado corretamente."""
        try:
            me = await self.bot.get_me()
            logger.info("telegram_connected", bot_username=me.username)
            return True
        except TelegramError as exc:
            logger.error("telegram_connection_error", error=str(exc))
            return False
