"""
Crivo — Title Review Bot
Bot interativo de Telegram para revisão de títulos pelo admin.

Fluxo:
  1. sender_loop gera um título via IA
  2. request_review() envia o título ao admin com botões inline
  3. Admin pode: Aprovar / Rejeitar / Editar
  4. O resultado é retornado ao sender_loop via asyncio.Future
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass

import structlog
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from src.config import settings

logger = structlog.get_logger(__name__)


@dataclass
class TitleReviewResult:
    """Resultado da revisão de um título pelo admin."""

    action: str  # "approved" | "rejected" | "edited" | "timeout"
    final_title: str
    generated_title: str


class TitleReviewBot:
    """
    Bot interativo para revisão de títulos via Telegram.

    Usa telegram.ext.Application com polling para receber callbacks
    dos botões inline e mensagens de texto (edição).

    Não conflita com TelegramBot existente (que só envia, nunca faz polling).
    """

    def __init__(self) -> None:
        self._app: Application | None = None
        self._pending: dict[str, asyncio.Future[TitleReviewResult]] = {}
        self._editing: dict[str, str] = {}  # request_id -> generated_title
        self._admin_chat_id = int(settings.telegram.admin_chat_id)
        # Usa o bot de alertas (mesmo bot do grupo SempreBlack Monitor)
        self._bot_token = (
            settings.telegram.alert_bot_token or settings.telegram.bot_token
        )
        self._loop: asyncio.AbstractEventLoop | None = None

    async def start(self) -> None:
        """Inicializa e começa o polling de updates."""
        self._loop = asyncio.get_running_loop()
        self._app = (
            Application.builder()
            .token(self._bot_token)
            .build()
        )

        # Handlers
        self._app.add_handler(
            CallbackQueryHandler(self._on_callback, pattern=r"^title_")
        )
        self._app.add_handler(
            MessageHandler(
                filters.TEXT & filters.Chat(self._admin_chat_id),
                self._on_text_message,
            )
        )

        await self._app.initialize()
        await self._app.start()
        if self._app.updater:
            await self._app.updater.start_polling(drop_pending_updates=True)

        # Testa envio pro admin
        try:
            await self._app.bot.send_message(
                chat_id=self._admin_chat_id,
                text="🤖 Title Review Bot iniciado.",
            )
        except Exception as exc:
            logger.warning(
                "title_review_bot_test_failed",
                admin_chat_id=self._admin_chat_id,
                error=str(exc),
            )

        logger.info(
            "title_review_bot_started",
            admin_chat_id=self._admin_chat_id,
        )

    async def stop(self) -> None:
        """Para o polling e resolve futures pendentes como timeout."""
        # Resolve pendentes
        for request_id, future in self._pending.items():
            if not future.done():
                gen_title = self._editing.get(request_id, "")
                future.set_result(
                    TitleReviewResult(
                        action="timeout",
                        final_title=gen_title,
                        generated_title=gen_title,
                    )
                )
        self._pending.clear()
        self._editing.clear()

        if self._app:
            try:
                if self._app.updater and self._app.updater.running:
                    await self._app.updater.stop()
            except Exception as exc:
                logger.debug("updater_stop_error", error=str(exc))
            try:
                if self._app.running:
                    await self._app.stop()
            except Exception as exc:
                logger.debug("app_stop_error", error=str(exc))
            try:
                await self._app.shutdown()
            except Exception as exc:
                logger.debug("app_shutdown_error", error=str(exc))

        logger.info("title_review_bot_stopped")

    async def request_review(
        self,
        product_title: str,
        category: str,
        price: float,
        discount_pct: float,
        generated_title: str,
    ) -> TitleReviewResult:
        """
        Envia título para revisão do admin e aguarda resposta.

        Args:
            product_title: Título original do produto.
            category: Categoria do produto.
            price: Preço final.
            discount_pct: Percentual de desconto.
            generated_title: Título gerado pela IA.

        Returns:
            TitleReviewResult com a decisão do admin.
        """
        request_id = str(uuid.uuid4())[:8]
        timeout = settings.title_review.timeout_seconds

        # Monta mensagem de review (texto plano — evita problemas de escape)
        text = (
            f"📝 REVIEW DE TÍTULO\n\n"
            f"Produto: {product_title[:100]}\n"
            f"Categoria: {category}\n"
            f"Preço: R$ {price:.2f} ({discount_pct:.0f}% OFF)\n\n"
            f"Título gerado:\n{generated_title}"
        )

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Aprovar", callback_data=f"title_approve:{request_id}"),
                InlineKeyboardButton("❌ Rejeitar", callback_data=f"title_reject:{request_id}"),
            ],
            [
                InlineKeyboardButton("✏️ Editar", callback_data=f"title_edit:{request_id}"),
            ],
        ])

        # Cria future e registra
        future: asyncio.Future[TitleReviewResult] = asyncio.Future()
        self._pending[request_id] = future
        self._editing[request_id] = generated_title

        # Envia mensagem
        if self._app and self._app.bot:
            try:
                await self._app.bot.send_message(
                    chat_id=self._admin_chat_id,
                    text=text,
                    reply_markup=keyboard,
                )
            except Exception as exc:
                logger.error("review_send_failed", error=str(exc))
                self._pending.pop(request_id, None)
                self._editing.pop(request_id, None)
                return TitleReviewResult(
                    action="timeout",
                    final_title=generated_title,
                    generated_title=generated_title,
                )

        # Aguarda resposta com timeout
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            logger.info("title_review_timeout", request_id=request_id)
            self._pending.pop(request_id, None)
            self._editing.pop(request_id, None)
            return TitleReviewResult(
                action="timeout",
                final_title=generated_title,
                generated_title=generated_title,
            )

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    async def _on_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Trata cliques nos botões inline (Aprovar/Rejeitar/Editar)."""
        query = update.callback_query
        if not query or not query.data:
            return
        await query.answer()

        parts = query.data.split(":")
        if len(parts) != 2:
            return
        action_type, request_id = parts

        future = self._pending.get(request_id)
        if not future or future.done():
            await query.edit_message_text("⏰ Review expirado ou já respondido.")
            return

        generated_title = self._editing.get(request_id, "")

        if action_type == "title_approve":
            future.set_result(
                TitleReviewResult(
                    action="approved",
                    final_title=generated_title,
                    generated_title=generated_title,
                )
            )
            self._pending.pop(request_id, None)
            self._editing.pop(request_id, None)
            await query.edit_message_text(
                f"✅ Título aprovado:\n{generated_title}"
            )
            logger.info("title_approved", title=generated_title)

        elif action_type == "title_reject":
            future.set_result(
                TitleReviewResult(
                    action="rejected",
                    final_title=generated_title,
                    generated_title=generated_title,
                )
            )
            self._pending.pop(request_id, None)
            self._editing.pop(request_id, None)
            await query.edit_message_text("❌ Título rejeitado. Regenerando...")
            logger.info("title_rejected", title=generated_title)

        elif action_type == "title_edit":
            # Marca que estamos esperando texto do admin
            # Mantém o future pendente e o request_id nos _editing
            await query.edit_message_text(
                f"✏️ Manda o título que tu quer:\n\n"
                f"(Original: {generated_title})"
            )
            logger.info("title_edit_requested", request_id=request_id)

    async def _on_text_message(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Captura texto do admin quando em modo edição."""
        if not update.message or not update.message.text:
            return

        # Procura o request_id mais recente aguardando edição
        # (que ainda tem future pendente)
        active_request = None
        for request_id, future in self._pending.items():
            if not future.done() and request_id in self._editing:
                active_request = request_id
                # Não break — pega o mais recente (último inserido no dict)

        if not active_request:
            return  # Nenhum review pendente aguardando edição

        future = self._pending.get(active_request)
        if not future or future.done():
            return

        admin_title = update.message.text.strip().upper()
        generated_title = self._editing.get(active_request, "")

        future.set_result(
            TitleReviewResult(
                action="edited",
                final_title=admin_title,
                generated_title=generated_title,
            )
        )
        self._pending.pop(active_request, None)
        self._editing.pop(active_request, None)

        await update.message.reply_text(
            f"✅ Título editado:\n{admin_title}"
        )
        logger.info("title_edited", original=generated_title, edited=admin_title)
