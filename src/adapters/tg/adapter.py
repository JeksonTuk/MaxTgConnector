"""
Telegram Adapter — бот + форум-группа с Topics.

Ответственность:
  - Создание топиков (один MAX чат = один топик)
  - Отправка текста, фото, документов в нужный топик
  - Получение reply от пользователя → передача в Bridge Core
  - Команды: /status, /chats, /reauth
  - Уведомления владельцу (ошибки, потеря MAX сессии)
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Callable, Optional, Awaitable

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    MessageReactionUpdated,
    ReactionTypeEmoji,
)

from .notifier import TelegramNotifier
from ...bridge.contracts import (
    CallbackActionHandler,
    TelegramCallbackAction,
    TelegramInlineButton,
    TelegramReactionAction,
)
from ...logging_utils import build_tg_flow_id, log_event, sanitize_path
from ...runtime.health import (
    AlertOutboxStore,
    RuntimeHealthStore,
)
from ...runtime.timeouts import (
    DEFAULT_OPERATION_TIMEOUT_SECONDS,
    MEDIA_TRANSFER_TIMEOUT_SECONDS,
    with_timeout,
)
from .outbound_policy import authorize_outbound_message
from .safe_media import is_allowed_media_size, safe_media_path

logger = logging.getLogger("src.adapters.tg_adapter")


ReplyHandler = Callable[
    [int, Optional[int], str, Optional[int], Optional[str], Optional[str], Optional[str]],
    Awaitable[None],
]
# args: tg_topic_id, tg_msg_id, text, reply_to_tg_msg_id, sender_name, media_path, media_type


class TelegramAdapter:
    def __init__(self, bot_token: str, owner_id: int, forum_group_id: int,
                 tmp_dir: str = "/tmp",
                 ops_topic_id: Optional[int] = None,
                 outbox_store: Optional[AlertOutboxStore] = None,
                 health_store: Optional[RuntimeHealthStore] = None,
                 allowed_user_ids: Optional[frozenset[int]] = None,
                 bot_username: Optional[str] = None,
                 outbound_enabled: bool = False,
                 max_file_size_mb: int = 50):
        self._token = bot_token
        self._owner_id = owner_id
        self._group_id = forum_group_id
        self._tmp_dir = Path(tmp_dir)
        self._ops_topic_id = ops_topic_id
        self._outbox = outbox_store
        self._health = health_store
        self._allowed_user_ids = (
            frozenset(allowed_user_ids) if allowed_user_ids is not None else frozenset({owner_id})
        )
        self._bot_username = bot_username
        self._reaction_handler = None
        self._outbound_enabled = bool(outbound_enabled)
        self._max_file_size_mb = max_file_size_mb
        self._notifier = TelegramNotifier(
            owner_id=owner_id,
            forum_group_id=forum_group_id,
            ops_topic_id=ops_topic_id,
            outbox_store=outbox_store,
            health_store=health_store,
            send_system_message=lambda text, chat_id, message_thread_id, label: self._send_system_message(
                text=text,
                chat_id=chat_id,
                message_thread_id=message_thread_id,
                label=label,
            ),
        )
        self._bot: Optional[Bot] = None
        self._dp: Optional[Dispatcher] = None
        self._reply_handlers: list[ReplyHandler] = []
        self._callback_handlers: list[CallbackActionHandler] = []
        self._command_handlers: dict[str, Callable] = {}
        self._arg_command_handlers: dict[str, Callable] = {}
        self._public_group_arg_commands: set[str] = set()
        self._last_send_error: Optional[str] = None

    def get_last_send_error(self) -> Optional[str]:
        return self._last_send_error

    def on_command(self, cmd: str, handler: Callable):
        """Зарегистрировать внешний обработчик команды без аргументов."""
        self._command_handlers[cmd.lstrip("/")] = handler

    def on_arg_command(self, cmd: str, handler: Callable, *, allow_group_general: bool = False):
        """Зарегистрировать обработчик команды, принимающий аргументы (строку после команды)."""
        normalized = cmd.lstrip("/").lower()
        self._arg_command_handlers[normalized] = handler
        if allow_group_general:
            self._public_group_arg_commands.add(normalized)

    def on_reply(self, handler: ReplyHandler):
        self._reply_handlers.append(handler)

    def on_callback_action(self, handler: CallbackActionHandler):
        self._callback_handlers.append(handler)

    def on_reaction_action(self, handler):
        self._reaction_handler = handler

    # ── Топики ────────────────────────────────────────────────────────────

    async def create_topic(self, title: str, *, flow_id: Optional[str] = None) -> int:
        """Создать топик в форум-группе, вернуть message_thread_id."""
        result = await self._bot.create_forum_topic(
            chat_id=self._group_id,
            name=title[:128],  # Telegram limit
        )
        log_event(
            logger,
            logging.INFO,
            "tg.topic.created",
            flow_id=flow_id,
            stage="routing",
            outcome="created",
            tg_topic_id=result.message_thread_id,
            title=title[:128],
        )
        return result.message_thread_id

    async def rename_topic(self, topic_id: int, new_title: str, *, flow_id: Optional[str] = None):
        """Переименовать существующий топик."""
        try:
            await self._bot.edit_forum_topic(
                chat_id=self._group_id,
                message_thread_id=topic_id,
                name=new_title[:128],
            )
            log_event(
                logger,
                logging.INFO,
                "tg.topic.renamed",
                flow_id=flow_id,
                stage="routing",
                outcome="renamed",
                tg_topic_id=topic_id,
                title=new_title[:128],
            )
        except TelegramAPIError as e:
            log_event(
                logger,
                logging.ERROR,
                "tg.topic.rename_failed",
                flow_id=flow_id,
                stage="routing",
                outcome="failed",
                tg_topic_id=topic_id,
                reason="tg_api_error",
                error=str(e),
            )

    async def delete_topic(self, topic_id: int, *, flow_id: Optional[str] = None) -> bool:
        """Удалить forum topic, если Telegram разрешает."""
        try:
            await self._bot.delete_forum_topic(
                chat_id=self._group_id,
                message_thread_id=topic_id,
            )
            log_event(
                logger,
                logging.INFO,
                "tg.topic.deleted",
                flow_id=flow_id,
                stage="routing",
                outcome="deleted",
                tg_topic_id=topic_id,
            )
            return True
        except TelegramAPIError as e:
            log_event(
                logger,
                logging.WARNING,
                "tg.topic.delete_failed",
                flow_id=flow_id,
                stage="routing",
                outcome="failed",
                reason="tg_api_error",
                tg_topic_id=topic_id,
                error=str(e),
            )
            return False

    async def close_topic(self, topic_id: int, *, flow_id: Optional[str] = None) -> bool:
        """Закрыть forum topic, если delete недоступен."""
        try:
            await self._bot.close_forum_topic(
                chat_id=self._group_id,
                message_thread_id=topic_id,
            )
            log_event(
                logger,
                logging.INFO,
                "tg.topic.closed",
                flow_id=flow_id,
                stage="routing",
                outcome="closed",
                tg_topic_id=topic_id,
            )
            return True
        except TelegramAPIError as e:
            log_event(
                logger,
                logging.WARNING,
                "tg.topic.close_failed",
                flow_id=flow_id,
                stage="routing",
                outcome="failed",
                reason="tg_api_error",
                tg_topic_id=topic_id,
                error=str(e),
            )
            return False

    # ── Retry helper ──────────────────────────────────────────────────────

    async def _tg_retry(self, coro_fn, label: str, *,
                        flow_id: Optional[str] = None,
                        direction: Optional[str] = None,
                        tg_topic_id: Optional[int] = None,
                        tg_msg_id: Optional[int] = None,
                        media_type: Optional[str] = None) -> Optional[int]:
        """Выполнить TG API вызов с retry + exponential backoff.

        3 попытки: немедленно → sleep 1s → sleep 2s.
        TelegramRetryAfter: ждём retry_after секунд вместо стандартной задержки.
        Возвращает message_id при успехе, None после трёх неудач.
        """
        delays = (1, 2)  # пауза перед 2-й и 3-й попытками
        last_exc: Exception = RuntimeError("no attempt made")
        log_event(
            logger,
            logging.INFO,
            "tg.outbound.send",
            flow_id=flow_id,
            direction=direction,
            stage="transport",
            outcome="started",
            tg_topic_id=tg_topic_id,
            tg_msg_id=tg_msg_id,
            media_type=media_type,
            label=label,
        )
        self._last_send_error = None
        timeout_seconds = (
            MEDIA_TRANSFER_TIMEOUT_SECONDS
            if media_type in {"photo", "document", "video", "audio", "voice"}
            else DEFAULT_OPERATION_TIMEOUT_SECONDS
        )

        for attempt in range(1, 4):
            try:
                msg = await with_timeout(
                    coro_fn(),
                    timeout_seconds=timeout_seconds,
                    operation=f"tg.{label}",
                )
                self._last_send_error = None
                log_event(
                    logger,
                    logging.INFO,
                    "tg.outbound.sent",
                    flow_id=flow_id,
                    direction=direction,
                    stage="transport",
                    outcome="sent",
                    tg_topic_id=tg_topic_id,
                    tg_msg_id=getattr(msg, "message_id", None) or tg_msg_id,
                    media_type=media_type,
                    attempts=attempt,
                    label=label,
                )
                # Telegram setMessageReaction возвращает bool, а send_* — объект Message.
                # Для обоих случаев наружу нужен ненулевой признак успеха.
                return getattr(msg, "message_id", None) or tg_msg_id or 1
            except TimeoutError as e:
                self._last_send_error = f"TimeoutError: {timeout_seconds}s"
                log_event(
                    logger,
                    logging.WARNING,
                    "tg.outbound.retry",
                    flow_id=flow_id,
                    direction=direction,
                    stage="transport",
                    outcome="retry",
                    reason="timeout",
                    tg_topic_id=tg_topic_id,
                    tg_msg_id=tg_msg_id,
                    media_type=media_type,
                    attempts=attempt,
                    timeout_seconds=timeout_seconds,
                    label=label,
                )
                last_exc = e
                if attempt < 3:
                    await asyncio.sleep(delays[attempt - 1])
            except TelegramRetryAfter as e:
                wait = max(int(e.retry_after), 1) + 1
                self._last_send_error = (
                    f"{e.__class__.__name__}: retry_after={getattr(e, 'retry_after', None)} {e}"
                )
                log_event(
                    logger,
                    logging.WARNING,
                    "tg.outbound.retry",
                    flow_id=flow_id,
                    direction=direction,
                    stage="transport",
                    outcome="retry",
                    reason="rate_limited",
                    tg_topic_id=tg_topic_id,
                    tg_msg_id=tg_msg_id,
                    media_type=media_type,
                    attempts=attempt,
                    retry_in_seconds=wait,
                    label=label,
                )
                last_exc = e
                if attempt < 3:
                    await asyncio.sleep(wait)
            except TelegramAPIError as e:
                self._last_send_error = f"{e.__class__.__name__}: {e}"
                log_event(
                    logger,
                    logging.WARNING,
                    "tg.outbound.retry",
                    flow_id=flow_id,
                    direction=direction,
                    stage="transport",
                    outcome="retry",
                    reason="tg_api_error",
                    tg_topic_id=tg_topic_id,
                    tg_msg_id=tg_msg_id,
                    media_type=media_type,
                    attempts=attempt,
                    label=label,
                    error=str(e),
                )
                last_exc = e
                if attempt < 3:
                    await asyncio.sleep(delays[attempt - 1])

        log_event(
            logger,
            logging.ERROR,
            "tg.outbound.failed",
            flow_id=flow_id,
            direction=direction,
            stage="transport",
            outcome="failed",
            reason="tg_send_failed",
            tg_topic_id=tg_topic_id,
            tg_msg_id=tg_msg_id,
            media_type=media_type,
            attempts=3,
            label=label,
            error=str(last_exc),
        )
        return None

    # ── Отправка сообщений ────────────────────────────────────────────────
    def _build_inline_markup(
        self,
        buttons: Optional[list[TelegramInlineButton]],
    ) -> InlineKeyboardMarkup | None:
        if not buttons:
            return None
        rows: list[list[InlineKeyboardButton]] = []
        for button in buttons[:8]:
            text = (button.text or "").strip()[:64] or "Открыть"
            if button.url:
                rows.append([InlineKeyboardButton(text=text, url=button.url)])
            elif button.callback_data:
                rows.append([InlineKeyboardButton(text=text, callback_data=button.callback_data)])
        return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

    async def send_text(
        self,
        topic_id: int,
        text: str,
        reply_to_msg_id: Optional[int] = None,
        *,
        flow_id: Optional[str] = None,
        buttons: Optional[list[TelegramInlineButton]] = None,
    ) -> Optional[int]:
        """Отправить текст в топик. Возвращает message_id."""
        kwargs: dict = dict(
            chat_id=self._group_id,
            text=text[:4096],
            message_thread_id=topic_id,
        )
        if reply_to_msg_id:
            kwargs["reply_to_message_id"] = reply_to_msg_id
        markup = self._build_inline_markup(buttons)
        if markup:
            kwargs["reply_markup"] = markup
        return await self._tg_retry(
            lambda: self._bot.send_message(**kwargs),
            f"send_text topic={topic_id}",
            flow_id=flow_id,
            direction="inbound",
            tg_topic_id=topic_id,
            tg_msg_id=reply_to_msg_id,
            media_type="text",
        )

    async def set_message_reaction(self, tg_msg_id: int, reaction: str, *, flow_id: Optional[str] = None) -> bool:
        bot = self._bot
        if bot is None:
            return False
        """Заменить реакцию на исходном outbound-сообщении."""
        try:
            result = await self._tg_retry(
                lambda: bot.set_message_reaction(
                    chat_id=self._group_id,
                    message_id=tg_msg_id,
                    reaction=[ReactionTypeEmoji(type="emoji", emoji=reaction)],
                ),
                f"set_reaction msg={tg_msg_id}",
                direction="outbound",
                tg_topic_id=None,
                tg_msg_id=tg_msg_id,
                media_type="reaction",
            )
            return result is not None
        except Exception:
            return False

    async def send_photo(self, topic_id: int, path: str, caption: str = "",
                         *, reply_to_msg_id: Optional[int] = None,
                         flow_id: Optional[str] = None) -> Optional[int]:
        """Отправить фото в топик."""
        return await self._tg_retry(
            lambda: self._bot.send_photo(
                chat_id=self._group_id,
                photo=FSInputFile(path),
                caption=caption[:1024] if caption else None,
                message_thread_id=topic_id,
                reply_to_message_id=reply_to_msg_id,
            ),
            f"send_photo topic={topic_id}",
            flow_id=flow_id,
            direction="inbound",
            tg_topic_id=topic_id,
            tg_msg_id=reply_to_msg_id,
            media_type="photo",
        )

    async def send_document(self, topic_id: int, path: str,
                             caption: str = "", filename: str = "",
                             *, reply_to_msg_id: Optional[int] = None,
                             flow_id: Optional[str] = None) -> Optional[int]:
        """Отправить документ в топик."""
        return await self._tg_retry(
            lambda: self._bot.send_document(
                chat_id=self._group_id,
                document=FSInputFile(path, filename=filename or Path(path).name),
                caption=caption[:1024] if caption else None,
                message_thread_id=topic_id,
                reply_to_message_id=reply_to_msg_id,
            ),
            f"send_document topic={topic_id}",
            flow_id=flow_id,
            direction="inbound",
            tg_topic_id=topic_id,
            tg_msg_id=reply_to_msg_id,
            media_type="document",
        )

    async def send_owner_document(self, path: str, caption: str = "", filename: str = "") -> bool:
        """Отправить приватный документ владельцу."""
        msg_id = await self._tg_retry(
            lambda: self._bot.send_document(
                chat_id=self._owner_id,
                document=FSInputFile(path, filename=filename or Path(path).name),
                caption=caption[:1024] if caption else None,
            ),
            f"send_owner_document {Path(path).name}",
            direction="system",
            media_type="document",
        )
        return msg_id is not None

    async def send_video(self, topic_id: int, path: str, caption: str = "",
                         filename: str = "", duration: Optional[int] = None,
                         width: Optional[int] = None,
                         height: Optional[int] = None,
                         *, reply_to_msg_id: Optional[int] = None,
                         flow_id: Optional[str] = None) -> Optional[int]:
        """Отправить видео в топик."""
        return await self._tg_retry(
            lambda: self._bot.send_video(
                chat_id=self._group_id,
                video=FSInputFile(path, filename=filename or Path(path).name),
                caption=caption[:1024] if caption else None,
                message_thread_id=topic_id,
                duration=duration,
                width=width,
                height=height,
                supports_streaming=True,
                reply_to_message_id=reply_to_msg_id,
            ),
            f"send_video topic={topic_id}",
            flow_id=flow_id,
            direction="inbound",
            tg_topic_id=topic_id,
            tg_msg_id=reply_to_msg_id,
            media_type="video",
        )

    async def send_audio(self, topic_id: int, path: str, caption: str = "",
                         filename: str = "", duration: Optional[int] = None,
                         *, reply_to_msg_id: Optional[int] = None,
                         flow_id: Optional[str] = None) -> Optional[int]:
        """Отправить аудио в топик."""
        return await self._tg_retry(
            lambda: self._bot.send_audio(
                chat_id=self._group_id,
                audio=FSInputFile(path, filename=filename or Path(path).name),
                caption=caption[:1024] if caption else None,
                message_thread_id=topic_id,
                duration=duration,
                title=Path(filename or path).stem,
                reply_to_message_id=reply_to_msg_id,
            ),
            f"send_audio topic={topic_id}",
            flow_id=flow_id,
            direction="inbound",
            tg_topic_id=topic_id,
            tg_msg_id=reply_to_msg_id,
            media_type="audio",
        )

    async def send_voice(self, topic_id: int, path: str,
                         caption: str = "", duration: Optional[int] = None,
                         *, reply_to_msg_id: Optional[int] = None,
                         flow_id: Optional[str] = None) -> Optional[int]:
        """Отправить voice note в топик (нативный voice bubble)."""
        return await self._tg_retry(
            lambda: self._bot.send_voice(
                chat_id=self._group_id,
                voice=FSInputFile(path),
                caption=caption[:1024] if caption else None,
                message_thread_id=topic_id,
                duration=duration,
                reply_to_message_id=reply_to_msg_id,
            ),
            f"send_voice topic={topic_id}",
            flow_id=flow_id,
            direction="inbound",
            tg_topic_id=topic_id,
            tg_msg_id=reply_to_msg_id,
            media_type="voice",
        )

    async def send_system_notification(self, text: str, *, category: str = "system") -> bool:
        """Отправить системное уведомление во все ops-каналы и сохранить failover в outbox."""
        return await self._notifier.send_system_notification(text, category=category)

    async def send_notification(self, text: str) -> bool:
        """Backwards-compatible alias for ops/system notifications."""
        return await self._notifier.send_notification(text)

    async def send_typing_indicator(self, topic_id: int) -> None:
        """Пробросить индикатор ввода из MAX в Telegram-топик (best-effort)."""
        try:
            await self._bot.send_chat_action(
                chat_id=self._group_id,
                action="typing",
                message_thread_id=topic_id,
            )
        except Exception:
            pass

    async def edit_message_text(self, msg_id: int, text: str) -> bool:
        """Обновить текст уже отправленного сообщения в Telegram (для реакций)."""
        try:
            await self._bot.edit_message_text(
                chat_id=self._group_id,
                message_id=msg_id,
                text=text[:4096],
            )
            return True
        except Exception:
            return False

    async def flush_notification_outbox(self, *, limit: int = 100) -> int:
        return await self._notifier.flush_notification_outbox(limit=limit)

    async def run_notification_outbox(self, *, poll_interval_seconds: int = 30):
        await self._notifier.run_notification_outbox(
            poll_interval_seconds=poll_interval_seconds,
        )

    async def _send_system_message(self, *, text: str, chat_id: int,
                                   message_thread_id: Optional[int], label: str) -> tuple[bool, str]:
        kwargs = {
            "chat_id": chat_id,
            "text": text[:4096],
        }
        if message_thread_id is not None:
            kwargs["message_thread_id"] = message_thread_id

        msg_id = await self._tg_retry(
            lambda: self._bot.send_message(**kwargs),
            f"send_system_notification {label}",
            direction="system",
            tg_topic_id=message_thread_id,
            media_type="system",
        )
        if msg_id is not None:
            return True, ""
        return False, f"send_system_notification failed for {label}"

    # ── Скачивание медиа из Telegram ─────────────────────────────────────

    async def _download_tg_media(self, file_id: str, filename: str, *,
                                 flow_id: Optional[str] = None,
                                 media_type: Optional[str] = None) -> Optional[str]:
        """Скачать медиафайл из Telegram в tmp_dir, вернуть локальный путь."""
        try:
            self._tmp_dir.mkdir(parents=True, exist_ok=True)
            if media_type not in {"photo", "document", "video", "audio", "voice"}:
                return None
            local_path = safe_media_path(self._tmp_dir, media_type, filename)
            await with_timeout(
                self._bot.download(file_id, destination=str(local_path)),
                timeout_seconds=MEDIA_TRANSFER_TIMEOUT_SECONDS,
                operation="tg.download_media",
            )
            size = local_path.stat().st_size if local_path.exists() else None
            if not local_path.exists() or not is_allowed_media_size(local_path, self._max_file_size_mb):
                local_path.unlink(missing_ok=True)
                return None
            log_event(
                logger,
                logging.INFO,
                "tg.inbound.media_download",
                flow_id=flow_id,
                direction="outbound",
                stage="download",
                outcome="downloaded",
                media_type=media_type,
                filename=sanitize_path(filename),
                size_bytes=size,
            )
            return str(local_path)
        except Exception as e:
            log_event(
                logger,
                logging.ERROR,
                "tg.inbound.media_download",
                flow_id=flow_id,
                direction="outbound",
                stage="download",
                outcome="failed",
                reason="download_failed",
                media_type=media_type,
                filename=sanitize_path(filename),
                error=str(e),
            )
            return None

    # ── Получение reply ───────────────────────────────────────────────────

    def _is_owner(self, message: Message) -> bool:
        return bool(message.from_user and message.from_user.id == self._owner_id)

    def _is_group_message(self, message: Message) -> bool:
        return bool(message.chat and message.chat.id == self._group_id)

    def _render_sender_name(self, message: Message) -> Optional[str]:
        user = getattr(message, "from_user", None)
        if not user:
            return None

        full_name = getattr(user, "full_name", None)
        if isinstance(full_name, str) and full_name.strip():
            return full_name.strip()

        parts = [
            getattr(user, "first_name", None),
            getattr(user, "last_name", None),
        ]
        joined = " ".join(part.strip() for part in parts if isinstance(part, str) and part.strip()).strip()
        if joined:
            return joined

        username = getattr(user, "username", None)
        if isinstance(username, str) and username.strip():
            return f"@{username.strip()}"

        user_id = getattr(user, "id", None)
        return str(user_id) if user_id is not None else None

    def _is_owner_dm(self, message: Message) -> bool:
        """Личный чат владельца с ботом."""
        return bool(message.chat and message.chat.id == self._owner_id)

    async def _dispatch_incoming_message(self, message: Message):
        is_group = self._is_group_message(message)
        is_owner_dm = self._is_owner_dm(message)

        # Игнорируем всё, что не из нашей группы и не из личного чата владельца
        if not is_group and not is_owner_dm:
            return

        # Игнорируем сообщения от ботов, включая самого bridge-бота
        if message.from_user and message.from_user.is_bot:
            return

        # Команды
        if message.text and message.text.startswith("/"):
            cmd = message.text.split()[0].lstrip("/").lower()
            # Публичные arg-команды в General топике (без thread_id).
            if cmd in self._public_group_arg_commands and is_group and not message.message_thread_id:
                await self._handle_command(message)
                return
            # Остальные команды — только от владельца
            if not self._is_owner(message):
                return
            await self._handle_command(message)
            return

        # Дальше — только сообщения из форум-группы (reply → MAX)
        if not is_group:
            return

        if not self._outbound_enabled:
            return

        if getattr(message, "media_group_id", None):
            log_event(
                logger,
                logging.INFO,
                "tg.inbound.skipped",
                flow_id=build_tg_flow_id(message.message_thread_id, message.message_id),
                direction="outbound",
                stage="authorization",
                outcome="skipped",
                reason="album_unsupported",
                tg_topic_id=message.message_thread_id,
                tg_msg_id=message.message_id,
            )
            return

        authorized = authorize_outbound_message(
            message,
            forum_group_id=self._group_id,
            allowed_user_ids=self._allowed_user_ids,
            bot_username=self._bot_username,
        )
        if authorized is None:
            return

        # Reply/сообщение в топике → bridge в MAX
        topic_id = message.message_thread_id
        if not topic_id:
            return

        tg_msg_id = getattr(message, "message_id", None)
        flow_id = build_tg_flow_id(topic_id, tg_msg_id)
        reply_to_tg_id = None
        if message.reply_to_message:
            reply_to_tg_id = message.reply_to_message.message_id

        text = authorized.text
        sender_name = None
        log_event(
            logger,
            logging.INFO,
            "tg.inbound.received",
            flow_id=flow_id,
            direction="outbound",
            stage="received",
            outcome="accepted",
            tg_topic_id=topic_id,
            tg_msg_id=tg_msg_id,
            reply_to_tg_msg_id=reply_to_tg_id,
            has_text=bool(text),
        )

        # Скачиваем медиа если есть
        media_path: Optional[str] = None
        media_type: Optional[str] = None
        ts = int(time.time())

        if message.photo:
            media_path = await self._download_tg_media(
                message.photo[-1].file_id, f"tg_photo_{ts}.jpg",
                flow_id=flow_id, media_type="photo",
            )
            media_type = "photo"
        elif message.video:
            ext = Path(message.video.file_name or "video.mp4").suffix or ".mp4"
            media_path = await self._download_tg_media(
                message.video.file_id, f"tg_video_{ts}{ext}",
                flow_id=flow_id, media_type="video",
            )
            media_type = "video"
        elif message.audio:
            ext = Path(message.audio.file_name or "audio.mp3").suffix or ".mp3"
            media_path = await self._download_tg_media(
                message.audio.file_id, f"tg_audio_{ts}{ext}",
                flow_id=flow_id, media_type="audio",
            )
            media_type = "audio"
        elif message.voice:
            media_path = await self._download_tg_media(
                message.voice.file_id, f"tg_voice_{ts}.ogg",
                flow_id=flow_id, media_type="voice",
            )
            media_type = "voice"
        elif message.document:
            fname = message.document.file_name or f"tg_doc_{ts}.bin"
            media_path = await self._download_tg_media(
                message.document.file_id, fname,
                flow_id=flow_id, media_type="document",
            )
            media_type = "document"

        if not text and not media_path:
            log_event(
                logger,
                logging.INFO,
                "tg.inbound.skipped",
                flow_id=flow_id,
                direction="outbound",
                stage="received",
                outcome="skipped",
                reason="empty_event",
                tg_topic_id=topic_id,
                tg_msg_id=tg_msg_id,
            )
            return

        for handler in self._reply_handlers:
            try:
                await handler(topic_id, tg_msg_id, text, reply_to_tg_id, sender_name, media_path, media_type)
            except Exception as e:
                log_event(
                    logger,
                    logging.ERROR,
                    "tg.inbound.handler_failed",
                    flow_id=flow_id,
                    direction="outbound",
                    stage="dispatch",
                    outcome="failed",
                    tg_topic_id=topic_id,
                    tg_msg_id=tg_msg_id,
                    error=str(e),
                )

    def _setup_handlers(self):
        @self._dp.callback_query()
        async def handle_callback(callback: CallbackQuery):
            await self._dispatch_callback_query(callback)

        @self._dp.message()
        async def handle_message(message: Message):
            await self._dispatch_incoming_message(message)

        @self._dp.message_reaction()
        async def handle_message_reaction(update: MessageReactionUpdated):
            await self._dispatch_message_reaction(update)

    async def _dispatch_message_reaction(self, update: MessageReactionUpdated) -> None:
        """Реакция в топике форума → та же реакция на сообщении в MAX.

        Deny-by-default как у исходящих сообщений: только своя форум-группа,
        только разрешённый пользователь. Снятие реакции (пустой new_reaction)
        снимает её и в MAX.
        """
        if not self._outbound_enabled:
            return
        handler = getattr(self, "_reaction_handler", None)
        if handler is None:
            return

        chat = getattr(update, "chat", None)
        if not chat or getattr(chat, "id", None) != self._group_id:
            return
        user = getattr(update, "user", None)
        if not user or getattr(user, "id", None) not in self._allowed_user_ids:
            return
        tg_msg_id = getattr(update, "message_id", None)
        if tg_msg_id is None:
            return

        emoji = None
        for reaction in getattr(update, "new_reaction", None) or []:
            candidate = getattr(reaction, "emoji", None)
            if candidate:
                emoji = str(candidate)
                break

        await handler(TelegramReactionAction(tg_msg_id=int(tg_msg_id), emoji=emoji))

    #: Callback-действия, которые bridge готов принимать. Всё остальное молча
    #: игнорируется: кнопка из чужого/старого сообщения не должна ничего запускать.
    KNOWN_CALLBACK_ACTIONS = ("max_join", "watchdog_check")

    async def _dispatch_callback_query(self, callback: CallbackQuery):
        data = callback.data or ""
        action_name = next(
            (a for a in self.KNOWN_CALLBACK_ACTIONS if data.startswith(f"{a}:")),
            None,
        )
        if action_name is None:
            await callback.answer()
            return
        if (
            not callback.from_user
            or callback.from_user.id != self._owner_id
            or callback.from_user.id not in self._allowed_user_ids
        ):
            await callback.answer("Только владелец bridge", show_alert=False)
            return
        action_id = data.split(":", 1)[1].strip()
        if not action_id:
            await callback.answer("Действие не найдено", show_alert=False)
            return
        message = getattr(callback, "message", None)
        topic_id = getattr(message, "message_thread_id", None)
        tg_msg_id = getattr(message, "message_id", None)
        action = TelegramCallbackAction(
            action=action_name,
            action_id=action_id,
            user_id=callback.from_user.id,
            topic_id=topic_id,
            tg_msg_id=tg_msg_id,
        )
        # Успешный callback тоже логируем: иначе «кнопка не сработала»
        # неотличимо от «событие не доехало» — а это разные поломки.
        log_event(
            logger,
            logging.INFO,
            "tg.callback.received",
            direction="callback",
            stage="dispatch",
            outcome="accepted",
            action=action_name,
        )
        answer = "Действие не обработано"
        try:
            for handler in self._callback_handlers:
                answer = await handler(action)
                break
        except Exception as exc:
            log_event(
                logger,
                logging.ERROR,
                "tg.callback.handler_failed",
                direction="callback",
                stage="dispatch",
                outcome="failed",
                action=action_name,
                error_type=type(exc).__name__,
            )
            answer = "Ошибка при выполнении действия"
        await callback.answer(answer[:200], show_alert=False)

    async def _handle_command(self, message: Message):
        parts = message.text.split()
        cmd = parts[0].lstrip("/").lower()
        args = " ".join(parts[1:])
        try:
            if cmd in self._arg_command_handlers:
                reply_text = await self._arg_command_handlers[cmd](args)
                await message.reply(reply_text)
            elif cmd in self._command_handlers:
                result = await self._command_handlers[cmd]()
                # Обработчик может вернуть просто текст или пару (текст, кнопки):
                # так команда получает кнопку, не меняя контракт остальных команд.
                if isinstance(result, tuple):
                    reply_text, buttons = result
                    await message.reply(
                        reply_text,
                        reply_markup=self._build_inline_markup(buttons),
                    )
                else:
                    await message.reply(result)
            elif cmd == "reauth":
                await message.reply(
                    "⚠️ Для повторной авторизации MAX:\n"
                    "Перезапусти bridge и введи новый SMS код."
                )
        except Exception as e:
            logger.error("Command handler /%s error: %s", cmd, e)
            await message.reply("⚠️ Ошибка при выполнении команды")

    # ── Жизненный цикл ────────────────────────────────────────────────────

    async def start(self):
        """Запустить polling (блокирующий)."""
        self._bot = Bot(token=self._token)
        self._dp = Dispatcher()
        await self._resolve_bot_username()
        self._setup_handlers()
        log_event(
            logger,
            logging.INFO,
            "tg.adapter.starting",
            stage="startup",
            outcome="started",
            group_id=self._group_id,
            owner_id=self._owner_id,
        )
        # message_reaction приходит только при явном запросе: Telegram не шлёт
        # его в составе дефолтного набора апдейтов.
        await self._dp.start_polling(
            self._bot,
            allowed_updates=["message", "callback_query", "message_reaction"],
        )

    async def setup(self) -> Bot:
        """Инициализировать бота без запуска polling (для использования в bridge)."""
        self._bot = Bot(token=self._token)
        self._dp = Dispatcher()
        await self._resolve_bot_username()
        self._setup_handlers()
        return self._bot

    async def _resolve_bot_username(self) -> None:
        if self._bot_username or self._bot is None:
            return
        try:
            me = await self._bot.get_me()
            self._bot_username = getattr(me, "username", None)
        except Exception:
            # Без подтверждённого username policy остаётся deny-by-default.
            self._bot_username = None

    def get_dispatcher(self) -> Dispatcher:
        return self._dp

    def get_bot(self) -> Bot:
        return self._bot

    async def close(self):
        if self._bot is not None:
            await self._bot.session.close()
