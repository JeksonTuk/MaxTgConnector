"""Telegram reply to MAX routing."""

import logging
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path
from typing import Optional

from . import delivery as bridge_delivery
from . import forwarding as bridge_forwarding
from . import mapping as bridge_mapping
from . import outbound_retry as bridge_outbound_retry
from .contracts import MaxBridgePort, TelegramBridgePort
from ..config.loader import AppConfig
from ..db.repository import Repository
from ..db.types import PendingOutboundMessage
from ..logging_utils import build_tg_flow_id, log_event, sanitize_path
from ..runtime.timeouts import (
    DEFAULT_OPERATION_TIMEOUT_SECONDS,
    MEDIA_TRANSFER_TIMEOUT_SECONDS,
    with_timeout_or_none,
)

logger = logging.getLogger("src.bridge.core")


OpsSender = Callable[[str], Awaitable[None]]


def compose_tg_outbound_text(text: str, sender_name: Optional[str]) -> str:
    # Telegram author identity remains in Telegram only; MAX receives content.
    return text.strip()


def compose_tg_outbound_failure_notice(max_error: Optional[str]) -> str:
    if max_error and "pymax_tcp_sequence_overflow" in max_error:
        return "❌ Не удалось отправить сообщение в MAX (MAX transport: pymax_tcp_sequence_overflow)"
    return "❌ Не удалось отправить сообщение в MAX"


async def handle_tg_reply(
    *,
    cfg: AppConfig,
    repo: Repository,
    max_adapter: MaxBridgePort,
    tg: TelegramBridgePort,
    stats: dict[str, int | float],
    send_ops_notification: OpsSender,
    topic_id: int,
    tg_msg_id: Optional[int],
    text: str,
    reply_to_tg_msg_id: Optional[int],
    sender_name: Optional[str],
    media_path: Optional[str] = None,
    media_type: Optional[str] = None,
):
    """Reply из Telegram → отправляем в MAX."""
    if not getattr(cfg.bridge, "outbound_enabled", True):
        return
    flow_id = build_tg_flow_id(topic_id, tg_msg_id)
    log_event(
        logger,
        logging.INFO,
        "bridge.outbound.forward_started",
        flow_id=flow_id,
        direction="outbound",
        stage="received",
        outcome="accepted",
        tg_topic_id=topic_id,
        tg_msg_id=tg_msg_id,
        reply_to_tg_msg_id=reply_to_tg_msg_id,
        media_type=media_type,
        has_text=bool(text.strip()),
        filename=sanitize_path(media_path),
    )

    binding = await repo.get_binding_by_topic(topic_id)
    if not binding:
        await send_ops_notification(f"⚠️ Не найден MAX чат для топика {topic_id}")
        await bridge_delivery.log_outbound_failure(
            repo,
            topic_id=topic_id,
            tg_msg_id=tg_msg_id,
            max_chat_id=f"tg_topic:{topic_id}",
            error="no_topic",
            attempts=1,
        )
        log_event(
            logger,
            logging.ERROR,
            "bridge.outbound.forward_finished",
            flow_id=flow_id,
            direction="outbound",
            stage="routing",
            outcome="failed",
            reason="no_topic",
            tg_topic_id=topic_id,
            tg_msg_id=tg_msg_id,
        )
        return

    is_allowed_max_chat = getattr(cfg, "is_allowed_max_chat_id", None)
    if callable(is_allowed_max_chat) and not is_allowed_max_chat(binding.max_chat_id):
        log_event(
            logger,
            logging.INFO,
            "bridge.outbound.forward_finished",
            flow_id=flow_id,
            direction="outbound",
            stage="authorization",
            outcome="skipped",
            reason="max_chat_not_allowed",
            tg_topic_id=topic_id,
            tg_msg_id=tg_msg_id,
            max_chat_id=binding.max_chat_id,
        )
        return

    if binding.mode == "readonly":
        log_event(
            logger,
            logging.INFO,
            "bridge.outbound.forward_finished",
            flow_id=flow_id,
            direction="outbound",
            stage="routing",
            outcome="skipped",
            reason="readonly",
            tg_topic_id=topic_id,
            tg_msg_id=tg_msg_id,
            max_chat_id=binding.max_chat_id,
        )
        return

    if binding.mode == "disabled":
        log_event(
            logger,
            logging.INFO,
            "bridge.outbound.forward_finished",
            flow_id=flow_id,
            direction="outbound",
            stage="routing",
            outcome="skipped",
            reason="disabled",
            tg_topic_id=topic_id,
            tg_msg_id=tg_msg_id,
            max_chat_id=binding.max_chat_id,
        )
        return

    outbound_text = compose_tg_outbound_text(text, sender_name)
    if tg_msg_id is None:
        # Без Telegram message_id нельзя доказать идемпотентность операции.
        return

    async def set_reaction(reaction: str):
        setter = getattr(tg, "set_message_reaction", None)
        if not callable(setter):
            return
        try:
            ok = await setter(tg_msg_id, reaction, flow_id=flow_id)
        except Exception:
            ok = False
        if not ok:
            queue = getattr(repo, "queue_outbound_reaction", None)
            if callable(queue):
                await queue(
                    topic_id=topic_id,
                    tg_msg_id=tg_msg_id,
                    reaction=reaction,
                    error="telegram_reaction_failed",
                )
        else:
            clear = getattr(repo, "mark_pending_reaction_current_delivered", None)
            if callable(clear):
                await clear(topic_id=topic_id, tg_msg_id=tg_msg_id, reaction=reaction)

    if media_path and bridge_forwarding.is_file_too_large(cfg, media_path):
        with suppress(Exception):
            Path(media_path).unlink(missing_ok=True)
        stats["failed_outbound"] += 1
        await set_reaction("👎")
        await bridge_delivery.log_outbound_failure(
            repo,
            topic_id=topic_id,
            tg_msg_id=tg_msg_id,
            max_chat_id=binding.max_chat_id,
            error=f"too_large:{Path(media_path).name}",
            attempts=1,
        )
        log_event(
            logger,
            logging.ERROR,
            "bridge.outbound.forward_finished",
            flow_id=flow_id,
            direction="outbound",
            stage="validation",
            outcome="failed",
            reason="too_large",
            tg_topic_id=topic_id,
            tg_msg_id=tg_msg_id,
            max_chat_id=binding.max_chat_id,
            filename=sanitize_path(media_path),
        )
        return

    reply_to_max_id = None
    if reply_to_tg_msg_id:
        get_mapping = getattr(repo, "get_tg_reply_mapping", None)
        if callable(get_mapping):
            mapping = await get_mapping(reply_to_tg_msg_id)
            if mapping and mapping.max_chat_id == binding.max_chat_id:
                reply_to_max_id = mapping.max_msg_id
            elif mapping:
                log_event(
                    logger,
                    logging.INFO,
                    "bridge.outbound.reply_resolved",
                    flow_id=flow_id,
                    direction="outbound",
                    stage="routing",
                    outcome="skipped",
                    reason="stale_remap_chat",
                    tg_topic_id=topic_id,
                    tg_msg_id=tg_msg_id,
                    reply_to_tg_msg_id=reply_to_tg_msg_id,
                    max_chat_id=binding.max_chat_id,
                    mapped_max_chat_id=mapping.max_chat_id,
                )
        else:
            reply_to_max_id = await repo.get_max_msg_id_by_tg(reply_to_tg_msg_id)
    log_event(
        logger,
        logging.INFO,
        "bridge.outbound.reply_resolved",
        flow_id=flow_id,
        direction="outbound",
        stage="routing",
        outcome="found" if reply_to_max_id else "missing",
        tg_topic_id=topic_id,
        tg_msg_id=tg_msg_id,
        reply_to_tg_msg_id=reply_to_tg_msg_id,
        max_chat_id=binding.max_chat_id,
        reply_to_max_id=reply_to_max_id,
    )

    had_media = bool(media_path)
    claim = getattr(repo, "create_or_claim_pending_outbound", None)
    operation = None
    if callable(claim):
        operation, claimed = await claim(
            PendingOutboundMessage(
                tg_topic_id=topic_id,
                tg_msg_id=tg_msg_id,
                max_chat_id=binding.max_chat_id,
                reply_to_max_id=reply_to_max_id,
                text=outbound_text,
                created_at=int(time.time()),
            ),
            lease_until=int(time.time()) + bridge_outbound_retry.PENDING_OUTBOUND_LEASE_SECONDS,
        )
        if not claimed:
            # Другой update/worker уже владеет этой операцией или она завершена.
            return

    await set_reaction("👀")
    send_timeout = MEDIA_TRANSFER_TIMEOUT_SECONDS if media_path else DEFAULT_OPERATION_TIMEOUT_SECONDS
    sent_id = await with_timeout_or_none(
        max_adapter.send_message(
            chat_id=binding.max_chat_id,
            text=outbound_text,
            reply_to_msg_id=reply_to_max_id,
            media_path=media_path,
            media_type=media_type,
            flow_id=flow_id,
        ),
        timeout_seconds=send_timeout,
        logger=logger,
        event="bridge.external_await_timeout",
        operation="max.send_message",
        flow_id=flow_id,
        direction="outbound",
        tg_topic_id=topic_id,
        tg_msg_id=tg_msg_id,
        max_chat_id=binding.max_chat_id,
        media_type=media_type,
    )

    if media_path:
        with suppress(Exception):
            Path(media_path).unlink(missing_ok=True)

    if sent_id is None:
        max_error = max_adapter.get_last_outbound_error()
        if max_error is None:
            max_error = "MAX send timeout"
        attempts = max_adapter.get_last_outbound_attempts()
        delivery_error = max_error or "max_send_failed"
        if attempts > 1:
            delivery_error = f"{delivery_error} (attempts={attempts})"
        retry_candidate = (
            not had_media
            and tg_msg_id is not None
            and outbound_text.strip()
            and bridge_outbound_retry.is_definite_unsent_outbound_error(max_error)
        )
        queued = False
        if operation is not None and retry_candidate:
            await repo.mark_pending_outbound_retry(
                operation.id,
                error=delivery_error,
                next_attempt_at=int(time.time()) + bridge_outbound_retry.pending_outbound_retry_delay(
                    int(operation.attempts or 0) + 1
                ),
            )
            await set_reaction("👎")
        elif operation is not None:
            if "ack timeout" in delivery_error.lower() or "timeout" in delivery_error.lower():
                await repo.mark_pending_outbound_unknown(operation.id, error=delivery_error)
                await set_reaction("🤔")
            else:
                await repo.mark_pending_outbound_failed(operation.id, error=delivery_error)
                await set_reaction("👎")
        elif retry_candidate:
            assert tg_msg_id is not None
            async with bridge_mapping.repo_transaction(repo):
                await bridge_delivery.log_outbound_failure(
                    repo,
                    topic_id=topic_id,
                    tg_msg_id=tg_msg_id,
                    max_chat_id=binding.max_chat_id,
                    error=delivery_error,
                    attempts=attempts or 1,
                )
                await bridge_outbound_retry.enqueue_text_outbound_retry(
                    repo=repo,
                    topic_id=topic_id,
                    tg_msg_id=tg_msg_id,
                    max_chat_id=binding.max_chat_id,
                    reply_to_max_id=reply_to_max_id,
                    text=outbound_text,
                    error=delivery_error,
                    attempts=attempts or 1,
                )
            queued = True
        else:
            await bridge_delivery.log_outbound_failure(
                repo,
                topic_id=topic_id,
                tg_msg_id=tg_msg_id,
                max_chat_id=binding.max_chat_id,
                error=delivery_error,
                attempts=attempts or 1,
            )

        # Служебные статусы живут в reaction, отдельные сообщения в topic не создаём.
        stats["failed_outbound"] += 1
        log_event(
            logger,
            logging.INFO if queued else logging.ERROR,
            "bridge.outbound.forward_finished",
            flow_id=flow_id,
            direction="outbound",
            stage="forward",
            outcome="queued" if queued else "failed",
            reason="queued_for_retry" if queued else "max_send_failed",
            tg_topic_id=topic_id,
            tg_msg_id=tg_msg_id,
            max_chat_id=binding.max_chat_id,
            error=max_error,
            attempts=attempts,
        )
        return

    async with bridge_mapping.repo_transaction(repo):
        await bridge_mapping.save_outbound_mapping(
            repo,
            max_msg_id=sent_id,
            max_chat_id=binding.max_chat_id,
            tg_topic_id=topic_id,
            tg_msg_id=tg_msg_id,
        )
        await repo.log_delivery(sent_id, binding.max_chat_id, "outbound", "delivered")
        if operation is not None:
            await repo.mark_pending_outbound_delivered(operation.id, max_msg_id=sent_id)
    await set_reaction("👍")
    if media_path:
        stats["outbound_media"] += 1
    else:
        stats["outbound_text"] += 1
    log_event(
        logger,
        logging.INFO,
        "bridge.outbound.forward_finished",
        flow_id=flow_id,
        direction="outbound",
        stage="forward",
        outcome="delivered",
        tg_topic_id=topic_id,
        tg_msg_id=tg_msg_id,
        max_chat_id=binding.max_chat_id,
        max_msg_id=sent_id,
        media_type=media_type,
    )
