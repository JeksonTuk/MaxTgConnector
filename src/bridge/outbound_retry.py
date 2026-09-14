"""Durable Telegram -> MAX text retry helpers."""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Optional

from . import mapping as bridge_mapping
from .contracts import MaxBridgePort, TelegramBridgePort
from .retry_policy import (
    TEXT_RETRY_LEASE_SECONDS,
    TEXT_RETRY_POLL_SECONDS,
    TEXT_RETRY_TTL_SECONDS,
    exponential_backoff_seconds,
    is_expired,
)
from ..db.repository import PendingOutboundMessage, Repository
from ..logging_utils import build_tg_flow_id, log_event
from ..runtime.timeouts import DEFAULT_OPERATION_TIMEOUT_SECONDS, with_timeout_or_none

logger = logging.getLogger("src.bridge.core")
OpsSender = Callable[[str], Awaitable[None]]

PENDING_OUTBOUND_TTL_SECONDS = TEXT_RETRY_TTL_SECONDS
PENDING_OUTBOUND_POLL_SECONDS = TEXT_RETRY_POLL_SECONDS
PENDING_OUTBOUND_LEASE_SECONDS = TEXT_RETRY_LEASE_SECONDS


def is_definite_unsent_outbound_error(error: Optional[str]) -> bool:
    if not error:
        return False
    lowered = error.lower()
    if "ack timeout" in lowered or "outbound ack timeout" in lowered:
        return False
    markers = (
        "not connected to the server",
        "connection is not open",
        "socket is not connected",
        "max adapter is not connected",
        "max client is not initialized",
        "must be online session",
        "недопустимое состояние сессии",
    )
    return any(marker in lowered for marker in markers)


def pending_outbound_retry_delay(attempts_after_failure: int) -> int:
    return exponential_backoff_seconds(
        attempts_after_failure,
        base_seconds=30,
        cap_seconds=3600,
        max_exponent=7,
    )


async def enqueue_text_outbound_retry(
    *,
    repo: Repository,
    topic_id: int,
    tg_msg_id: int,
    max_chat_id: str,
    reply_to_max_id: Optional[str],
    text: str,
    error: str,
    attempts: int,
) -> int:
    now = int(time.time())
    return await repo.enqueue_pending_outbound(
        PendingOutboundMessage(
            tg_topic_id=topic_id,
            tg_msg_id=tg_msg_id,
            max_chat_id=max_chat_id,
            reply_to_max_id=reply_to_max_id,
            text=text,
            status="pending",
            attempts=max(0, attempts),
            next_attempt_at=now,
            last_error=error,
            created_at=now,
        )
    )


def queued_notice() -> str:
    return (
        "⏳ MAX сейчас недоступен. Текст поставлен в очередь и будет отправлен "
        "автоматически после восстановления подключения."
    )


def media_not_queued_notice() -> str:
    return (
        "❌ Не удалось отправить файл в MAX. Файл не сохранён для автоповтора; "
        "переотправьте его вручную после восстановления MAX."
    )


async def process_pending_outbound_message(
    *,
    repo: Repository,
    max_adapter: MaxBridgePort,
    tg: TelegramBridgePort,
    stats: dict[str, int | float],
    job: PendingOutboundMessage,
    send_ops_notification: OpsSender | None = None,
    ttl_seconds: int = PENDING_OUTBOUND_TTL_SECONDS,
):
    if not job.id:
        return
    flow_id = build_tg_flow_id(job.tg_topic_id, job.tg_msg_id)
    now = int(time.time())
    if is_expired(job.created_at, ttl_seconds, now=now):
        await repo.mark_pending_outbound_failed(job.id, error="expired", now=now)
        await _set_reaction(repo=repo, tg=tg, job=job, reaction="👎", flow_id=flow_id)
        if send_ops_notification is not None:
            await send_ops_notification(
                "⚠️ Отложенное TG→MAX сообщение не доставлено за 48 часов; "
                f"текст удалён из очереди. topic={job.tg_topic_id} "
                f"tg_msg_id={job.tg_msg_id} max_chat_id={job.max_chat_id}"
            )
        log_event(
            logger,
            logging.ERROR,
            "bridge.outbound_retry.failed",
            flow_id=flow_id,
            direction="outbound",
            stage="outbound_retry",
            outcome="failed",
            reason="expired",
            tg_topic_id=job.tg_topic_id,
            tg_msg_id=job.tg_msg_id,
            max_chat_id=job.max_chat_id,
            pending_outbound_id=job.id,
        )
        return

    if not job.text:
        await repo.mark_pending_outbound_failed(job.id, error="missing_text", now=now)
        return

    log_event(
        logger,
        logging.INFO,
        "bridge.outbound_retry.attempt_started",
        flow_id=flow_id,
        direction="outbound",
        stage="outbound_retry",
        outcome="started",
        tg_topic_id=job.tg_topic_id,
        tg_msg_id=job.tg_msg_id,
        max_chat_id=job.max_chat_id,
        pending_outbound_id=job.id,
        attempts=int(job.attempts or 0) + 1,
    )
    sent_id = await with_timeout_or_none(
        max_adapter.send_message(
            chat_id=job.max_chat_id,
            text=job.text,
            reply_to_msg_id=job.reply_to_max_id,
            flow_id=flow_id,
        ),
        timeout_seconds=DEFAULT_OPERATION_TIMEOUT_SECONDS,
        logger=logger,
        event="bridge.external_await_timeout",
        operation="max.send_message",
        flow_id=flow_id,
        direction="outbound",
        tg_topic_id=job.tg_topic_id,
        tg_msg_id=job.tg_msg_id,
        max_chat_id=job.max_chat_id,
        pending_outbound_id=job.id,
    )
    if sent_id:
        async with bridge_mapping.repo_transaction(repo):
            await bridge_mapping.save_outbound_mapping(
                repo,
                max_msg_id=sent_id,
                max_chat_id=job.max_chat_id,
                tg_topic_id=job.tg_topic_id,
                tg_msg_id=job.tg_msg_id,
            )
            await repo.log_delivery(
                sent_id,
                job.max_chat_id,
                "outbound",
                "delivered",
                attempts=int(job.attempts or 0) + 1,
            )
            await repo.mark_pending_outbound_delivered(job.id, max_msg_id=sent_id)
        stats["outbound_text"] += 1
        await _set_reaction(repo=repo, tg=tg, job=job, reaction="👍", flow_id=flow_id)
        log_event(
            logger,
            logging.INFO,
            "bridge.outbound_retry.delivered",
            flow_id=flow_id,
            direction="outbound",
            stage="outbound_retry",
            outcome="delivered",
            tg_topic_id=job.tg_topic_id,
            tg_msg_id=job.tg_msg_id,
            max_chat_id=job.max_chat_id,
            max_msg_id=sent_id,
            pending_outbound_id=job.id,
        )
        return

    error = max_adapter.get_last_outbound_error() or "max_send_failed"
    attempts_after_failure = int(job.attempts or 0) + 1
    if is_definite_unsent_outbound_error(error):
        delay = pending_outbound_retry_delay(attempts_after_failure)
        await repo.mark_pending_outbound_retry(
            job.id,
            error=error,
            next_attempt_at=int(time.time()) + delay,
        )
        log_event(
            logger,
            logging.WARNING,
            "bridge.outbound_retry.retry_scheduled",
            flow_id=flow_id,
            direction="outbound",
            stage="outbound_retry",
            outcome="retry",
            reason="transport_error",
            tg_topic_id=job.tg_topic_id,
            tg_msg_id=job.tg_msg_id,
            max_chat_id=job.max_chat_id,
            pending_outbound_id=job.id,
            attempts=attempts_after_failure,
            retry_in_seconds=delay,
            error=error,
        )
        return

    if "timeout" in error.lower() or "ack" in error.lower():
        await repo.mark_pending_outbound_unknown(job.id, error=error)
        await _set_reaction(repo=repo, tg=tg, job=job, reaction="🤔", flow_id=flow_id)
        return

    await repo.mark_pending_outbound_failed(job.id, error=error)
    await _set_reaction(repo=repo, tg=tg, job=job, reaction="👎", flow_id=flow_id)
    log_event(
        logger,
        logging.ERROR,
        "bridge.outbound_retry.failed",
        flow_id=flow_id,
        direction="outbound",
        stage="outbound_retry",
        outcome="failed",
        reason="ambiguous_send_failure",
        tg_topic_id=job.tg_topic_id,
        tg_msg_id=job.tg_msg_id,
        max_chat_id=job.max_chat_id,
        pending_outbound_id=job.id,
        error=error,
    )


async def run_pending_outbound_messages(
    *,
    repo: Repository,
    max_adapter: MaxBridgePort,
    tg: TelegramBridgePort,
    stats: dict[str, int | float],
    send_ops_notification: OpsSender | None = None,
    poll_interval: int = PENDING_OUTBOUND_POLL_SECONDS,
    lease_seconds: int = PENDING_OUTBOUND_LEASE_SECONDS,
    ttl_seconds: int = PENDING_OUTBOUND_TTL_SECONDS,
    limit: int = 5,
    outbound_enabled: bool = True,
    is_max_chat_allowed: Callable[[str], bool] | None = None,
):
    recovered_inflight = False
    while True:
        if not outbound_enabled:
            await asyncio.sleep(poll_interval)
            continue
        if not recovered_inflight:
            await repo.recover_inflight_outbound_as_unknown()
            recovered_inflight = True
        if not max_adapter.is_ready():
            await asyncio.sleep(poll_interval)
            continue
        now = int(time.time())
        jobs = await repo.get_due_pending_outbound(now=now, limit=limit)
        for job in jobs:
            if not job.id:
                continue
            if is_max_chat_allowed is not None and not is_max_chat_allowed(job.max_chat_id):
                await repo.mark_pending_outbound_failed(
                    job.id,
                    error="max_chat_not_allowed",
                    now=now,
                )
                await _set_reaction(
                    repo=repo,
                    tg=tg,
                    job=job,
                    reaction="👎",
                    flow_id=build_tg_flow_id(job.tg_topic_id, job.tg_msg_id),
                )
                continue
            leased = await repo.lease_pending_outbound(
                job.id,
                lease_until=int(time.time()) + lease_seconds,
            )
            if not leased:
                continue
            await process_pending_outbound_message(
                repo=repo,
                max_adapter=max_adapter,
                tg=tg,
                stats=stats,
                job=job,
                send_ops_notification=send_ops_notification,
                ttl_seconds=ttl_seconds,
            )
        await asyncio.sleep(poll_interval)


async def _set_reaction(*, repo: Repository, tg: TelegramBridgePort, job, reaction: str, flow_id: str | None):
    setter = getattr(tg, "set_message_reaction", None)
    if not callable(setter):
        return
    try:
        ok = await setter(job.tg_msg_id, reaction, flow_id=flow_id)
    except Exception:
        ok = False
    if not ok:
        await repo.queue_outbound_reaction(
            topic_id=job.tg_topic_id,
            tg_msg_id=job.tg_msg_id,
            reaction=reaction,
            error="telegram_reaction_failed",
        )
    else:
        clear = getattr(repo, "mark_pending_reaction_current_delivered", None)
        if callable(clear):
            await clear(
                topic_id=job.tg_topic_id,
                tg_msg_id=job.tg_msg_id,
                reaction=reaction,
            )


async def process_pending_reaction(*, repo: Repository, tg: TelegramBridgePort, job) -> None:
    """Повторить только Telegram reaction update; MAX send здесь недоступен."""
    setter = getattr(tg, "set_message_reaction", None)
    if not callable(setter):
        return
    try:
        ok = await setter(
            job.tg_msg_id,
            job.reaction,
            flow_id=build_tg_flow_id(job.tg_topic_id, job.tg_msg_id),
        )
    except Exception:
        ok = False
    if ok:
        await repo.mark_pending_reaction_delivered(
            job.id,
            expected_reaction=job.reaction,
            expected_updated_at=job.updated_at,
        )
        return
    attempts = int(job.attempts or 0) + 1
    await repo.mark_pending_reaction_retry(
        job.id,
        error="telegram_reaction_failed",
        next_attempt_at=int(time.time()) + min(3600, 30 * (2 ** min(attempts, 6))),
    )


async def run_pending_reactions(
    *,
    repo: Repository,
    tg: TelegramBridgePort,
    poll_interval: int = 30,
    limit: int = 20,
):
    while True:
        for job in await repo.get_due_pending_reactions(limit=limit):
            await process_pending_reaction(repo=repo, tg=tg, job=job)
        await asyncio.sleep(poll_interval)
