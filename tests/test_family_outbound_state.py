"""Durable state tests for one Telegram event -> at most one MAX operation."""

import pytest

from src.db.repository import Repository
from src.db.types import PendingOutboundMessage


@pytest.mark.asyncio
async def test_duplicate_telegram_event_claims_one_operation(tmp_path):
    repo = Repository(str(tmp_path / "bridge.db"))
    await repo.connect()
    try:
        job = PendingOutboundMessage(
            tg_topic_id=77,
            tg_msg_id=123,
            max_chat_id="<ALLOWED_MAX_CHAT_ID>",
            text="Привет",
        )
        first, first_claimed = await repo.create_or_claim_pending_outbound(
            job, lease_until=9999999999, now=100
        )
        second, second_claimed = await repo.create_or_claim_pending_outbound(
            job, lease_until=9999999999, now=101
        )
        assert first is not None and first_claimed is True
        assert second is not None and second_claimed is False
        assert first.id == second.id
    finally:
        await repo.close()


@pytest.mark.asyncio
async def test_restart_marks_inflight_send_unknown_instead_of_retry(tmp_path):
    repo = Repository(str(tmp_path / "bridge.db"))
    await repo.connect()
    try:
        job, claimed = await repo.create_or_claim_pending_outbound(
            PendingOutboundMessage(
                tg_topic_id=77,
                tg_msg_id=124,
                max_chat_id="<ALLOWED_MAX_CHAT_ID>",
                text="Привет",
            ),
            lease_until=9999999999,
            now=100,
        )
        assert claimed and job and job.id
        assert await repo.recover_inflight_outbound_as_unknown(now=200) == 1
        async with repo._db.execute(
            "SELECT status, text, last_error FROM pending_outbound_messages WHERE id=?",
            (job.id,),
        ) as cursor:
            row = await cursor.fetchone()
        assert dict(row) == {
            "status": "unknown",
            "text": None,
            "last_error": "restart_during_send",
        }
    finally:
        await repo.close()


@pytest.mark.asyncio
async def test_reaction_retry_has_no_message_text(tmp_path):
    repo = Repository(str(tmp_path / "bridge.db"))
    await repo.connect()
    try:
        await repo.queue_outbound_reaction(
            topic_id=77,
            tg_msg_id=125,
            reaction="👍",
            error="telegram_reaction_failed",
            now=100,
        )
        jobs = await repo.get_due_pending_reactions(now=100)
        assert len(jobs) == 1
        assert jobs[0].reaction == "👍"
        assert not hasattr(jobs[0], "text")
    finally:
        await repo.close()


@pytest.mark.asyncio
async def test_stale_reaction_success_does_not_ack_newer_reaction(tmp_path):
    repo = Repository(str(tmp_path / "bridge.db"))
    await repo.connect()
    try:
        await repo.queue_outbound_reaction(
            topic_id=77,
            tg_msg_id=126,
            reaction="👀",
            now=100,
        )
        stale = (await repo.get_due_pending_reactions(now=100))[0]
        await repo.queue_outbound_reaction(
            topic_id=77,
            tg_msg_id=126,
            reaction="👍",
            now=101,
        )

        marked = await repo.mark_pending_reaction_delivered(
            stale.id,
            expected_reaction=stale.reaction,
            expected_updated_at=stale.updated_at,
            now=102,
        )

        assert marked is False
        current = await repo.get_due_pending_reactions(now=102)
        assert len(current) == 1
        assert current[0].reaction == "👍"
        assert current[0].status == "pending"
    finally:
        await repo.close()
