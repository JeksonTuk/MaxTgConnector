"""pending_outbound_messages repository."""

import time
from typing import Optional

from .base import BaseRepo
from ..types import PendingOutboundMessage, PendingOutboundReaction


class PendingOutboundRepo(BaseRepo):
    def _pending_outbound_from_row(self, row) -> PendingOutboundMessage:
        return PendingOutboundMessage(**dict(row))

    async def enqueue_pending_outbound(self, job: PendingOutboundMessage) -> int:
        now = int(time.time())
        created_at = job.created_at or now
        updated_at = now
        next_attempt_at = job.next_attempt_at or now
        cursor = await self._db.execute(
            """INSERT INTO pending_outbound_messages
               (tg_topic_id, tg_msg_id, max_chat_id, reply_to_max_id, text,
                status, attempts, next_attempt_at, last_error, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tg_topic_id, tg_msg_id)
               DO UPDATE SET
                 max_chat_id = excluded.max_chat_id,
                 reply_to_max_id = excluded.reply_to_max_id,
                 text = excluded.text,
                 status = excluded.status,
                 attempts = MAX(pending_outbound_messages.attempts, excluded.attempts),
                 next_attempt_at = MIN(pending_outbound_messages.next_attempt_at,
                                       excluded.next_attempt_at),
                 last_error = excluded.last_error,
                 updated_at = excluded.updated_at,
                 lease_until = NULL
               WHERE pending_outbound_messages.status != 'delivered'""",
            (
                job.tg_topic_id,
                job.tg_msg_id,
                job.max_chat_id,
                job.reply_to_max_id,
                job.text,
                job.status,
                job.attempts,
                next_attempt_at,
                job.last_error,
                created_at,
                updated_at,
            ),
        )
        await self._commit()
        if cursor.lastrowid:
            return int(cursor.lastrowid)
        async with self._db.execute(
            """SELECT id FROM pending_outbound_messages
               WHERE tg_topic_id = ? AND tg_msg_id = ?""",
            (job.tg_topic_id, job.tg_msg_id),
        ) as cur:
            row = await cur.fetchone()
            return int(row["id"]) if row else 0

    async def create_or_claim_pending_outbound(
        self,
        job: PendingOutboundMessage,
        *,
        lease_until: int,
        now: Optional[int] = None,
    ) -> tuple[Optional[PendingOutboundMessage], bool]:
        """Создать операцию до MAX-вызова и атомарно получить право на отправку."""
        now = int(time.time()) if now is None else now
        await self._db.execute(
            """INSERT INTO pending_outbound_messages
               (tg_topic_id, tg_msg_id, max_chat_id, reply_to_max_id, text,
                status, attempts, next_attempt_at, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
               ON CONFLICT(tg_topic_id, tg_msg_id) DO NOTHING""",
            (job.tg_topic_id, job.tg_msg_id, job.max_chat_id, job.reply_to_max_id,
             job.text, now, now, now),
        )
        cursor = await self._db.execute(
            """UPDATE pending_outbound_messages
               SET status='leased', lease_until=?, updated_at=?
               WHERE tg_topic_id=? AND tg_msg_id=?
                 AND status IN ('pending', 'retry')
                 AND text IS NOT NULL
                 AND (lease_until IS NULL OR lease_until < ?)""",
            (lease_until, now, job.tg_topic_id, job.tg_msg_id, now),
        )
        await self._commit()
        async with self._db.execute(
            """SELECT * FROM pending_outbound_messages
               WHERE tg_topic_id=? AND tg_msg_id=?""",
            (job.tg_topic_id, job.tg_msg_id),
        ) as cur:
            row = await cur.fetchone()
        return (self._pending_outbound_from_row(row) if row else None, cursor.rowcount > 0)

    async def recover_inflight_outbound_as_unknown(self, *, now: Optional[int] = None) -> int:
        """После рестарта не повторять MAX-вызов, который мог уже быть принят."""
        now = int(time.time()) if now is None else now
        cursor = await self._db.execute(
            """UPDATE pending_outbound_messages
               SET status='unknown', text=NULL, lease_until=NULL, updated_at=?,
                   last_error='restart_during_send'
               WHERE status='leased'""",
            (now,),
        )
        await self._commit()
        return cursor.rowcount

    async def get_due_pending_outbound(
        self,
        *,
        now: Optional[int] = None,
        limit: int = 5,
    ) -> list[PendingOutboundMessage]:
        now = int(time.time()) if now is None else now
        async with self._db.execute(
            """SELECT * FROM pending_outbound_messages
               WHERE status IN ('pending', 'retry')
                 AND text IS NOT NULL
                 AND next_attempt_at <= ?
                 AND (lease_until IS NULL OR lease_until < ?)
               ORDER BY next_attempt_at ASC, id ASC
               LIMIT ?""",
            (now, now, limit),
        ) as cur:
            rows = await cur.fetchall()
        return [self._pending_outbound_from_row(row) for row in rows]

    async def lease_pending_outbound(
        self,
        job_id: int,
        *,
        lease_until: int,
        now: Optional[int] = None,
    ) -> bool:
        now = int(time.time()) if now is None else now
        cursor = await self._db.execute(
            """UPDATE pending_outbound_messages
               SET status = 'leased', lease_until = ?, updated_at = ?
               WHERE id = ?
                 AND status IN ('pending', 'retry', 'leased')
                 AND text IS NOT NULL
                 AND (lease_until IS NULL OR lease_until < ?)""",
            (lease_until, now, job_id, now),
        )
        await self._commit()
        return cursor.rowcount > 0

    async def mark_pending_outbound_retry(
        self,
        job_id: int,
        *,
        error: str,
        next_attempt_at: int,
        now: Optional[int] = None,
    ):
        now = int(time.time()) if now is None else now
        await self._db.execute(
            """UPDATE pending_outbound_messages
               SET status = 'retry',
                   attempts = attempts + 1,
                   updated_at = ?,
                   last_attempt_at = ?,
                   next_attempt_at = ?,
                   lease_until = NULL,
                   last_error = ?
               WHERE id = ?""",
            (now, now, next_attempt_at, error, job_id),
        )
        await self._commit()

    async def mark_pending_outbound_delivered(
        self,
        job_id: int,
        *,
        max_msg_id: str,
        now: Optional[int] = None,
    ):
        now = int(time.time()) if now is None else now
        await self._db.execute(
            """UPDATE pending_outbound_messages
               SET status = 'delivered',
                   attempts = attempts + 1,
                   text = NULL,
                   updated_at = ?,
                   last_attempt_at = ?,
                   lease_until = NULL,
                   delivered_max_msg_id = ?,
                   delivered_at = ?,
                   last_error = NULL
               WHERE id = ?""",
            (now, now, max_msg_id, now, job_id),
        )
        await self._commit()

    async def mark_pending_outbound_failed(
        self,
        job_id: int,
        *,
        error: str,
        now: Optional[int] = None,
    ):
        now = int(time.time()) if now is None else now
        await self._db.execute(
            """UPDATE pending_outbound_messages
               SET status = 'failed',
                   attempts = attempts + 1,
                   text = NULL,
                   updated_at = ?,
                   last_attempt_at = ?,
                   lease_until = NULL,
                   last_error = ?
               WHERE id = ?""",
            (now, now, error, job_id),
        )
        await self._commit()

    async def mark_pending_outbound_unknown(
        self,
        job_id: int,
        *,
        error: str,
        now: Optional[int] = None,
    ):
        now = int(time.time()) if now is None else now
        await self._db.execute(
            """UPDATE pending_outbound_messages
               SET status='unknown', attempts=attempts+1, text=NULL,
                   updated_at=?, last_attempt_at=?, lease_until=NULL, last_error=?
               WHERE id=?""",
            (now, now, error, job_id),
        )
        await self._commit()

    async def expire_pending_outbound(
        self,
        *,
        older_than_seconds: int,
        now: Optional[int] = None,
    ) -> int:
        now = int(time.time()) if now is None else now
        cutoff = now - older_than_seconds
        cursor = await self._db.execute(
            """UPDATE pending_outbound_messages
               SET status = 'expired',
                   text = NULL,
                   updated_at = ?,
                   lease_until = NULL,
                   last_error = 'expired'
               WHERE status IN ('pending', 'retry', 'leased')
                 AND created_at < ?""",
            (now, cutoff),
        )
        await self._commit()
        return cursor.rowcount

    async def count_pending_outbound(self) -> dict[str, Optional[int]]:
        async with self._db.execute(
            """SELECT COUNT(*) AS pending_count, MIN(created_at) AS oldest_created_at
               FROM pending_outbound_messages
               WHERE status IN ('pending', 'retry', 'leased')"""
        ) as cur:
            row = await cur.fetchone()
        return {
            "pending_count": int(row["pending_count"] or 0),
            "oldest_created_at": row["oldest_created_at"],
        }

    async def queue_outbound_reaction(
        self,
        *,
        topic_id: int,
        tg_msg_id: int,
        reaction: str,
        error: Optional[str] = None,
        now: Optional[int] = None,
    ) -> int:
        now = int(time.time()) if now is None else now
        cursor = await self._db.execute(
            """INSERT INTO pending_outbound_reactions
               (tg_topic_id, tg_msg_id, reaction, status, next_attempt_at,
                last_error, created_at, updated_at)
               VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)
               ON CONFLICT(tg_topic_id, tg_msg_id) DO UPDATE SET
                 reaction=excluded.reaction,
                 status='pending',
                 next_attempt_at=excluded.next_attempt_at,
                 last_error=excluded.last_error,
                 updated_at=excluded.updated_at""",
            (topic_id, tg_msg_id, reaction, now, error, now, now),
        )
        await self._commit()
        return int(cursor.lastrowid or 0)

    async def get_due_pending_reactions(self, *, now: Optional[int] = None, limit: int = 20):
        now = int(time.time()) if now is None else now
        async with self._db.execute(
            """SELECT * FROM pending_outbound_reactions
               WHERE status IN ('pending', 'retry') AND next_attempt_at <= ?
               ORDER BY next_attempt_at, id LIMIT ?""",
            (now, limit),
        ) as cur:
            return [PendingOutboundReaction(**dict(row)) for row in await cur.fetchall()]

    async def mark_pending_reaction_retry(
        self, reaction_id: int, *, error: str, next_attempt_at: int, now: Optional[int] = None
    ):
        now = int(time.time()) if now is None else now
        await self._db.execute(
            """UPDATE pending_outbound_reactions
               SET status='retry', attempts=attempts+1, next_attempt_at=?,
                   last_error=?, updated_at=? WHERE id=?""",
            (next_attempt_at, error, now, reaction_id),
        )
        await self._commit()

    async def mark_pending_reaction_delivered(
        self,
        reaction_id: int,
        *,
        expected_reaction: Optional[str] = None,
        expected_updated_at: Optional[int] = None,
        now: Optional[int] = None,
    ) -> bool:
        now = int(time.time()) if now is None else now
        conditions = ["id = ?", "status IN ('pending', 'retry')"]
        params: list[object] = [reaction_id]
        if expected_reaction is not None:
            conditions.append("reaction = ?")
            params.append(expected_reaction)
        if expected_updated_at is not None:
            conditions.append("updated_at = ?")
            params.append(expected_updated_at)
        cursor = await self._db.execute(
            f"""UPDATE pending_outbound_reactions
                SET status='delivered', attempts=attempts+1, last_error=NULL,
                    updated_at=? WHERE {' AND '.join(conditions)}""",
            [now, *params],
        )
        await self._commit()
        return cursor.rowcount > 0

    async def mark_pending_reaction_current_delivered(
        self,
        *,
        topic_id: int,
        tg_msg_id: int,
        reaction: Optional[str] = None,
        now: Optional[int] = None,
    ):
        now = int(time.time()) if now is None else now
        conditions = ["tg_topic_id = ?", "tg_msg_id = ?"]
        params: list[object] = [topic_id, tg_msg_id]
        if reaction is not None:
            conditions.append("reaction = ?")
            params.append(reaction)
        await self._db.execute(
            f"""UPDATE pending_outbound_reactions
                SET status='delivered', last_error=NULL, updated_at=?
                WHERE {' AND '.join(conditions)}""",
            [now, *params],
        )
        await self._commit()
