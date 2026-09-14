"""Проверки, разрешающие Telegram → MAX только по явному opt-in."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OutboundTelegramMessage:
    """Очищенное событие после всех deny-by-default проверок."""

    text: str
    media_kind: str | None


def _utf16_length(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def _python_index_from_utf16(value: str, offset: int) -> int | None:
    total = 0
    for index, char in enumerate(value):
        if total == offset:
            return index
        total += _utf16_length(char)
        if total > offset:
            return None
    return len(value) if total == offset else None


def _entity_value(entity: Any, name: str, default: Any = None) -> Any:
    if isinstance(entity, dict):
        return entity.get(name, default)
    return getattr(entity, name, default)


def _entity_type(entity: Any) -> str:
    value = _entity_value(entity, "type", "")
    return str(getattr(value, "value", value) or "").lower()


def _leading_mention_end(text: str, entities: list[Any], bot_username: str | None) -> int | None:
    username = (bot_username or "").strip().lstrip("@").lower()
    if not username:
        return None

    first_content = len(text) - len(text.lstrip())
    first_offset = _utf16_length(text[:first_content])
    for entity in entities or []:
        if _entity_type(entity) != "mention":
            continue
        offset = _entity_value(entity, "offset")
        length = _entity_value(entity, "length")
        if not isinstance(offset, int) or not isinstance(length, int) or offset != first_offset:
            continue
        start = _python_index_from_utf16(text, offset)
        end = _python_index_from_utf16(text, offset + length)
        if start is None or end is None:
            continue
        mention = text[start:end]
        if mention.startswith("@") and mention[1:].lower() == username:
            return end
    return None


def authorize_outbound_message(
    message: Any,
    *,
    forum_group_id: int,
    allowed_user_ids: frozenset[int],
    bot_username: str | None,
) -> OutboundTelegramMessage | None:
    """Вернуть очищенное сообщение или None без раскрытия причины наружу."""
    if getattr(message, "edit_date", None) is not None:
        return None
    if any(
        getattr(message, field, None) is not None
        for field in ("forward_origin", "forward_date", "forward_from", "forward_from_chat")
    ):
        return None
    chat = getattr(message, "chat", None)
    user = getattr(message, "from_user", None)
    if not chat or getattr(chat, "id", None) != forum_group_id:
        return None
    if not user or getattr(user, "is_bot", False) or getattr(user, "id", None) not in allowed_user_ids:
        return None
    if not getattr(message, "message_thread_id", None):
        return None

    media_kind = None
    if getattr(message, "photo", None):
        media_kind = "photo"
    elif getattr(message, "document", None):
        media_kind = "document"
    elif getattr(message, "video", None):
        media_kind = "video"
    elif getattr(message, "audio", None):
        media_kind = "audio"
    elif getattr(message, "voice", None):
        media_kind = "voice"

    text = getattr(message, "text", None) or getattr(message, "caption", None) or ""
    entities = (
        getattr(message, "entities", None)
        if getattr(message, "text", None) is not None
        else getattr(message, "caption_entities", None)
    )
    mention_end = _leading_mention_end(text, entities or [], bot_username)
    if mention_end is None:
        return None

    clean_text = text[mention_end:].lstrip()
    if not clean_text and media_kind is None:
        return None
    return OutboundTelegramMessage(text=clean_text, media_kind=media_kind)
