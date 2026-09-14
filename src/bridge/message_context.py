"""Small, transport-neutral context labels for rendered MAX messages."""

from typing import Optional


def sender_prefix(*, is_own: bool, is_dm: bool, sender_name: Optional[str]) -> str:
    """Подпись отправителя в начале сообщения: значок, имя и двоеточие.

    Единый вид для групп и личных чатов: в ленте топика видно, где свои
    сообщения, а где чужие, без вглядывания в текст. Разметка не
    используется — текст уходит в Telegram без parse_mode.
    """
    if is_own:
        return "🟢 Вы: "
    if is_dm:
        # В личной переписке собеседник один и назван в заголовке топика.
        return ""
    name = " ".join((sender_name or "").split())
    return f"👤 {name}: " if name else ""


def forwarded_context_marker(source_title: Optional[str]) -> str:
    """Render a MAX-forward marker with an optional cache-derived source title."""
    normalized_title = " ".join((source_title or "").split())
    if normalized_title:
        return f"↪️ Переслано из «{normalized_title}»"
    return "↪️ Переслано из MAX"
