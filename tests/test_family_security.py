"""Минимальные regression/security-проверки семейного deny-by-default режима."""

from pathlib import Path
from types import SimpleNamespace

from src.adapters.tg.outbound_policy import authorize_outbound_message
from src.adapters.tg.safe_media import safe_media_path
from src.bridge.outbound_retry import is_definite_unsent_outbound_error


def _message(*, user_id=101, chat_id=-100, text="@FamilyMaxBot Привет", entities=None, **extra):
    values = dict(
        chat=SimpleNamespace(id=chat_id),
        from_user=SimpleNamespace(id=user_id, is_bot=False),
        message_thread_id=77,
        text=text,
        caption=None,
        entities=entities if entities is not None else [SimpleNamespace(type="mention", offset=0, length=13)],
        caption_entities=None,
        photo=None,
        document=None,
        video=None,
        audio=None,
        voice=None,
        edit_date=None,
        forward_origin=None,
        forward_date=None,
        forward_from=None,
        forward_from_chat=None,
    )
    values.update(extra)
    return SimpleNamespace(**values)


def test_allowed_user_and_real_leading_entity_mention_clean_text():
    result = authorize_outbound_message(
        _message(),
        forum_group_id=-100,
        allowed_user_ids=frozenset({101, 202}),
        bot_username="FamilyMaxBot",
    )
    assert result is not None
    assert result.text == "Привет"


def test_regular_message_does_not_trigger_outbound():
    assert authorize_outbound_message(
        _message(text="Привет", entities=[]),
        forum_group_id=-100,
        allowed_user_ids=frozenset({101}),
        bot_username="FamilyMaxBot",
    ) is None


def test_unauthorized_user_and_wrong_group_are_denied():
    for message in (_message(user_id=999), _message(chat_id=-200)):
        assert authorize_outbound_message(
            message,
            forum_group_id=-100,
            allowed_user_ids=frozenset({101}),
            bot_username="FamilyMaxBot",
        ) is None


def test_mention_inside_forwarded_message_is_denied():
    assert authorize_outbound_message(
        _message(forward_origin=SimpleNamespace(type="user")),
        forum_group_id=-100,
        allowed_user_ids=frozenset({101}),
        bot_username="FamilyMaxBot",
    ) is None


def test_photo_without_caption_mention_is_not_download_candidate():
    message = _message(text=None, entities=[], caption="Фото без команды", caption_entities=[])
    message.photo = [SimpleNamespace(file_id="photo")]
    assert authorize_outbound_message(
        message,
        forum_group_id=-100,
        allowed_user_ids=frozenset({101}),
        bot_username="FamilyMaxBot",
    ) is None


def test_media_paths_are_unique_and_contained(tmp_path):
    first = safe_media_path(tmp_path, "document", "../../evil.pdf")
    second = safe_media_path(tmp_path, "document", "../../evil.pdf")
    assert first != second
    assert first.parent == Path(tmp_path).resolve()
    assert second.parent == Path(tmp_path).resolve()
    assert first.suffix == ".pdf"


def test_ack_timeout_is_not_a_definite_unsent_retry():
    assert not is_definite_unsent_outbound_error("MAX outbound ack timeout")
    assert is_definite_unsent_outbound_error("MAX client is not initialized")
