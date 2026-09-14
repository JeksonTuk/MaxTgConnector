"""Безопасные временные файлы для Telegram → MAX."""

from __future__ import annotations

import uuid
from pathlib import Path


ALLOWED_EXTENSIONS: dict[str, frozenset[str]] = {
    "photo": frozenset({".jpg", ".jpeg", ".png", ".webp"}),
    "document": frozenset({".pdf", ".txt", ".csv", ".zip", ".doc", ".docx", ".xls", ".xlsx"}),
    "video": frozenset({".mp4", ".mov", ".mkv", ".webm"}),
    "audio": frozenset({".mp3", ".m4a", ".wav", ".ogg", ".flac"}),
    "voice": frozenset({".ogg", ".oga", ".opus"}),
}


def safe_media_path(tmp_dir: Path, media_kind: str, original_name: str | None = None) -> Path:
    """Создать collision-free path внутри tmp_dir без пользовательского имени."""
    root = tmp_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    suffix = Path(original_name or "").suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS.get(media_kind, frozenset()):
        suffix = {
            "photo": ".jpg",
            "document": ".bin",
            "video": ".mp4",
            "audio": ".mp3",
            "voice": ".ogg",
        }.get(media_kind, ".bin")
    candidate = (root / f"{uuid.uuid4().hex}{suffix}").resolve()
    if candidate.parent != root:
        raise ValueError("temporary media path escaped dedicated tmp directory")
    return candidate


def is_allowed_media_size(path: Path, max_size_mb: int) -> bool:
    if max_size_mb <= 0:
        return False
    return path.stat().st_size <= max_size_mb * 1024 * 1024
