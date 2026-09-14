"""Одиночная безопасная проверка SMS-авторизации MAX.

Скрипт не запускает bridge, Telegram, reconnect, watchdog или supervisor.
Режим ``first`` допускает только один запрос SMS-кода. Режим ``reuse``
запрещает любую новую авторизацию и проверяет только сохранённую сессию.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import math
import os
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pymax.auth import AuthFlow, SmsAuthFlow

from src.adapters.max.backends.pymax.client_factory import (
    create_pymax_client,
    make_extra_config,
)


class AuthTestStop(RuntimeError):
    """Безопасная локальная причина остановки теста без текста API-ошибки."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass
class AuthRequestBudget:
    """Счётчик реально разрешённых AUTH_REQUEST в одном процессе."""

    request_count: int = 0
    blocked_count: int = 0


def emit(event: str, **fields: object) -> None:
    """Печатает только заранее подготовленные безопасные поля."""
    parts = [f"AUTH_TEST event={event}"]
    for key, value in fields.items():
        if value is not None:
            parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def stdin_is_interactive(stream=None) -> bool:
    """Возвращает False, если интерактивный ввод недоступен."""
    stream = sys.stdin if stream is None else stream
    try:
        return bool(stream.isatty())
    except (AttributeError, OSError):
        return False


def _safe_number(value: object) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    return None


def _start_response_fields(response: object) -> dict[str, object]:
    fields: dict[str, object] = {
        "token_present": bool(getattr(response, "token", None)),
    }
    for name in (
        "request_max_duration",
        "request_count_left",
        "alt_action_duration",
    ):
        number = _safe_number(getattr(response, name, None))
        if number is not None:
            fields[name] = number
    return fields


def install_one_shot_request_guard(
    auth_service: Any,
    budget: AuthRequestBudget,
) -> Callable[[], None]:
    """Разрешает ровно один вызов штатного AuthService.request_code()."""
    original = auth_service.request_code

    async def guarded_request_code(phone: str):
        if budget.request_count >= 1:
            budget.blocked_count += 1
            raise AuthTestStop("auth_request_budget_exhausted")

        budget.request_count += 1
        response = await original(phone)
        emit("start_auth_response", **_start_response_fields(response))
        return response

    auth_service.request_code = guarded_request_code

    def restore() -> None:
        auth_service.request_code = original

    return restore


class _TimedConsoleProvider:
    def __init__(
        self,
        *,
        timeout: float,
        reader: Callable[..., str] | None,
        to_thread: Callable[..., Awaitable[str]] = asyncio.to_thread,
        hidden: bool = False,
    ) -> None:
        self.timeout = timeout
        self.reader = reader
        self.to_thread = to_thread
        self.hidden = hidden
        self.calls = 0

    async def _read(self, prompt: str) -> str:
        self.calls += 1
        try:
            if self.reader is None:
                value = await asyncio.wait_for(
                    _read_terminal_line(prompt, hidden=self.hidden),
                    timeout=self.timeout,
                )
            else:
                value = await asyncio.wait_for(
                    self.to_thread(self.reader, prompt),
                    timeout=self.timeout,
                )
        except asyncio.TimeoutError as exc:
            raise AuthTestStop("timeout") from exc
        except EOFError as exc:
            raise AuthTestStop("stdin_eof") from exc
        if not isinstance(value, str) or not value.strip():
            raise AuthTestStop("empty_input")
        return value.strip()


async def _read_terminal_line(prompt: str, *, hidden: bool) -> str:
    """Читает одну строку через TTY без зависающего потока после тайм-аута."""
    try:
        fd = sys.stdin.fileno()
        loop = asyncio.get_running_loop()
    except (AttributeError, OSError, RuntimeError, io.UnsupportedOperation) as exc:
        raise AuthTestStop("interactive_reader_unavailable") from exc

    if not os.isatty(fd) or not hasattr(loop, "add_reader"):
        raise AuthTestStop("interactive_reader_unavailable")

    terminal_state = None
    if hidden:
        try:
            import termios
        except ImportError as exc:
            raise AuthTestStop("hidden_input_unavailable") from exc
        try:
            terminal_state = termios.tcgetattr(fd)
            hidden_state = termios.tcgetattr(fd)
            hidden_state[3] &= ~termios.ECHO
            termios.tcsetattr(fd, termios.TCSANOW, hidden_state)
        except (OSError, termios.error) as exc:
            raise AuthTestStop("hidden_input_unavailable") from exc

    print(prompt, end="", flush=True)
    line_future = loop.create_future()

    def on_readable() -> None:
        if line_future.done():
            return
        try:
            line = sys.stdin.readline()
        except EOFError:
            line_future.set_exception(AuthTestStop("stdin_eof"))
            return
        if line == "":
            line_future.set_exception(AuthTestStop("stdin_eof"))
            return
        line_future.set_result(line)

    try:
        loop.add_reader(fd, on_readable)
        return await line_future
    finally:
        with contextlib.suppress(Exception):
            loop.remove_reader(fd)
        if terminal_state is not None:
            import termios

            with contextlib.suppress(OSError, termios.error):
                termios.tcsetattr(fd, termios.TCSANOW, terminal_state)
        if hidden:
            print(flush=True)


class TimedConsoleSmsCodeProvider(_TimedConsoleProvider):
    """Консольный ввод без номера телефона и с одним ограниченным ожиданием."""

    def __init__(
        self,
        *,
        timeout: float,
        reader: Callable[..., str] | None = None,
        to_thread: Callable[..., Awaitable[str]] = asyncio.to_thread,
    ) -> None:
        super().__init__(timeout=timeout, reader=reader, to_thread=to_thread)

    async def get_code(self, _phone: str) -> str:
        return await self._read("Введите SMS-код в этом приватном терминале: ")


class TimedConsolePasswordProvider(_TimedConsoleProvider):
    """Ввод пароля 2FA без отображения значения и без подсказки в логе."""

    def __init__(
        self,
        *,
        timeout: float,
        reader: Callable[..., str] | None = None,
        to_thread: Callable[..., Awaitable[str]] = asyncio.to_thread,
    ) -> None:
        super().__init__(
            timeout=timeout,
            reader=reader,
            to_thread=to_thread,
            hidden=True,
        )

    async def get_password(self, _hint: str | None = None) -> str:
        return await self._read("Введите пароль 2FA в этом приватном терминале: ")


class OneShotSmsAuthFlow(AuthFlow):
    """Тот же PyMax SmsAuthFlow с внешним лимитом AUTH_REQUEST=1."""

    def __init__(self, *, timeout: float, budget: AuthRequestBudget) -> None:
        self.budget = budget
        self.delegate = SmsAuthFlow(
            TimedConsoleSmsCodeProvider(timeout=timeout),
            TimedConsolePasswordProvider(timeout=timeout),
        )

    async def authenticate(self, app):
        restore = install_one_shot_request_guard(app.api.auth, self.budget)
        try:
            return await self.delegate.authenticate(app)
        finally:
            restore()


class SessionOnlyAuthFlow(AuthFlow):
    """AuthFlow второго запуска: любая попытка новой авторизации запрещена."""

    async def authenticate(self, _app):
        raise AuthTestStop("session_unavailable")


def make_auth_test_extra_config():
    """Сохраняет текущий user-agent и отключает только повторные попытки."""
    return make_extra_config().model_copy(
        update={
            "log_level": "CRITICAL",
            "password_max_attempts": 1,
        },
    )


def build_auth_test_egress():
    """Строит только явно выбранный direct/http_connect MAX-egress."""
    from src.adapters.max.network import build_max_egress_profile
    from src.config.loader import MaxEgressConfig, MaxEgressProfileConfig

    egress_type = os.environ.get("AUTH_TEST_EGRESS_TYPE", "").strip()
    if egress_type not in {"direct", "http_connect"}:
        raise AuthTestStop("egress_type_missing_or_invalid")

    proxy_url = os.environ.get("MAX_EGRESS_PROXY_URL")
    if egress_type == "http_connect" and not proxy_url:
        raise AuthTestStop("proxy_url_missing")

    profile = MaxEgressProfileConfig(
        type=egress_type,
        proxy_url=proxy_url if egress_type == "http_connect" else None,
    )
    config = MaxEgressConfig(
        active="auth_test",
        profiles={"auth_test": profile},
        fallback_policy="manual",
    )
    return build_max_egress_profile(config)


def classify_error(error: BaseException) -> str:
    """Классифицирует ошибку по whitelist-причинам, не выводя её текст."""
    if isinstance(error, AuthTestStop):
        return error.reason
    if isinstance(error, asyncio.TimeoutError):
        return "timeout"
    if isinstance(error, EOFError):
        return "stdin_eof"
    if error.__class__.__name__ == "PasswordAttemptsExceededError":
        return "password_attempts_exceeded"

    text = str(error).lower()
    if "limit.violate" in text or ("too many" in text and "attempt" in text):
        return "limit_violate"
    if "login token" in text and "no" in text:
        return "authentication_failed"
    if "session" in text and ("invalid" in text or "reject" in text):
        return "session_rejected"
    return "api_error"


def _session_name() -> str:
    name = os.environ.get("AUTH_TEST_SESSION_NAME", "session.db").strip()
    if not name or Path(name).name != name:
        raise AuthTestStop("session_name_invalid")
    return name


async def run_mode(mode: str, *, timeout: float) -> int:
    budget = AuthRequestBudget()
    if mode == "first" and not stdin_is_interactive():
        emit(
            "finished",
            mode=mode,
            outcome="aborted",
            reason="stdin_unavailable",
            auth_request_count=0,
        )
        return 2

    phone = os.environ.get("MAX_PHONE", "").strip()
    if not phone:
        emit("finished", mode=mode, outcome="aborted", reason="phone_missing", auth_request_count=0)
        return 2

    data_dir = Path(os.environ.get("AUTH_TEST_DATA_DIR", "/app/auth-data"))
    try:
        session_name = _session_name()
        data_dir.mkdir(parents=True, exist_ok=True)
        egress = build_auth_test_egress()
    except AuthTestStop as exc:
        emit("finished", mode=mode, outcome="aborted", reason=exc.reason, auth_request_count=0)
        return 2

    auth_flow: AuthFlow
    if mode == "first":
        auth_flow = OneShotSmsAuthFlow(timeout=timeout, budget=budget)
    else:
        auth_flow = SessionOnlyAuthFlow()

    client = create_pymax_client(
        phone=phone,
        data_dir=str(data_dir),
        session_name=session_name,
        egress=egress,
        extra_config=make_auth_test_extra_config(),
        auth_flow=auth_flow,
        import_legacy_session=False,
    )
    outcome = "failed"
    exit_code = 3
    profile_available = False
    try:
        emit("connect_started", mode=mode)
        await asyncio.wait_for(client.connect(), timeout=timeout)
        profile_available = client.me is not None
        if not profile_available:
            emit("profile_check", success=False)
            outcome = "failed"
            exit_code = 4
        else:
            emit("profile_check", success=True)
            outcome = "success"
            exit_code = 0
    except (asyncio.CancelledError, KeyboardInterrupt):
        raise
    except BaseException as exc:
        reason = classify_error(exc)
        emit(
            "failure",
            error_class=exc.__class__.__name__,
            reason=reason,
        )
        outcome = "failed"
        exit_code = 3
    finally:
        with contextlib.suppress(BaseException):
            await client.close()

    if outcome == "success" and mode == "first":
        session_saved = (data_dir / session_name).is_file()
        emit("session_save", success=session_saved)
        if not session_saved:
            outcome = "failed"
            exit_code = 4
    elif outcome == "success":
        emit("session_reuse", success=True)

    emit(
        "finished",
        mode=mode,
        outcome=outcome,
        profile_available=profile_available,
        auth_request_count=budget.request_count,
        blocked_auth_requests=budget.blocked_count,
    )
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("first", "reuse"), required=True)
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.environ.get("AUTH_TEST_TIMEOUT_SECONDS", "180")),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.timeout <= 0:
        emit(
            "finished",
            mode=args.mode,
            outcome="aborted",
            reason="timeout_invalid",
            auth_request_count=0,
        )
        return 2
    return asyncio.run(run_mode(args.mode, timeout=args.timeout))


if __name__ == "__main__":
    raise SystemExit(main())
