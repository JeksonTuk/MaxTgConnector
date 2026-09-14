"""Офлайн-проверки ограничений одиночного MAX SMS auth-test."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import yaml
from pymax import PasswordAttemptsExceededError
from pymax.auth import SmsAuthFlow

from scripts.max_sms_auth_test import (
    AuthRequestBudget,
    AuthTestStop,
    OneShotSmsAuthFlow,
    SessionOnlyAuthFlow,
    TimedConsolePasswordProvider,
    TimedConsoleSmsCodeProvider,
    classify_error,
    install_one_shot_request_guard,
    make_auth_test_extra_config,
    stdin_is_interactive,
)


class _NonInteractiveStream:
    def isatty(self):
        return False


class _FakeResponse:
    token = "not-used-in-test-output"
    request_max_duration = 60
    request_count_left = 1
    alt_action_duration = 0


class _FakeAuthService:
    def __init__(self):
        self.calls = 0

    async def request_code(self, _phone):
        self.calls += 1
        return _FakeResponse()


class _FakeApi:
    def __init__(self):
        self.auth = _FakeAuthService()


class _FakeApp:
    def __init__(self):
        self.api = _FakeApi()


class _FakePasswordAuth:
    def __init__(self):
        self.calls = 0

    async def check_password(self, _track_id, _password):
        self.calls += 1
        return type("PasswordResponse", (), {"error": "wrong", "login_token": None})()


class _FakePasswordApp:
    def __init__(self):
        self.config = type("Config", (), {"password_max_attempts": 1})()
        self.api = type("Api", (), {"auth": _FakePasswordAuth()})()


async def _never_finishes(*_args):
    await asyncio.sleep(60)


async def _return_text(*_args):
    return "123456"


async def test_offline_a_missing_stdin_is_detected_before_auth():
    assert stdin_is_interactive(_NonInteractiveStream()) is False


async def test_offline_b_request_budget_allows_one_real_request():
    app = _FakeApp()
    budget = AuthRequestBudget()
    restore = install_one_shot_request_guard(app.api.auth, budget)
    try:
        await app.api.auth.request_code("not-used")
        with pytest.raises(AuthTestStop, match="auth_request_budget_exhausted"):
            await app.api.auth.request_code("not-used")
    finally:
        restore()

    assert app.api.auth.calls == 1
    assert budget.request_count == 1
    assert budget.blocked_count == 1


async def test_offline_c_timeout_does_not_retry_or_resend():
    provider = TimedConsoleSmsCodeProvider(
        timeout=0.001,
        reader=lambda _prompt: "unused",
        to_thread=_never_finishes,
    )
    with pytest.raises(AuthTestStop, match="timeout"):
        await provider.get_code("not-used")
    assert provider.calls == 1


def test_offline_d_limit_violate_is_terminal_reason():
    error = RuntimeError("limit.violate: too many attempts")
    assert classify_error(error) == "limit_violate"


async def test_offline_e_eof_is_terminal():
    def eof_reader(_prompt):
        raise EOFError

    provider = TimedConsoleSmsCodeProvider(timeout=1, reader=eof_reader)
    with pytest.raises(AuthTestStop, match="stdin_eof"):
        await provider.get_code("not-used")
    assert provider.calls == 1


async def test_offline_f_password_is_one_attempt_and_config_is_bounded():
    provider = TimedConsolePasswordProvider(
        timeout=1,
        reader=lambda _prompt: "unused",
        to_thread=_return_text,
    )
    app = _FakePasswordApp()
    flow = OneShotSmsAuthFlow(timeout=1, budget=AuthRequestBudget())
    flow.delegate.password_provider = provider
    with pytest.raises(PasswordAttemptsExceededError):
        await flow.delegate._authenticate_with_password(app, track_id="not-used", hint=None)

    assert provider.calls == 1
    assert app.api.auth.calls == 1

    config = make_auth_test_extra_config()
    assert config.reconnect is False
    assert config.relogin is False
    assert config.password_max_attempts == 1


async def test_offline_g_missing_session_cannot_start_auth():
    app = _FakeApp()
    with pytest.raises(AuthTestStop, match="session_unavailable"):
        await SessionOnlyAuthFlow().authenticate(app)
    assert app.api.auth.calls == 0


def test_offline_h_first_mode_uses_standard_sms_flow():
    flow = OneShotSmsAuthFlow(timeout=1, budget=AuthRequestBudget())
    assert isinstance(flow.delegate, SmsAuthFlow)


def test_offline_i_container_is_one_shot_and_isolated():
    compose_path = Path(__file__).parents[1] / "deploy" / "docker-compose.auth-test.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    service = compose["services"]["max-sms-auth-test"]

    assert service["restart"] == "no"
    assert service["read_only"] is True
    assert "ports" not in service
    assert service["user"] == "10001:10001"
    assert service["volumes"] == ["max_sms_auth_test_session:/app/auth-data"]
    assert service["mem_limit"] == "512m"
    assert service["cpus"] == "0.50"
    assert service["pids_limit"] == 128
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["cap_drop"] == ["ALL"]
