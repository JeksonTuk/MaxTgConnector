> **Статус:** в работе
> **Дата:** 2026-09-12
> **Назначение:** План реализации
> **Связанное решение:** нет

# Приватный семейный режим MAX ↔ Telegram

## Готово, когда

Исходное состояние: MAX-сообщения зеркалируются в Telegram, а любое сообщение
из Telegram topic может уйти обратно в MAX; имена отправителей добавляются в
текст, медиа скачивается до проверки opt-in, а retry после неоднозначного MAX
ACK допускает повторную попытку.

Действие человека: разрешённый пользователь отправляет в разрешённой теме
сообщение, начинающееся с entity-mention bridge-бота; обычное сообщение,
сообщение неразрешённого пользователя, сообщение из DM, неизвестный MAX-чат и
медиа без mention остаются без внешнего действия.

Видимый итог: в MAX ровно один раз появляется очищенный текст/медиа без имени
Telegram-автора; reply на сообщение того же разрешённого MAX-чата становится
native reply; исходное Telegram-сообщение получает 👀, затем 👍 только после
реального `max_msg_id`, либо 🤔/👎 по безопасному результату. MAX-сообщения
продолжают приходить в свои topics, а прямое MAX-сообщение явно помечается как
написанное напрямую в MAX.

Чего быть не должно: автосообщений из Telegram без opt-in, скачивания
неактивного медиа, передачи sender name/username/user id или внутренней цитаты,
создания topic для неизвестного MAX-чата, blind resend после потерянного ACK,
повторной отправки при duplicate update/restart, выхода временного файла из
dedicated tmp и утечки секретов/полного текста в логах.

## Источники и границы

- Требования владельца из входящего задания 2026-09-12.
- Текущие границы `TelegramAdapter → BridgeCore → MaxAdapter`, SQLite
  repository и PyMax-only egress boundary из `CLAUDE.md`.
- Не делать Telegram userbot, личную Telegram-сессию, dashboard, public API,
  AI/analytics, третье хранилище сообщений или отправку во все MAX-чаты.
- Сохранять MAX egress abstraction и существующее MAX → Telegram направление.

## План

1. Добавить deny-by-default конфигурацию `TG_ALLOWED_USER_IDS`, обязательный
   `TG_FORUM_GROUP_ID` scope, `OUTBOUND_ENABLED` и явную проверку разрешённых
   MAX chat ids без `forward_all` production-режима.
2. Вынести проверку начала сообщения через Telegram entities и точное имя
   bridge-бота. До успешной проверки не скачивать медиа и не передавать
   событие в core; исключить edited updates и сообщения из DM.
3. Убрать Telegram author metadata из MAX-текста, оставить reply mapping только
   для сообщения того же MAX-чата, добавить исходный источник прямого MAX.
4. Сделать безопасные временные файлы: UUID-имя, allowlist расширений/типов,
   размер, containment, `noexec`-tmp и cleanup.
5. Добавить durable outbound operation state с уникальным Telegram ключом,
   lease/TTL, реакциями состояния и retry только для доказуемо unsent ошибок;
   timeout/потерянный ACK перевести в `unknown` без автоматического повтора.
6. Реализовать Telegram reaction update как отдельную retry-операцию, не
   повторяющую MAX send; при выключенном outbound не запускать и очередь.
7. Покрыть сценарии A–Q из задания unit/integration-моками и SQLite-тестами,
   обновить существующие конфликтующие ожидания.
8. Обновить README-ru, deployment/runbook, env-примеры, Docker Compose и
   `.gitignore` без реальных токенов, телефонов и user IDs.

## Проверки блока

- `pytest -q` и новые regression/security tests в окружении с Python 3.13.
- Статический поиск секретов, plaintext message logging и небезопасных путей.
- Проверка Compose: non-root, read-only, `cap_drop: ALL`, no public ports по
  умолчанию, data/session volume, dedicated tmpfs, healthcheck.
- Сквозной fake-backend сценарий: разрешённый mention → одна MAX-операция и
  правильная реакция; все deny-by-default ветки → ноль MAX/Telegram side effects.
