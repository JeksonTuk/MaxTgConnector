> **Статус:** реализовано (реальный тест выполнен 2026-09-14, см. § Результат)
> **Дата:** 2026-09-13
> **Назначение:** План реализации
> **Связанное решение:** нет

# Один контролируемый A/B-тест SMS-авторизации MAX

## Артефакт

Изолированный `auth-test` для PyMax 2.4.1 на Python 3.13. Он использует
существующую фабрику `create_pymax_client`, тот же `BridgeClient`/TCP-boundary,
текущий DESKTOP user-agent и штатный `SmsAuthFlow`, но не запускает bridge,
Telegram или фоновые циклы.

## Готово, когда

- рабочее дерево содержит отдельные `Dockerfile.auth-test`, Compose и скрипт;
- первый режим требует интерактивный stdin и технически разрешает один
  `AUTH_REQUEST`;
- второй режим использует только отдельную сохранённую сессию и запрещает
  новую авторизацию;
- при EOF, тайм-ауте, `limit.violate` и ошибке пароля процесс завершается;
- офлайн-тесты A–I подтверждают эти ограничения;
- staging старого сервера, Telegram, реальные токены и MAX-сессии не читались.

Видимый безопасный признак успеха будущего запуска:
`AUTH_TEST event=profile_check success=True`, затем
`AUTH_TEST event=session_save success=True`.

## Трогать только

- `scripts/max_sms_auth_test.py`;
- `deploy/Dockerfile.auth-test`;
- `deploy/docker-compose.auth-test.yml`;
- `deploy/auth-test.env.example`;
- `tests/test_max_sms_auth_test.py`;
- этот план.

## Не трогать

- действующий bridge, Telegram и staging Compose;
- старые volumes, session-файлы и секреты;
- `device_type`, `app_version`, fingerprint/calls_seed и текущий auth-flow;
- IP, proxy и параметры аккаунта MAX;
- firewall, глобальные Docker-настройки и постоянный запуск.

## Стоп-гейты

1. До отдельного сообщения пользователя `разрешаю реальный тест` не выполнять
   `AUTH_REQUEST`, SMS, RESEND, QR, запуск bridge или постоянного контейнера.
2. После подготовки и офлайн-тестов остановиться на контрольной точке №1.
3. Даже после разрешения выполнить только один запуск первого режима, без
   перезапуска ради новой попытки; второй режим запускать отдельно только
   после результата первого.

## Реализация

### 1. Изоляция

Compose создаёт отдельный проектный volume `max_sms_auth_test_session`, не
подключает `data.staging` и не публикует порты. Контейнер работает от UID
10001, с `read_only`, `tmpfs`, `cap_drop: ALL`, `no-new-privileges`, лимитами
512 MB / 0.50 CPU / 128 PID и `restart: no`.

### 2. Первый запуск

`OneShotSmsAuthFlow` делегирует реальному `SmsAuthFlow` PyMax. Вызов
`AuthService.request_code()` обёрнут бюджетом: первый вызов разрешён, второй
сразу блокируется локально. Провайдеры ввода не показывают телефон, читают
SMS и пароль только из приватного терминала и имеют одну попытку с тайм-аутом.
В `ExtraConfig` сохраняются текущие user-agent/sync-настройки, а для теста
явно устанавливаются `reconnect=False`, `relogin=False` и
`password_max_attempts=1`.

### 3. Второй запуск

`SessionOnlyAuthFlow` немедленно завершает процесс, если PyMax не смог
использовать сессию. Поэтому отсутствие, повреждение или отклонение сессии
не приводит к SMS, QR, `AUTH_REQUEST` или RESEND.

### 4. Сетевой путь

Тип egress обязателен в приватном env-файле: `direct` либо
`http_connect`. При `http_connect` URL прокси читается только процессом и
не выводится. Для A/B нужно указать тот же согласованный proxy-режим, что и
в предыдущем запуске; сам скрипт не переключает egress автоматически.

## Риски и проверки

| Риск | Защита | Вердикт |
|---|---|---|
| stdin недоступен | проверка `isatty()` до создания клиента | покрывается тестом A |
| повторный `AUTH_REQUEST` | бюджет и `reconnect/relogin=False` | покрывается тестом B |
| retry после ожидания кода | один provider-вызов + общий timeout | покрывается тестом C |
| `limit.violate` | terminal classification, без retry | покрывается тестом D |
| EOF | terminal provider exception | покрывается тестом E |
| бесконечный парольный цикл | `password_max_attempts=1`, provider без retry | покрывается тестом F |
| новая auth во втором запуске | `SessionOnlyAuthFlow`, импорт legacy отключён | покрывается тестом G |
| подмена auth-flow | делегирование именно `SmsAuthFlow` | покрывается тестом H |
| контейнерный drift | статическая проверка Compose | покрывается тестом I |

Локальный Python и Docker на Windows не требуются. Если офлайн-тесты нельзя
запустить в удалённом Docker без разрешения на серверное действие, их нужно
отметить как `не выполнены`, а не заменять реальной авторизацией.

## Результат (2026-09-14)

Площадка — временный российский VPS (адрес и реквизиты в репозиторий не
заносятся), код из commit `3f8e815`, отдельная папка `/opt/maxgram-auth-test`,
egress `direct`, без прокси. Staging старого сервера не запускался.

1. Офлайн-проверки A–I на этой площадке: `9 passed, 1 warning` (warning —
   кеш pytest в read-only FS).
2. Первый режим, один запуск: `start_auth_response` → SMS пришла →
   `profile_check success=True` → `session_save success=True` →
   `finished mode=first outcome=success auth_request_count=1
   blocked_auth_requests=0`. Password challenge не потребовался.
3. Второй режим, один запуск: `profile_check success=True` →
   `session_reuse success=True` → `finished mode=reuse outcome=success
   auth_request_count=0`.

**Вывод A/B.** При том же PyMax 2.4.1, том же DESKTOP user-agent,
`app_version` из `VersionCatalog().recommended()` и штатном `SmsAuthFlow`
авторизация с российского сетевого выхода проходит с первого AUTH_REQUEST.
Прежний сбой «StartAuthResponse есть, SMS нет» относился к сетевому пути
(зарубежный выход / прокси), а не к клиенту, версии или профилю устройства.

После теста на площадке остаются: том `max_sms_auth_test_session` с сессией
и приватный `auth-test.env`; постоянных контейнеров нет. Дальнейший перенос
bridge на российский выход — отдельное решение, этим планом не покрывается.

## Команды на сервере (справочно)

Команды ниже показываются для контрольной точки; до отдельного разрешения
реальный запуск не выполнять.

Офлайн-проверки выполняются в уже существующем тестовом образе проекта,
который использует Python 3.13 и `requirements-dev.txt`, с отключённой сетью:

```bash
docker compose -p max-sms-auth-offline -f deploy/docker-compose.test.yml \
  build

docker compose -p max-sms-auth-offline -f deploy/docker-compose.test.yml \
  run --rm tests pytest -q tests/test_max_sms_auth_test.py
```

Для реального auth-test тип egress должен быть выбран до разрешения запуска.
Это важно для смысла A/B: MAX видит IP сетевого выхода, а не обязательно
адрес самого VPS. Если предыдущий запуск шёл через `home_ru_proxy`, то
российский VPS с тем же proxy не изменит видимый MAX IP; менять proxy
автоматически нельзя.

```bash
docker compose --env-file auth-test.env -p max-sms-auth-test \
  -f deploy/docker-compose.auth-test.yml build

# первый режим — CMD образа по умолчанию; нужен живой TTY (ssh -t)
docker compose --env-file auth-test.env -p max-sms-auth-test \
  -f deploy/docker-compose.auth-test.yml run --rm \
  -e AUTH_TEST_TIMEOUT_SECONDS=300 max-sms-auth-test

# второй режим — команда переопределяется целиком (голый `--mode reuse`
# Compose воспринял бы как исполняемый файл)
docker compose --env-file auth-test.env -p max-sms-auth-test \
  -f deploy/docker-compose.auth-test.yml run --rm max-sms-auth-test \
  python scripts/max_sms_auth_test.py --mode reuse
```

`auth-test.env` создаёт оператор на сервере из шаблона и вводит туда только
свои реальные значения. Значения не присылаются в чат. Команда `build` не
запускает приложение; обе команды `run` запускают ровно один процесс и
удаляют одноразовый контейнер после завершения, сохраняя только отдельный
volume сессии.
