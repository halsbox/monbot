from monbot.handlers.consts import VALID_ROLES

# Captions
HOST_SELECT_TITLE = "Выберите устройство:"
ITEM_SELECT_TITLE = "{host}: выберите датчик"
ITEM_STATUS_ACTIVE = "Период обслуживания активен"
ITEM_STATUS_INACTIVE = "Нет активного периода обслуживания"
PERIODS_EMPTY = "—"
ACCESS_DENIED = "Доступ запрещен. Попросите администратора выдать доступ."
DEVICE_SELECT_TITLE = "Устройство:"  # used by /start
NO_ITEMS_FOR_HOST = "Нет доступных датчиков для этого устройства."
RESULT_FAST_ADDED = "Добавлен период обслуживания на 24 часа, начиная с сейчас."
RESULT_ENDED = "Активный период обслуживания завершен."
# Split regex for parsing custom period input (non-capturing; safe for ISO dates)
PERIOD_SPLIT_REGEX = r"(?:\n|;|\s+-\s+)"

# Errors and prompts
PARSE_FAIL_END = "Не удалось распознать когда закончить обслуживание. Попробуйте еще раз."
PARSE_FAIL_BOTH = "Не удалось распознать когда начать и когда закончить обслуживание. Попробуйте еще раз."
INVALID_PERIOD_MSG = "Неправильный период обслуживания (окончание в прошлом или не позже начала). Попробуйте еще раз.."
INSUFFICIENT_PERMISSIONS = "Недостаточно прав."

# Periods list formatting
PERIODS_FUTURE_CAPTION = "*Запланированные:*"
PERIODS_ACTIVE_CAPTION = "*Активные:*"
PERIODS_FINISHED_CAPTION = "*Завершенные:*"
ACTIVE_BULLET = "●"
INACTIVE_BULLET = "○"
PERIOD_LINE_FMT = "{bullet} {start} → {end} ({duration})"
DT_FMT = "%d.%m.%Y %H:%M"

# Graphs keyboard extra button
BTN_DEVICE = "Устройство"
INPUT_INSTRUCTIONS = (
  "Введите начало и конец периода обслуживания, разделенные переводом строки, `;` или ` - `. "
  "Можно ввести только время окончания, в таком случае обслуживание начнется немедленно. "
  "Когда начать и закончить (или только когда закончить) можно указать в любом стандартном формате или на естественном языке. "
  "Обе даты/времени, введенные естественным языком, должны отвечать на вопрос \"Когда?\" "
  "Если указать день/дату без указания времени, то время будет считаться равным текущему.\n"
  "Примеры: \n"
  "✓ `23.11.2025 11:40 - 02.12.2025 14:00`\n"
  "✓ `через 10 мин; завтра в 14:00`\n"
  "✓ `послезавтра` (равносильно `сейчас - через 2 дня`)\n"
  "✓ `через 2ч - в четверг в 11:15`\n"
  "✓ `в среду в 23:12; 17 окт в 08:15`"
)
INPUT_PERIOD_REQUEST_FMT = "Обслуживание {item_name}:"
INPUT_PERIOD_REQUEST_PLACEHOLDER = "в четверг в 14:30"
CONFIRM_ADD_PROMPT_FMT = "*Добавить период обслуживания?*\n\n✓ {start} → {end} ({duration})\n\n"
CONFIRM_UPDATE_PROMPT_FMT = "*Обновить период обслуживания?*\n\n✓ {start} → {end} ({duration})\n\n"
CONFIRM_END_PROMPT = "Завершить текущий период обслуживания прямо сейчас?"

# Buttons
BTN_MAINT = "⚙ Обслуживание"
BTN_GRAPH = "📈 График"
BTN_BACK_HOST = "◀ Назад"
BTN_BACK_ITEMS = "◀ Назад"
BTN_ADD_DAY_NOW = "➕ на 1д с сейчас"
BTN_END_NOW = "⏹ Завершить сейчас"
BTN_NEW_PERIOD = "📝 Новый период"
BTN_CONFIRM = "✅ Подтвердить"
BTN_CANCEL = "❌ Отмена"

# Preset prolong durations for active period (C)
# list of (label, seconds)
PRESET_ACTIVE_PROLONG = [
  ("продлить на 1ч", 3600),
  ("продлить на 4ч", 4 * 3600),
  ("продлить на 12ч", 12 * 3600),
  ("продлить на 1д", 24 * 3600),
  ("продлить на 2д", 2 * 24 * 3600),
  ("продлить на 3д", 3 * 24 * 3600),
]

# Preset quick additions for custom entry screen (E)
PRESET_CUSTOM_QUICK = [
  ("на 1ч с сейчас", 3600),
  ("на 4ч с сейчас", 4 * 3600),
  ("на 12ч с сейчас", 12 * 3600),
  ("на 1д с сейчас", 24 * 3600),
  ("на 2д с сейчас", 2 * 24 * 3600),
  ("на 3д с сейчас", 3 * 24 * 3600),
]

HELP_NEED_INVITE = "Требуется приглашение. Попросите администратора сгенерировать ссылку."
HELP_VIEWER = (
  "Доступные команды:\n"
  "/monbot graphs — графики\n"
  "/monbot sensors — датчики\n"
  "/monbot maint — обслуживание (только просмотр)\n"
  "/monbot report <list|week|month> [когда]\n"
  "/monbot settz <TZ> — установить часовой пояс"
)
HELP_MAINTAINER = (
  "Доступные команды:\n"
  "/monbot graphs — графики\n"
  "/monbot sensors — датчики\n"
  "/monbot maint — обслуживание\n"
  "/monbot report <list|week|month> [когда]\n"
  "/monbot settz <TZ>"
)
HELP_ADMIN = (
  "Доступные команды:\n"
  "/monbot graphs — графики\n"
  "/monbot sensors — датчики\n"
  "/monbot maint — обслуживание\n"
  "/monbot report <list|week|month> [когда]\n"
  "/monbot settz <TZ>\n"
  "/monbot listusers — список пользователей\n"
  "/monbot invite <role> [max_uses] [ttl_sec] — создать приглашение\n"
  "/monbot adduser <mattermost_id> [role] — добавить/обновить пользователя\n"
  "/monbot setrole <mattermost_id> <role> — изменить роль\n"
  "/monbot deluser <mattermost_id> — удалить пользователя\n"
  "/monbot refresh — обновить кэш\n"
  "/monbot audit [фильтр] — последние записи аудита\n\n"
  "Доступные роли: {roles}".format(roles=", ".join(VALID_ROLES))
)
START_INVITE_REQUIRED = "Требуется приглашение. Используйте /monbot start <otp> из ссылки приглашения."
START_INVITE_INVALID = "Неверный или просроченный код приглашения."
START_INVITE_OK_FMT = "Добро пожаловать. Ваша роль: {role}. Используйте /monbot help."
START_EXISTING_USER = "Привет, {name}. Ваша роль: {role}. Используйте /monbot help."

INVGEN_USAGE = "Использование: /monbot invite <role> [max_uses=1] [ttl_sec]"
INVALID_ROLE = "Роль должна быть одна из: {roles}".format(roles=", ".join(VALID_ROLES))
INVITE_REPLY_FMT = "Нажмите на пригласительную ссылку чтобы скопировать:\n`{link}`\n\nРоль: {role}\nМакс. пользователей: {max_uses}\nВремя жизни: {ttl}"
LIST_USERS_HEADER = "Нажмите на UID чтобы скопировать:"

ADDUSER_USAGE = "Использование: /monbot adduser <mattermost_id> [role]"
SETROLE_USAGE = "Использование: /monbot setrole <mattermost_id> <role>"
DELUSER_USAGE = "Использование: /monbot deluser <mattermost_id>"
INVALID_TELEGRAM_ID = "Некорректный mattermost_id."
ADDUSER_OK_FMT = "Пользователь {uid} добавлен/обновлюн с ролью {role}."
SETROLE_OK_FMT = "Пользователь {uid} установка роли {role}: {ok}"
DELUSER_OK_FMT = "Пользователь {uid} удаление: {ok}"
USERS_EMPTY = "Нет пользователей."
SETTZ_CURRENT_FMT = "Ваш часовой пояс: {tz}. Использование: /monbot settz <IANA TZ, e.g., Europe/Moscow>"
SETTZ_INVALID = "Некорректный часовой пояс. Пример: Europe/Moscow"
SETTZ_OK_FMT = "Установлен часовой пояс {tz}."
REFRESH_DONE = "Индексы обновлены"

# Reports
REPORT_USAGE = "Использование: /monbot report <list|week|month> [когда]"
REPORT_BAD_PERIOD = "Первый аргумент должен быть 'list', 'week' или 'month'."
REPORT_DATE_PARSE_FAIL = "Не удалось распознать дату. Примеры: '5 мая', 'авгут', '15.09.2025'."
REPORT_CONFIRM_TITLE_WEEK = "Отчёт за неделю"
REPORT_CONFIRM_TITLE_MONTH = "Отчёт за месяц"
REPORT_CONFIRM_RANGE_FMT = "{start} - {end}"
REPORT_SENDING = "Формирую отчёт, пожалуйста подождите…"
REPORT_CANCELLED = "Отчёт отменён."
BTN_REPORT_CONFIRM = "✅ Сформировать"
BTN_REPORT_CANCEL = "❌ Отмена"

# Reports list
REPORT_LIST_TITLE = "Выберите отчёт:"
REPORT_LIST_WEEKS_CAP = "Недели:"
REPORT_LIST_MONTHS_CAP = "Месяцы:"

AUDIT_EMPTY = "Нет записей аудита."
AUDIT_USAGE = "Использование: /audit [фильтр_по_хосту_или_датчику]"

# action verbs (display)
AUDIT_VERBS = {
  "create": "добавил(а)",
  "update": "обновил(а)",
  "delete": "удалил(а)",
  "end": "завершил(а)",
}

AUDIT_LINE_DT_FMT = "%d.%m %H:%M"

AUDIT_LINE_FMT = (
  "⏱ {dt} *{item}*\n"
  "@{user} {verb}:\n"
  "{period}"
)
