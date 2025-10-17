import asyncio
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import dateparser
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import CallbackContext, ConversationHandler

from monbot.config import REPORT_DASHBOARD_ID, REPORT_PREGEN_MONTHS, REPORT_PREGEN_WEEKS, REPORT_STORAGE_DIR
from monbot.db import UserDB
from monbot.graph_service import GraphService
from monbot.handlers.common import escape_markdown_v2, format_duration, get_tz, is_allowed_user
from monbot.handlers.consts import *
from monbot.handlers.keyboards import build_hosts_keyboard, build_report_confirm_kb, build_report_list_kb
from monbot.handlers.texts import *
from monbot.items_index import ItemsIndex
from monbot.maintenance_service import MaintenanceService
from monbot.report_service import ReportPeriod, ReportService


async def help_cmd(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  uid = update.effective_user.id
  role = await db.get_role(uid)
  if not role:
    await update.message.reply_text(HELP_NEED_INVITE)
    return
  if role == ROLE_ADMIN:
    await update.message.reply_text(HELP_ADMIN)
  elif role == ROLE_MAINTAINER:
    await update.message.reply_text(HELP_MAINTAINER)
  else:
    await update.message.reply_text(HELP_VIEWER)


async def start_register(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  tg_user = update.effective_user
  db_user_role = await db.get_role(tg_user.id)
  if db_user_role:
    await db.add_or_update_user(
      tg_user.id, role=db_user_role, username=tg_user.username, first_name=tg_user.first_name, last_name=tg_user.last_name
    )
    await update.message.reply_text(START_EXISTING_USER.format(name=f"@{tg_user.username}", role=db_user_role))
    return

  args = context.args or []
  if not args:
    await update.message.reply_text(START_INVITE_REQUIRED)
    return

  otp = args[0]
  role = await db.consume_invite(otp)
  if not role:
    await update.message.reply_text(START_INVITE_INVALID)
    return

  await db.add_or_update_user(
    tg_user.id, role=role, username=tg_user.username, first_name=tg_user.first_name, last_name=tg_user.last_name
  )
  await update.message.reply_text(START_INVITE_OK_FMT.format(role=role))


async def start_conv(update: Update, context: CallbackContext, conv_type: str):
  db: UserDB = context.application.bot_data[CTX_DB]
  user = update.effective_user
  if not await is_allowed_user(db, user.id):
    await update.message.reply_text(ACCESS_DENIED)
    return ConversationHandler.END
  await db.upsert_user_info_throttled(user, min_interval_sec=3600)

  allow_hosts = context.application.bot_data[CTX_ALLOW_HOSTS]
  host_names = list(allow_hosts.values())
  markup = build_hosts_keyboard(host_names, conv_type)
  await update.message.reply_text(DEVICE_SELECT_TITLE, reply_markup=markup)
  return SELECTING


async def start_graphs(update: Update, context: CallbackContext):
  return await start_conv(update, context, CONV_TYPE_GRAPH)


async def start_maint(update: Update, context: CallbackContext):
  return await start_conv(update, context, CONV_TYPE_MAINT)


async def invgen(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  user = update.effective_user
  if not await db.is_admin(user.id):
    return
  if not context.args:
    await update.message.reply_text(INVGEN_USAGE)
    return
  role = context.args[0]
  if role not in VALID_ROLES:
    await update.message.reply_text(INVALID_ROLE)
    return
  max_uses = 1
  ttl_sec = None
  if len(context.args) >= 2:
    try:
      max_uses = int(context.args[1])
    except Exception:
      pass
  if len(context.args) >= 3:
    try:
      ttl_sec = int(context.args[2])
    except Exception:
      pass
  otp = await db.create_invite(role, max_uses=max_uses, ttl_sec=ttl_sec)
  me = await context.bot.get_me()
  bot_name = me.username or ""
  link = f"https://t.me/{bot_name}?start={otp}" if bot_name else f"/start {otp}"
  ttl = format_duration(ttl_sec) if ttl_sec else "вечно"
  await update.message.reply_text(
    escape_markdown_v2(INVITE_REPLY_FMT.format(link=link, role=role, max_uses=max_uses, ttl=ttl)),
    parse_mode=ParseMode.MARKDOWN_V2,
  )


async def adduser(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  user = update.effective_user
  if not await db.is_admin(user.id):
    return
  if not context.args:
    await update.message.reply_text(ADDUSER_USAGE)
    return
  try:
    uid = int(context.args[0])
    role = context.args[1] if len(context.args) > 1 else ROLE_VIEWER
    if role not in VALID_ROLES:
      await update.message.reply_text(INVALID_ROLE)
      return
    await db.add_or_update_user(uid, role=role)
    await update.message.reply_text(ADDUSER_OK_FMT.format(uid=uid, role=role))
  except ValueError:
    await update.message.reply_text(INVALID_TELEGRAM_ID)


async def setrole(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  user = update.effective_user
  if not await db.is_admin(user.id):
    return
  if len(context.args) < 2:
    await update.message.reply_text(SETROLE_USAGE)
    return
  try:
    uid = int(context.args[0])
    role = context.args[1]
    if role not in VALID_ROLES:
      await update.message.reply_text(INVALID_ROLE)
      return
    ok = await db.set_role(uid, role)
    await update.message.reply_text(SETROLE_OK_FMT.format(uid=uid, role=role, ok=ok))
  except ValueError:
    await update.message.reply_text(INVALID_TELEGRAM_ID)


async def listusers(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  user = update.effective_user
  if not await db.is_admin(user.id):
    return
  users = await db.list_users()
  if not users:
    await update.message.reply_text(USERS_EMPTY)
    return
  lines = [LIST_USERS_HEADER]
  for uid, role, username, first_name, last_name in users:
    disp = f"{first_name or ''} {last_name or ''}".strip()
    uname = f"@{username}" if username else ""
    who = disp or uname or str(uid)
    lines.append(f"`{uid}`:{role}: {uname} ({who})")

  text = escape_markdown_v2("\n".join(lines))
  await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2)


async def deluser(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  user = update.effective_user
  if not await db.is_admin(user.id):
    return
  if not context.args:
    await update.message.reply_text(DELUSER_USAGE)
    return
  try:
    uid = int(context.args[0])
    ok = await db.delete_user(uid)
    await update.message.reply_text(DELUSER_OK_FMT.format(uid=uid, ok=ok))
  except ValueError:
    await update.message.reply_text(INVALID_TELEGRAM_ID)


async def settz(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  user = update.effective_user
  if not context.args:
    cur = await db.get_timezone(user.id)
    await update.message.reply_text(SETTZ_CURRENT_FMT.format(tz=cur))
    return
  tz = context.args[0]
  try:
    ZoneInfo(tz)
  except Exception:
    await update.message.reply_text(SETTZ_INVALID)
    return
  await db.set_timezone(user.id, tz)
  await update.message.reply_text(SETTZ_OK_FMT.format(tz=tz))


async def refresh(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  user = update.effective_user
  if not await db.is_admin(user.id):
    return

  items_idx: ItemsIndex = context.application.bot_data[CTX_ITEMS]
  await items_idx.refresh()

  msvc: MaintenanceService = context.application.bot_data[CTX_MAINT_SVC]
  for hid, _disp in context.application.bot_data[CTX_ALLOW_HOSTS].items():
    for it in items_idx.items_by_hostid(hid) or []:
      try:
        await asyncio.to_thread(msvc.ensure_container, it.itemid)
      except Exception:
        pass

  gsvc: GraphService = context.application.bot_data.get(CTX_GRAPH_SVC)
  if gsvc:
    gsvc.clear_signature_cache()

  await update.message.reply_text(REFRESH_DONE)


async def report_cmd(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  if not await is_allowed_user(db, update.effective_user.id):
    await update.message.reply_text(ACCESS_DENIED)
    return

  args = context.args or []
  if not args:
    await update.message.reply_text(REPORT_USAGE)
    return

  kind_raw = (args[0] or "").lower()
  if kind_raw.startswith(("w", "н")):
    period_type = "week"
  elif kind_raw.startswith(("m", "м")):
    period_type = "month"
  elif kind_raw.startswith(("l", "c")):
    await report_list_cmd(update, context)
    return
  else:
    await update.message.reply_text(REPORT_BAD_PERIOD)
    return

  tz = await get_tz(update, context)
  svc = ReportService(context.application.bot_data[CTX_ZBX], tz=tz)

  when_text = " ".join(args[1:]).strip() if len(args) > 1 else ""
  if when_text:
    dt = dateparser.parse(
      when_text,
      languages=['ru', 'en'],
      settings={
        'DATE_ORDER': 'DMY',
        'PREFER_DATES_FROM': 'future',
        'TIMEZONE': str(tz),
      }
    )
    if not dt:
      await update.message.reply_text(REPORT_DATE_PARSE_FAIL)
      return
    d = dt.date()
    if period_type == "week":
      s, e, _ = svc.week_bounds_by_any_date(d)
    else:
      s, e, _ = svc.month_bounds_by_any_date(d)
    period = ReportPeriod(start_ts=s, end_ts=e, label="")
  else:
    period = svc.last_week_period() if period_type == "week" else svc.last_month_period()

  start_s = datetime.fromtimestamp(period.start_ts, tz).strftime(DT_FMT)
  end_s = datetime.fromtimestamp(period.end_ts, tz).strftime(DT_FMT)
  title = REPORT_CONFIRM_TITLE_WEEK if period_type == "week" else REPORT_CONFIRM_TITLE_MONTH
  text = f"{title}\n{REPORT_CONFIRM_RANGE_FMT.format(start=start_s, end=end_s)}"

  kb = build_report_confirm_kb(period_type, period.start_ts, period.end_ts)
  await update.message.reply_text(text, reply_markup=kb)

async def report_confirm_action(update: Update, context: CallbackContext):
  q = update.callback_query
  await q.answer()
  data = q.data or ""

  if data == CB_REPORT_CANCEL:
    try:
      await q.edit_message_text(REPORT_CANCELLED)
    except Exception:
      pass
    return

  m = re.match(rf"^{CB_REPORT_CONFIRM}:(week|month):(\d+):(\d+)$", data)
  if not m:
    return
  period_type = m.group(1)
  start_ts = int(m.group(2))
  end_ts = int(m.group(3))

  db: UserDB = context.application.bot_data[CTX_DB]
  tz = await get_tz(update, context)
  svc = ReportService(context.application.bot_data[CTX_ZBX], tz=tz)
  period = ReportPeriod(start_ts=start_ts, end_ts=end_ts, label="")

  # Fast existence check to decide whether to show waiting spinner
  out_path = svc.resolve_report_path(REPORT_STORAGE_DIR, REPORT_DASHBOARD_ID, period_type, period)
  already_exists = out_path.exists()
  try:
    # Remove keyboard first
    await q.edit_message_reply_markup(reply_markup=None)
    # Only show waiting text if we need to actually generate
    if not already_exists:
      await q.edit_message_text(REPORT_SENDING)
  except Exception:
    pass

  # Ensure report exists (persist path in DB), offloaded in ReportService (see patch below)
  path = await svc.ensure_report_file(db, REPORT_DASHBOARD_ID, period_type, period, REPORT_STORAGE_DIR)

  # Reuse Telegram file_id if already uploaded
  rec = await db.get_report_record(REPORT_DASHBOARD_ID, period_type, start_ts)
  cached_file_id = rec[1] if rec else None

  caption = f"{datetime.fromtimestamp(start_ts, tz).strftime(DT_FMT)} — {datetime.fromtimestamp(end_ts, tz).strftime(DT_FMT)}"

  try:
    if cached_file_id:
      await q.message.reply_document(document=cached_file_id, caption=caption)
    else:
      with open(path, "rb") as f:
        msg = await q.message.reply_document(document=f, filename=Path(path).name, caption=caption)
      if msg and msg.document:
        await db.set_report_file_id(REPORT_DASHBOARD_ID, period_type, start_ts, msg.document.file_id)
  except Exception as e:
    try:
      await q.edit_message_text(str(e))
    except Exception:
      pass

async def report_list_cmd(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  if not await is_allowed_user(db, update.effective_user.id):
    await update.message.reply_text(ACCESS_DENIED)
    return

  tz = await get_tz(update, context)
  svc = ReportService(context.application.bot_data[CTX_ZBX], tz=tz)
  now = datetime.now(tz)

  # Weeks: last REPORT_PREGEN_WEEKS completed weeks
  monday_this = now.date() - timedelta(days=(now.isoweekday() - 1))
  week_buttons: list[tuple[str, int]] = []
  for i in range(1, max(0, REPORT_PREGEN_WEEKS) + 1):
    monday = monday_this - timedelta(days=7 * i)
    s, e, _ = svc.week_bounds_by_any_date(monday)
    sd = datetime.fromtimestamp(s, tz).date()
    # human-friendly: week number and start..end-1
    iso_year, iso_week, _w = sd.isocalendar()
    ed = (datetime.fromtimestamp(e, tz).date() - timedelta(days=1))
    label = f"Нед.{iso_week}/{iso_year} {sd.strftime('%d.%m')}–{ed.strftime('%d.%m')}"
    week_buttons.append((label, s))

  # Months: last REPORT_PREGEN_MONTHS completed months
  y, m = now.year, now.month
  month_buttons: list[tuple[str, int]] = []
  for _ in range(max(0, REPORT_PREGEN_MONTHS)):
    # previous month
    m -= 1
    if m == 0:
      m = 12
      y -= 1
    any_day = date(y, m, 15)
    s, e, _ = svc.month_bounds_by_any_date(any_day)
    sd = datetime.fromtimestamp(s, tz).date()
    label = sd.strftime("%Y-%m")
    month_buttons.append((label, s))

  kb = build_report_list_kb(week_buttons, month_buttons)
  await update.message.reply_text(REPORT_LIST_TITLE, reply_markup=kb)

async def report_send_action(update: Update, context: CallbackContext):
  q = update.callback_query
  await q.answer()
  data = q.data or ""

  # ignore captions/cancel
  if data == CB_REPORT_CANCEL:
    return

  m = re.match(rf"^{CB_REPORT_SEND}:(week|month):(\d+)$", data)
  if not m:
    return
  period_type = m.group(1)
  start_ts = int(m.group(2))

  db: UserDB = context.application.bot_data[CTX_DB]
  tz = await get_tz(update, context)
  svc = ReportService(context.application.bot_data[CTX_ZBX], tz=tz)

  # Compute end_ts from start_ts robustly
  if period_type == "week":
    d = datetime.fromtimestamp(start_ts, tz).date()
    s, e, _ = svc.week_bounds_by_any_date(d)
  else:
    d = datetime.fromtimestamp(start_ts, tz).date()
    s, e, _ = svc.month_bounds_by_any_date(d)
  period = ReportPeriod(start_ts=s, end_ts=e, label="")

  # Ensure or reuse saved file
  path = await svc.ensure_report_file(db, REPORT_DASHBOARD_ID, period_type, period, REPORT_STORAGE_DIR)

  # Reuse Telegram file_id when available
  rec = await db.get_report_record(REPORT_DASHBOARD_ID, period_type, start_ts)
  cached_file_id = rec[1] if rec else None

  caption = f"{datetime.fromtimestamp(s, tz).strftime(DT_FMT)} — {datetime.fromtimestamp(e, tz).strftime(DT_FMT)}"

  if cached_file_id:
    await q.message.reply_document(document=cached_file_id, caption=caption)
  else:
    with open(path, "rb") as f:
      msg = await q.message.reply_document(document=f, filename=Path(path).name, caption=caption)
    if msg and msg.document:
      await db.set_report_file_id(REPORT_DASHBOARD_ID, period_type, start_ts, msg.document.file_id)
