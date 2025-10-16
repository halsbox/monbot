import asyncio
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import CallbackContext, ConversationHandler

from monbot.db import UserDB
from monbot.graph_service import GraphService
from monbot.handlers.common import escape_markdown_v2, format_duration, is_allowed_user
from monbot.handlers.consts import *
from monbot.handlers.keyboards import build_hosts_keyboard
from monbot.handlers.texts import *
from monbot.items_index import ItemsIndex
from monbot.maintenance_service import MaintenanceService


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
