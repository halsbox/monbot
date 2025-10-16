import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import ForceReply, InlineKeyboardMarkup, MaybeInaccessibleMessage, Message, Update
from telegram.constants import ParseMode
from telegram.ext import CallbackContext, ConversationHandler

from monbot.cache2 import ImageCache2
from monbot.config import IMG_HEIGHT, IMG_WIDTH, MAINT_LIST_LIMIT
from monbot.db import UserDB
from monbot.graph_service import GraphService
from monbot.handlers.common import check_user, clean_all_messages, clean_flow_and_pending, escape_markdown_v2, \
  format_duration, format_periods, get_cb_data_val, get_host_data, get_tz, is_allowed_user, is_maint_manager, parse_date
from monbot.handlers.consts import *
from monbot.handlers.keyboards import build_hosts_keyboard, build_time_keyboard_item, get_maint_items_keyboard, \
  maint_actions_kb, maint_confirm_kb, maint_custom_kb
from monbot.handlers.texts import *
from monbot.items_index import ItemsIndex
from monbot.maintenance_service import MaintenanceService
from monbot.tg_media import edit_or_send_graph

logger = logging.getLogger(__name__)


async def _send_confirm(update: Update, context: CallbackContext, start_ts: int, end_ts: int):
  logger.info("_send_confirm")
  tz = await get_tz(update, context)
  s_str = datetime.fromtimestamp(start_ts, tz).strftime(DT_FMT)
  e_str = datetime.fromtimestamp(end_ts, tz).strftime(DT_FMT)
  itemid = context.user_data.get(CTX_MAINT_ITEM_KEY)
  msg_id = context.user_data.get(CTX_MAINT_MSG_ID)
  if not msg_id:
    logger.debug("_send_confirm: no MAINT_MSG_ID in context; cannot edit")
    return
  try:
    action = context.user_data.get(CTX_MAINT_PENDING_ACTION)
    if MAINT_PENDING_ACTION_EXTEND == action:
      msg = CONFIRM_UPDATE_PROMPT_FMT
    else:
      msg = CONFIRM_ADD_PROMPT_FMT
    await context.bot.edit_message_text(
      chat_id=update.effective_chat.id,
      message_id=msg_id,
      text=escape_markdown_v2(
        msg.format(
          start=s_str, end=e_str, duration=format_duration(end_ts - start_ts)
        )
      ),
      parse_mode=ParseMode.MARKDOWN_V2,
      reply_markup=maint_confirm_kb(itemid),
    )
  except Exception as e:
    logger.warning("confirm message failed: %s", e)


async def _send_prompt(message: Message | MaybeInaccessibleMessage, context: CallbackContext, extra: str = ''):
  logger.info("_send_prompt extra=%s", extra)
  itemid = context.user_data.get(CTX_MAINT_ITEM_KEY)
  items_idx: ItemsIndex = context.application.bot_data[CTX_ITEMS]
  item_name = items_idx.get_item_name(itemid)
  text = INPUT_PERIOD_REQUEST_FMT.format(item_name=item_name)
  if extra:
    text += "\n\n*{extra}*".format(extra=extra)
  prompt = await message.reply_text(
    escape_markdown_v2(text),
    parse_mode=ParseMode.MARKDOWN_V2,
    reply_markup=ForceReply(input_field_placeholder=INPUT_PERIOD_REQUEST_PLACEHOLDER, selective=True),
  )
  context.user_data[CTX_MAINT_FORCE_MSG_ID] = prompt.message_id


async def _delete_prompt_and_reply(update: Update, context: CallbackContext):
  """Delete ForceReply prompt (F) and user reply (G) if present; clear ids."""
  logger.info("_delete_prompt_and_reply")
  chat_id = update.effective_chat.id
  force_id = context.user_data.pop(CTX_MAINT_FORCE_MSG_ID, None)
  reply_id = context.user_data.pop(CTX_MAINT_REPLY_MSG_ID, None)
  for mid in (reply_id, force_id):
    if mid:
      try:
        await context.bot.delete_message(chat_id=chat_id, message_id=mid)
      except Exception as e:
        logger.debug("delete message %s failed: %s", mid, e)


async def host_handler(update: Update, context: CallbackContext):
  logger.info("maint_select_host cb_data=%s", update.callback_query.data)
  await update.callback_query.answer()
  if not await check_user(update, context):
    return ConversationHandler.END
  host_name, hostid = await get_host_data(update, context, CONV_TYPE_MAINT)
  if not host_name or not hostid:
    return SELECTING

  context.user_data[CTX_HOST_ID] = hostid
  context.user_data[CTX_HOST_NAME] = host_name
  kb = await get_maint_items_keyboard(hostid, context)
  msg = await context.bot.send_message(
    chat_id=update.effective_chat.id,
    text=ITEM_SELECT_TITLE.format(host=host_name),
    reply_markup=kb,
  )
  await clean_all_messages(update, context)
  context.user_data[CTX_MAINT_MSG_ID] = msg.message_id
  return SELECTING


async def maint_select_item(update: Update, context: CallbackContext):
  logger.info("maint_select_item cb_data=%s", update.callback_query.data)
  await update.callback_query.answer()
  data = update.callback_query.data

  # Note: This handler is routed by pattern only for:
  #   - maint_item:{itemid}
  #   - maint_back_host
  # However, maint_action() directly calls maint_select_item(update, context)
  # after processing FAST/END/CONFIRM/CANCEL, reusing the same Update (and its data).
  # Therefore, we must handle those callback prefixes here as well.

  if data == CB_MAINT_BACK_HOST:
    allow_hosts = context.application.bot_data[CTX_ALLOW_HOSTS]
    host_names = list(allow_hosts.values())
    kb = build_hosts_keyboard(host_names, CONV_TYPE_MAINT)
    await update.callback_query.edit_message_text(HOST_SELECT_TITLE, reply_markup=kb)
    return SELECTING

  if data.startswith(CB_MAINT_ITEM_KEYS):
    # Covers: maint_item:{id}, maint_confirm:{id}, maint_end:{id}, maint_fast:{id}
    itemid = get_cb_data_val(data)
    context.user_data[CTX_MAINT_ITEM_KEY] = itemid
  elif data == CB_MAINT_CANCEL:
    # Re-render current item view after cancel; keep last itemid from context
    itemid = context.user_data.get(CTX_MAINT_ITEM_KEY)
    logger.debug("maint_select_item: cancel pressed, using last itemid=%s", itemid)
  else:
    logger.debug("maint_select_item: unexpected data=%s", data)
    return SELECTING
  if not itemid:
    logger.debug("maint_select_item: no itemid in context after %s", data)
    return SELECTING

  text, kb = await build_maint_view_for_item(context, update.effective_user.id, itemid)
  msg = update.callback_query.message
  await update.callback_query.edit_message_text(
    text,
    parse_mode=ParseMode.MARKDOWN_V2,
    reply_markup=kb
  )
  context.user_data[CTX_MAINT_MSG_ID] = msg.message_id
  return SELECTING


async def maint_action(update: Update, context: CallbackContext):
  logger.info("maint_action cb_data=%s", update.callback_query.data)
  await update.callback_query.answer()
  data = update.callback_query.data
  db: UserDB = context.application.bot_data[CTX_DB]

  logger.debug(
    "maint_action: flow=%s pending_action=%s item=%s",
    context.user_data.get(CTX_MAINT_FLOW_KEY),
    context.user_data.get(CTX_MAINT_PENDING_ACTION),
    context.user_data.get(CTX_MAINT_ITEM_KEY),
  )

  if data.startswith(CB_MAINT_ACTION_KEYS):
    if not await is_maint_manager(db, update.effective_user.id):
      await update.callback_query.edit_message_text(ACCESS_DENIED)
      return SELECTING

  msvc = context.application.bot_data.get(CTX_MAINT_SVC)
  if msvc is None:
    msvc = MaintenanceService(context.application.bot_data[CTX_ZBX])
    context.application.bot_data[CTX_MAINT_SVC] = msvc

  itemid = context.user_data.get(CTX_MAINT_ITEM_KEY)
  logger.debug("maint_action: itemid in context: %s", itemid)

  if data.startswith(CB_MAINT_FAST):
    logger.debug("maint_action: FAST")
    now = int(time.time())
    res = msvc.add_period(itemid, now, now + 86400, mtype=0)
    await db.audit_maint(update.effective_user.id, "create", res["maintenanceid"], itemid, res.get("hostid", ""),
                         res["before"], res["after"])
    return await maint_select_item(update, context)

  if data.startswith(CB_MAINT_END):
    logger.debug("maint_action: END")
    res = msvc.end_now(itemid)
    if res:
      await db.audit_maint(update.effective_user.id, "end", res["maintenanceid"], itemid, res.get("hostid", ""),
                           res["before"], res["after"])
    return await maint_select_item(update, context)

  if data.startswith(CB_MAINT_ADD):
    logger.debug("maint_action: ADD seconds=%s", get_cb_data_val(data))
    try:
      secs = int(get_cb_data_val(data))
    except Exception:
      return SELECTING

    # If active: show confirm to extend; else: confirm to add start=now..now+secs
    msvc: MaintenanceService = context.application.bot_data[CTX_MAINT_SVC]
    c, periods = msvc.list_periods(itemid)
    now = int(time.time())
    active_period = next(((s, e) for (s, e) in periods if s <= now <= e), None)

    if active_period:
      s, e = active_period
      start_ts = s
      end_ts = e + secs
      context.user_data[CTX_MAINT_PENDING_ACTION] = MAINT_PENDING_ACTION_EXTEND
    else:
      start_ts = now
      end_ts = now + secs
      context.user_data[CTX_MAINT_PENDING_ACTION] = MAINT_PENDING_ACTION_ADD_NEW

    context.user_data[CTX_MAINT_PENDING_START] = start_ts
    context.user_data[CTX_MAINT_PENDING_END] = end_ts
    context.user_data[CTX_MAINT_FLOW_KEY] = MAINT_FLOW_AWAIT_CONFIRM
    await _delete_prompt_and_reply(update, context)
    await _send_confirm(update, context, start_ts, end_ts)
    return SELECTING

  if data.startswith(CB_MAINT_NEW):
    logger.debug("maint_action: NEW custom entry")
    context.user_data[CTX_MAINT_FLOW_KEY] = MAINT_FLOW_AWAIT_PERIOD
    try:
      msg_id = context.user_data.get(CTX_MAINT_MSG_ID)
      if msg_id:
        await context.bot.edit_message_text(
          chat_id=update.effective_chat.id,
          message_id=msg_id,
          text=escape_markdown_v2(INPUT_INSTRUCTIONS),
          parse_mode=ParseMode.MARKDOWN_V2,
          reply_markup=maint_custom_kb(),
        )
    except Exception as e:
      logger.warning("edit actions message failed: %s", e)

    await _send_prompt(update.callback_query.message, context)
    context.user_data.pop(CTX_MAINT_REPLY_MSG_ID, None)
    return SELECTING

  if data.startswith(CB_MAINT_CONFIRM):
    logger.debug("maint_action: CONFIRM action=%s", context.user_data.get(CTX_MAINT_PENDING_ACTION))
    itemid = context.user_data.get(CTX_MAINT_ITEM_KEY)
    start_ts = int(context.user_data.get(CTX_MAINT_PENDING_START))
    end_ts = int(context.user_data.get(CTX_MAINT_PENDING_END))
    action = context.user_data.get(CTX_MAINT_PENDING_ACTION)

    if action == MAINT_PENDING_ACTION_EXTEND:
      # compute delta = new_end - old_end; service extend_active handles delta
      now = int(time.time())
      msvc: MaintenanceService = context.application.bot_data[CTX_MAINT_SVC]
      # safest: extend by (end_ts - now) if you're prolonging from now; but we stored final end_ts
      # use delta = end_ts - current_end; recompute current_end from periods
      c, periods = msvc.list_periods(itemid)
      active = next(((s, e) for (s, e) in periods if s <= now <= e), None)
      delta = (end_ts - active[1]) if active else 0
      if delta > 0:
        res = msvc.extend_active(itemid, delta)
        await db.audit_maint(update.effective_user.id, "update", res["maintenanceid"], itemid, res.get("hostid", ""),
                             res["before"], res["after"])
    else:
      res = msvc.add_period(itemid, start_ts, end_ts, mtype=0)
      await db.audit_maint(update.effective_user.id, "create", res["maintenanceid"], itemid, res.get("hostid", ""),
                           res["before"], res["after"])

    clean_flow_and_pending(context)
    return await maint_select_item(update, context)

  if data == CB_MAINT_CANCEL:
    logger.debug("maint_action: CANCEL")
    clean_flow_and_pending(context)
    await _delete_prompt_and_reply(update, context)
    return await maint_select_item(update, context)

  if data == CB_MAINT_BACK_HOST:
    allow_hosts = context.application.bot_data[CTX_ALLOW_HOSTS]
    host_names = list(allow_hosts.values())
    kb = build_hosts_keyboard(host_names, CONV_TYPE_MAINT)
    try:
      await update.callback_query.edit_message_text(HOST_SELECT_TITLE, reply_markup=kb)
    except Exception as e:
      logger.warning("edit back to hosts failed: %s", e)
    return SELECTING

  if data == CB_MAINT_BACK_ITEMS:
    # Re-render items list for current host with active marks
    hostid = context.user_data.get(CTX_HOST_ID)
    host_name = context.user_data.get(CTX_HOST_NAME, "")
    kb = await get_maint_items_keyboard(hostid, context)
    await update.callback_query.edit_message_text(ITEM_SELECT_TITLE.format(host=host_name), reply_markup=kb)
    return SELECTING

  return SELECTING


async def maint_handle_text(update: Update, context: CallbackContext):
  logger.info("maint_text_input flow=%s text=%s msg_id=%s", context.user_data.get(CTX_MAINT_FLOW_KEY),
              (update.message.text or "").strip(), update.message.message_id)
  flow = context.user_data.get(CTX_MAINT_FLOW_KEY)
  if flow != MAINT_FLOW_AWAIT_PERIOD:
    return
  tz = await get_tz(update, context)
  context.user_data[CTX_MAINT_REPLY_MSG_ID] = update.message.message_id
  raw = (update.message.text or "").strip()
  parts = [p.strip() for p in re.split(PERIOD_SPLIT_REGEX, raw) if p.strip()]
  now_ts = int(time.time())
  if len(parts) == 1:
    start_ts = now_ts
    end_dt = parse_date(parts[0], tz)
    if not end_dt:
      await _delete_prompt_and_reply(update, context)
      await _send_prompt(update.message, context, PARSE_FAIL_END)
      context.user_data.pop(CTX_MAINT_REPLY_MSG_ID, None)
      return
    if end_dt.tzinfo is None:
      end_dt = end_dt.replace(tzinfo=tz)
    end_ts = int(end_dt.timestamp())
  else:
    s_dt = parse_date(parts[0], tz)
    e_dt = parse_date(parts[1], tz) if len(parts) >= 2 else None
    if not s_dt or not e_dt:
      await _delete_prompt_and_reply(update, context)
      await _send_prompt(update.message, context, PARSE_FAIL_BOTH)
      context.user_data.pop(CTX_MAINT_REPLY_MSG_ID, None)
      return
    if s_dt.tzinfo is None: s_dt = s_dt.replace(tzinfo=tz)
    if e_dt.tzinfo is None: e_dt = e_dt.replace(tzinfo=tz)
    start_ts = int(s_dt.timestamp())
    end_ts = int(e_dt.timestamp())

  # Validate
  if end_ts <= start_ts or end_ts <= now_ts:
    await _delete_prompt_and_reply(update, context)
    await _send_prompt(update.message, context, INVALID_PERIOD_MSG)
    context.user_data.pop(CTX_MAINT_REPLY_MSG_ID, None)
    return

  # Store pending and ask confirm on the actions message (edit)
  context.user_data[CTX_MAINT_PENDING_START] = start_ts
  context.user_data[CTX_MAINT_PENDING_END] = end_ts
  context.user_data[CTX_MAINT_PENDING_ACTION] = MAINT_PENDING_ACTION_ADD_NEW
  context.user_data[CTX_MAINT_FLOW_KEY] = MAINT_FLOW_AWAIT_CONFIRM

  await _delete_prompt_and_reply(update, context)
  await _send_confirm(update, context, start_ts, end_ts)


async def build_maint_view_for_item(context: CallbackContext, user_id: int, itemid: str) -> tuple[
  str, InlineKeyboardMarkup]:
  db: UserDB = context.application.bot_data[CTX_DB]
  tz_name = await db.get_timezone(user_id)
  tz = ZoneInfo(tz_name)
  msvc: MaintenanceService = context.application.bot_data[CTX_MAINT_SVC]
  c, periods = msvc.list_periods(itemid)
  now = int(time.time())
  is_active = any(s <= now <= e for s, e in periods)
  title = f"{c.get('name', '')}\n{ITEM_STATUS_ACTIVE if is_active else ITEM_STATUS_INACTIVE}\n"
  parts = format_periods(tz, periods, MAINT_LIST_LIMIT)
  text = "\n".join([title] + (parts or [PERIODS_EMPTY]))
  can_edit = await db.is_maintainer(user_id)
  kb = maint_actions_kb(itemid, is_active, with_graph_back=True, can_edit=can_edit)
  return escape_markdown_v2(text), kb


async def open_graph_from_maint(update: Update, context: CallbackContext):
  logger.info("go_graph_from_maint cb_data=%s", update.callback_query.data)
  db: UserDB = context.application.bot_data[CTX_DB]
  if not await is_allowed_user(db, update.effective_user.id):
    return ConversationHandler.END

  await update.callback_query.answer()
  data = update.callback_query.data  # "go_graph:{itemid}"
  if not data.startswith(CB_GO_GRAPH):
    return SELECTING
  itemid = data.split(":", 1)[1]

  items_idx: ItemsIndex = context.application.bot_data[CTX_ITEMS]
  info = items_idx.get_item(itemid)
  if not info:
    return SELECTING

  host_name = items_idx.host_name_by_hostid(info.hostid) or ""
  period = context.user_data.get(CTX_GRAPH_PERIOD) or DEFAULT_GRAPH_ITEM_PERIOD

  gsvc: GraphService = context.application.bot_data[CTX_GRAPH_SVC]
  key_parts, ttl, cache_res = await gsvc.get_item_media_from_item(
    hostid=info.hostid, itemid=info.itemid, name=info.name, color=info.color, units=info.units,
    period_label=period, width=IMG_WIDTH, height=IMG_HEIGHT
  )
  markup = build_time_keyboard_item(info.itemid, host_name)
  new_msg_id, new_file_id = await edit_or_send_graph(
    context.bot,
    chat_id=update.effective_chat.id,
    message_id=None,
    file_id=cache_res.file_id,
    image_bytes=cache_res.data,
    reply_markup=markup,
  )
  await clean_all_messages(update, context)
  context.user_data[CTX_GRAPH_MSG_ID] = new_msg_id
  if new_file_id and not cache_res.file_id:
    cache2: ImageCache2 = context.application.bot_data[CTX_CACHE2]
    await cache2.remember_file_id(key_parts, new_file_id, ttl)
  return SELECTING
