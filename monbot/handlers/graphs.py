import asyncio
import logging

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import CallbackContext, ConversationHandler

from monbot.cache2 import ImageCache2
from monbot.config import IMG_HEIGHT, IMG_WIDTH
from monbot.db import UserDB
from monbot.graph_service import GraphService
from monbot.handlers.common import get_cb_data_val, is_allowed_user
from monbot.handlers.consts import *
from monbot.handlers.keyboards import build_graphs_keyboard, build_hosts_keyboard, build_time_keyboard_item
from monbot.handlers.maintenance import MAINT_ITEM_KEY, build_maint_view_for_item
from monbot.handlers.texts import *
from monbot.items_index import ItemsIndex
from monbot.tg_media import edit_or_send_graph
from monbot.utils import safe_delete_query_message

logger = logging.getLogger(__name__)


async def host_handler(update: Update, context: CallbackContext):
  logger.info("host_handler cb_data=%s", update.callback_query.data)
  await update.callback_query.answer()
  db: UserDB = context.application.bot_data[CTX_DB]
  if not await is_allowed_user(db, update.effective_user.id):
    return ConversationHandler.END

  data = update.callback_query.data
  if not data.startswith(f"{CB_GRAPH_HOST}:"):
    return SELECTING
  host_name = get_cb_data_val(data)
  allow_hosts = context.application.bot_data[CTX_ALLOW_HOSTS]
  if host_name not in allow_hosts.values():
    return SELECTING

  items_idx: ItemsIndex = context.application.bot_data[CTX_ITEMS]
  hostid = items_idx.hostid_by_name(host_name)
  if not hostid:
    return SELECTING

  items = items_idx.items_for_hostid(hostid)
  if not items:
    await update.callback_query.edit_message_text(NO_ITEMS_FOR_HOST)
    return SELECTING

  triplets = [(it.itemid, it.name, it.color) for it in items]
  names = [(it.itemid, it.name) for it in items]
  gsvc: GraphService = context.application.bot_data[CTX_GSVC]
  key_parts, ttl, cres = await gsvc.get_overview_media_from_items(hostid, triplets, GRAPH_OVERVIEW_PERIOD, IMG_WIDTH,
                                                                  IMG_HEIGHT)
  markup = build_graphs_keyboard(names, host_name)
  chat_id = update.effective_chat.id
  await safe_delete_query_message(update, context)
  msg_id = context.user_data.get(CTX_GRAPH_MSG_ID)
  new_msg_id, new_file_id = await edit_or_send_graph(
    context.bot, chat_id, msg_id, cres.file_id, cres.data, markup
  )
  context.user_data[CTX_GRAPH_MSG_ID] = new_msg_id
  context.user_data[CTX_GRAPH_GRAPHID] = None
  context.user_data[CTX_GRAPH_HOST] = host_name
  context.user_data[CTX_GRAPH_PERIOD] = GRAPH_OVERVIEW_PERIOD

  if new_file_id and not cres.file_id:
    cache2: ImageCache2 = context.application.bot_data[CTX_CACHE2]
    await cache2.remember_file_id(key_parts, new_file_id, ttl)

  return SELECTING


async def item_handler(update: Update, context: CallbackContext):
  logger.info("item_handler cb_data=%s", update.callback_query.data)
  db: UserDB = context.application.bot_data[CTX_DB]
  if not await is_allowed_user(db, update.effective_user.id):
    return ConversationHandler.END

  q = update.callback_query
  await q.answer()
  data = q.data
  m = PAT_GRAPH_ITEM_MATCHER.match(data or "")
  if not m:
    return SELECTING
  itemid, period = m.group(1), (m.group(2) or DEFAULT_GRAPH_ITEM_PERIOD)

  items_idx: ItemsIndex = context.application.bot_data[CTX_ITEMS]
  info = items_idx.item_info_by_id(itemid)
  if not info:
    return SELECTING
  host_name = items_idx.host_name_by_hostid(info.hostid) or ""

  gsvc: GraphService = context.application.bot_data[CTX_GSVC]
  key_parts, ttl, cres = await gsvc.get_item_media_from_item(
    hostid=info.hostid, itemid=info.itemid, name=info.name, color=info.color, units=info.units,
    period_label=period, width=IMG_WIDTH, height=IMG_HEIGHT
  )

  chat_id = update.effective_chat.id
  msg_id = context.user_data.get(CTX_GRAPH_MSG_ID)
  markup = build_time_keyboard_item(info.itemid, info.name, host_name)

  new_msg_id, new_file_id = await edit_or_send_graph(
    context.bot,
    chat_id=chat_id,
    message_id=msg_id,
    file_id=cres.file_id,
    image_bytes=cres.data,
    reply_markup=markup,
  )
  context.user_data[CTX_GRAPH_MSG_ID] = new_msg_id
  context.user_data[CTX_GRAPH_GRAPHID] = None
  context.user_data[CTX_GRAPH_HOST] = host_name
  context.user_data[CTX_GRAPH_PERIOD] = period
  context.user_data[CTX_GRAPH_ITEMID] = info.itemid
  context.user_data[CTX_GRAPH_ITEM_NAME] = info.name

  if new_file_id and not cres.file_id:
    cache2: ImageCache2 = context.application.bot_data[CTX_CACHE2]
    await cache2.remember_file_id(key_parts, new_file_id, ttl)

  # prefetch neighbors
  try:
    i = TIME_RANGES.index(period)
  except ValueError:
    i = -1
  if i != -1:
    if i - 1 >= 0:
      asyncio.create_task(
        gsvc.get_item_media_from_item(info.hostid, info.itemid, info.name, info.color, info.units, TIME_RANGES[i - 1],
                                      IMG_WIDTH, IMG_HEIGHT))
    if i + 1 < len(TIME_RANGES):
      asyncio.create_task(
        gsvc.get_item_media_from_item(info.hostid, info.itemid, info.name, info.color, info.units, TIME_RANGES[i + 1],
                                      IMG_WIDTH, IMG_HEIGHT))

  return SELECTING


async def start_over(update: Update, context: CallbackContext):
  db: UserDB = context.application.bot_data[CTX_DB]
  if not await is_allowed_user(db, update.effective_user.id):
    return ConversationHandler.END

  query = update.callback_query
  await query.answer()

  context.user_data.pop(CTX_GRAPH_MSG_ID, None)
  context.user_data.pop(CTX_GRAPH_GRAPHID, None)
  context.user_data.pop(CTX_GRAPH_HOST, None)
  context.user_data.pop(CTX_GRAPH_PERIOD, None)

  await safe_delete_query_message(update, context)

  allow_hosts = context.application.bot_data[CTX_ALLOW_HOSTS]
  host_names = list(allow_hosts.values())
  markup = build_hosts_keyboard(host_names, CONV_TYPE_GRAPH)
  await context.bot.send_message(chat_id=update.effective_chat.id, text=DEVICE_SELECT_TITLE, reply_markup=markup)
  return SELECTING


async def go_maint_from_graph(update: Update, context: CallbackContext):
  logger.info("go_maint_from_graph cb_data=%s", update.callback_query.data)
  db: UserDB = context.application.bot_data[CTX_DB]
  if not await is_allowed_user(db, update.effective_user.id):
    return ConversationHandler.END

  q = update.callback_query
  await q.answer()
  m = PAT_GO_MAINT_MATCHER.match(q.data or "")
  if not m:
    return SELECTING
  itemid = m.group(1)

  # Resolve and store maintenance context for subsequent maint_* actions
  items_idx: ItemsIndex = context.application.bot_data[CTX_ITEMS]
  info = items_idx.item_info_by_id(itemid)
  if info:
    context.user_data[MAINT_ITEM_KEY] = info.itemid
    context.user_data[MAINT_HOST_KEY] = info.hostid
    context.user_data[MAINT_HOST_NAME_KEY] = items_idx.host_name_by_hostid(info.hostid) or ""

  text, kb = await build_maint_view_for_item(context, update.effective_user.id, itemid)

  # Delete current graph photo message, then send maintenance text
  chat_id = update.effective_chat.id
  graph_mid = context.user_data.get(CTX_GRAPH_MSG_ID)
  if graph_mid:
    try:
      await context.bot.delete_message(chat_id=chat_id, message_id=graph_mid)
    except Exception:
      pass
    # clear old anchor so next graph send is clean
    context.user_data.pop(CTX_GRAPH_MSG_ID, None)

  msg = await context.bot.send_message(
    chat_id=chat_id,
    text=text,
    reply_markup=kb,
    parse_mode=ParseMode.MARKDOWN_V2,
  )
  # track maintenance message id
  context.user_data[MAINT_MSG_ID] = msg.message_id
  return SELECTING


async def go_graph_from_maint(update: Update, context: CallbackContext):
  logger.info("go_graph_from_maint cb_data=%s", update.callback_query.data)
  db: UserDB = context.application.bot_data[CTX_DB]
  if not await is_allowed_user(db, update.effective_user.id):
    return ConversationHandler.END

  q = update.callback_query
  await q.answer()
  data = q.data  # "go_graph:{itemid}"
  if not data.startswith(CB_GO_GRAPH):
    return SELECTING
  itemid = data.split(":", 1)[1]

  items_idx: ItemsIndex = context.application.bot_data[CTX_ITEMS]
  info = items_idx.item_info_by_id(itemid)
  if not info:
    return SELECTING

  host_name = items_idx.host_name_by_hostid(info.hostid) or ""
  period = context.user_data.get(CTX_GRAPH_PERIOD) or DEFAULT_GRAPH_ITEM_PERIOD

  gsvc: GraphService = context.application.bot_data[CTX_GSVC]
  key_parts, ttl, cres = await gsvc.get_item_media_from_item(
    hostid=info.hostid, itemid=info.itemid, name=info.name, color=info.color, units=info.units,
    period_label=period, width=IMG_WIDTH, height=IMG_HEIGHT
  )

  markup = build_time_keyboard_item(info.itemid, info.name, host_name)
  chat_id = update.effective_chat.id
  # Try to edit previous graph photo (if any), else send new
  msg_id = context.user_data.get(CTX_GRAPH_MSG_ID)

  new_msg_id, new_file_id = await edit_or_send_graph(
    context.bot,
    chat_id=chat_id,
    message_id=msg_id,  # may be None or stale -> tg_media will fallback to send
    file_id=cres.file_id,
    image_bytes=cres.data,
    reply_markup=markup,
  )
  context.user_data[CTX_GRAPH_MSG_ID] = new_msg_id

  # Delete the maintenance text message if present and different
  if q.message and q.message.message_id != new_msg_id:
    try:
      await context.bot.delete_message(chat_id=chat_id, message_id=q.message.message_id)
    except Exception:
      pass

  if new_file_id and not cres.file_id:
    cache2: ImageCache2 = context.application.bot_data[CTX_CACHE2]
    await cache2.remember_file_id(key_parts, new_file_id, ttl)
  return SELECTING
