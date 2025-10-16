import asyncio
import logging

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import CallbackContext, ConversationHandler

from monbot.cache2 import ImageCache2
from monbot.config import IMG_HEIGHT, IMG_WIDTH
from monbot.db import UserDB
from monbot.graph_service import GraphService
from monbot.handlers.common import check_user, clean_all_messages, get_host_data, is_allowed_user
from monbot.handlers.consts import *
from monbot.handlers.keyboards import build_graphs_keyboard, build_hosts_keyboard, build_time_keyboard_item
from monbot.handlers.maintenance import CTX_MAINT_ITEM_KEY, build_maint_view_for_item
from monbot.handlers.texts import *
from monbot.items_index import ItemsIndex
from monbot.tg_media import edit_or_send_graph

logger = logging.getLogger(__name__)


async def host_handler(update: Update, context: CallbackContext):
  logger.info("host_handler cb_data=%s", update.callback_query.data)
  await update.callback_query.answer()
  if not await check_user(update, context):
    return ConversationHandler.END
  host_name, hostid = await get_host_data(update, context, CONV_TYPE_GRAPH)
  if not host_name or not hostid:
    return SELECTING

  items = context.application.bot_data[CTX_ITEMS].items_by_hostid(hostid)
  names = [(it.itemid, it.name) for it in items]
  graph_svc: GraphService = context.application.bot_data[CTX_GRAPH_SVC]
  key_parts, ttl, cache_res = await graph_svc.get_overview_media_from_items(
    hostid, items, GRAPH_OVERVIEW_PERIOD, IMG_WIDTH, IMG_HEIGHT
  )
  markup = build_graphs_keyboard(names)
  chat_id = update.effective_chat.id
  msg_id, file_id = await edit_or_send_graph(
    context.bot,
    chat_id,
    None,
    cache_res.file_id,
    cache_res.data,
    markup
  )
  await clean_all_messages(update, context)
  context.user_data[CTX_HOST_ID] = hostid
  context.user_data[CTX_HOST_NAME] = host_name
  context.user_data[CTX_GRAPH_MSG_ID] = msg_id
  context.user_data[CTX_GRAPH_PERIOD] = GRAPH_OVERVIEW_PERIOD
  if file_id and not cache_res.file_id:
    cache2: ImageCache2 = context.application.bot_data[CTX_CACHE2]
    await cache2.remember_file_id(key_parts, file_id, ttl)

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
  info = items_idx.get_item(itemid)
  if not info:
    return SELECTING
  host_name = items_idx.host_name_by_hostid(info.hostid) or ""

  gsvc: GraphService = context.application.bot_data[CTX_GRAPH_SVC]
  key_parts, ttl, cache_res = await gsvc.get_item_media_from_item(
    hostid=info.hostid, itemid=info.itemid, name=info.name, color=info.color, units=info.units,
    period_label=period, width=IMG_WIDTH, height=IMG_HEIGHT
  )

  chat_id = update.effective_chat.id
  msg_id = context.user_data.get(CTX_GRAPH_MSG_ID)
  markup = build_time_keyboard_item(info.itemid, host_name)

  new_msg_id, new_file_id = await edit_or_send_graph(
    context.bot,
    chat_id=chat_id,
    message_id=msg_id,
    file_id=cache_res.file_id,
    image_bytes=cache_res.data,
    reply_markup=markup,
  )
  context.user_data[CTX_HOST_ID] = info.hostid
  context.user_data[CTX_HOST_NAME] = host_name
  context.user_data[CTX_GRAPH_MSG_ID] = new_msg_id
  context.user_data[CTX_GRAPH_PERIOD] = period
  context.user_data[CTX_GRAPH_ITEMID] = info.itemid
  context.user_data[CTX_GRAPH_ITEM_NAME] = info.name

  if new_file_id and not cache_res.file_id:
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
  await update.callback_query.answer()

  context.user_data.pop(CTX_HOST_ID, None)
  context.user_data.pop(CTX_HOST_NAME, None)
  context.user_data.pop(CTX_GRAPH_PERIOD, None)
  allow_hosts = context.application.bot_data[CTX_ALLOW_HOSTS]
  host_names = list(allow_hosts.values())
  markup = build_hosts_keyboard(host_names, CONV_TYPE_GRAPH)
  await context.bot.send_message(chat_id=update.effective_chat.id, text=DEVICE_SELECT_TITLE, reply_markup=markup)
  await clean_all_messages(update, context)
  return SELECTING


async def open_maint_from_graph(update: Update, context: CallbackContext):
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
  info = items_idx.get_item(itemid)
  if info:
    context.user_data[CTX_MAINT_ITEM_KEY] = info.itemid
    context.user_data[CTX_HOST_ID] = info.hostid
    context.user_data[CTX_HOST_NAME] = items_idx.host_name_by_hostid(info.hostid) or ""
  text, kb = await build_maint_view_for_item(context, update.effective_user.id, itemid)
  msg = await context.bot.send_message(
    chat_id=update.effective_chat.id,
    text=text,
    reply_markup=kb,
    parse_mode=ParseMode.MARKDOWN_V2,
  )
  await clean_all_messages(update, context)
  context.user_data[CTX_MAINT_MSG_ID] = msg.message_id
  return SELECTING
