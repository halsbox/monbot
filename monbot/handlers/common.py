import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

import dateparser
from telegram import InlineKeyboardButton, Update
from telegram.error import BadRequest
from telegram.ext import CallbackContext

from monbot.db import UserDB
from monbot.handlers.consts import *
from monbot.handlers.texts import *

logger = logging.getLogger(__name__)


async def is_allowed_user(db: UserDB, user_id: int) -> bool:
  # viewer or above
  return await db.role_at_least(user_id, ROLE_VIEWER)


async def is_admin(db: UserDB, user_id: int) -> bool:
  return await db.is_admin(user_id)


async def is_maint_manager(db: UserDB, user_id: int) -> bool:
  # maintainer or admin
  return await db.is_maintainer(user_id)


def _chunk(buttons: List[InlineKeyboardButton], n: int) -> List[List[InlineKeyboardButton]]:
  return [buttons[i:i + n] for i in range(0, len(buttons), n)]


async def error_handler(update: object, context: CallbackContext):
  exc = context.error
  logger.exception("Unhandled error: %s", exc)
  if isinstance(exc, BadRequest) and "Image_process_failed" in str(exc):
    logger.error("Telegram failed to process image. Likely non-image bytes from Zabbix (auth/HTML).")


def escape_markdown_v2(text: str) -> str:
  """
  Smartly escape MarkdownV2 for Telegram bots while preserving valid formatting.
  """
  # MarkdownV2 special chars that must be escaped when not part of formatting
  specials = r'_*\[\]()~`>#+-=|{}.!'

  # Regex patterns for valid Telegram MarkdownV2 formatting pairs
  formatting_patterns = [
    r'\*(.*?)\*',  # *bold*
    r'_(.*?)_',  # _italic_
    r'~(.*?)~',  # ~strikethrough~
    r'__(.*?)__',  # __underline__
    r'\|\|(.*?)\|\|',  # ||spoiler||
    r'`([^`]+)`',  # `code`
    r'```([\s\S]*?)```',  # ```preformatted```
    r'\[([^\]]+)\]\([^)]+\)',  # [text](url)
  ]

  # Combine patterns into a single regex with named groups
  combined = '|'.join(f'({p})' for p in formatting_patterns)

  escaped = []
  last = 0

  for m in re.finditer(combined, text):
    start, end = m.span()
    # escape everything before the formatting entity
    pre = text[last:start]
    pre_escaped = re.sub(
      rf'([{re.escape(specials)}])',
      r'\\\1',
      pre
    )
    escaped.append(pre_escaped)
    # keep the formatting entity unchanged
    escaped.append(m.group(0))
    last = end

  # escape the remainder
  rest = text[last:]
  rest_escaped = re.sub(
    rf'([{re.escape(specials)}])',
    r'\\\1',
    rest
  )
  escaped.append(rest_escaped)
  return ''.join(escaped)


def format_duration(seconds: int) -> str:
  days = seconds // 86400
  hours = (seconds % 86400) // 3600
  minutes = (seconds % 3600) // 60
  return f"{days}д{hours}ч{minutes}м"


def parse_date(text: str, tz: str | ZoneInfo) -> datetime | None:
  return dateparser.parse(
    text,
    languages=['ru', 'en'],
    settings={
      'DATE_ORDER': 'DMY',
      'PREFER_DATES_FROM': 'future',
      'TIMEZONE': str(tz),
    }
  )


def divide_and_prepare_periods(periods: List[Tuple[int, int]], limit: int) -> List[Dict[str, Any]]:
  cutoff = int(time.time()) - 10 * 365 * 24 * 3600
  now = int(time.time())
  filtered = [(s, e) for index, (s, e) in enumerate(periods) if s >= cutoff and index < limit]
  future: List[Tuple[int, int]] = []
  active: List[Tuple[int, int]] = []
  finished: List[Tuple[int, int]] = []

  for s, e in filtered:
    if s > now:
      future.append((s, e))
    elif e < now:
      finished.append((s, e))
    else:
      active.append((s, e))
  result = []
  if future:
    result.append(
      {
        'caption': PERIODS_FUTURE_CAPTION,
        'periods': future,
        'bullet': INACTIVE_BULLET,
      }
    )
  if active:
    result.append(
      {
        'caption': PERIODS_ACTIVE_CAPTION,
        'periods': active,
        'bullet': ACTIVE_BULLET,
      }
    )
  if finished:
    result.append(
      {
        'caption': PERIODS_FINISHED_CAPTION,
        'periods': finished,
        'bullet': INACTIVE_BULLET,
      }
    )
  return result


def format_periods(tz: ZoneInfo, periods: List[Tuple[int, int]], limit: int) -> List[str]:
  groups = divide_and_prepare_periods(periods, limit)
  lines: List[str] = []
  for group in groups:
    lines.append(group['caption'])
    for s, e in group['periods']:
      s_str = datetime.fromtimestamp(s, tz).strftime(DT_FMT)
      e_str = datetime.fromtimestamp(e, tz).strftime(DT_FMT)
      dur = format_duration(e - s)
      lines.append(PERIOD_LINE_FMT.format(start=s_str, end=e_str, bullet=group['bullet'], duration=dur))
  return lines


async def get_tz(update: Update, context: CallbackContext) -> ZoneInfo:
  tz_name = await context.application.bot_data[CTX_DB].get_timezone(update.effective_user.id)
  try:
    tz = ZoneInfo(tz_name)
  except Exception:
    tz = ZoneInfo("Europe/Moscow")
  return tz


def get_cb_data_val(data: str) -> str:
  return data.split(":", 1)[1]


def clean_flow_and_pending(context: CallbackContext) -> None:
  context.user_data.pop(CTX_MAINT_FLOW_KEY, None)
  context.user_data.pop(CTX_MAINT_PENDING_START, None)
  context.user_data.pop(CTX_MAINT_PENDING_END, None)
  context.user_data.pop(CTX_MAINT_PENDING_ACTION, None)


async def remove_msg_by_ctx_id(context: CallbackContext, chat_id: str | int, ctx_id: str) -> None:
  msg_id = context.user_data.get(ctx_id)
  if msg_id:
    try:
      await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except Exception:
      pass
    context.user_data.pop(ctx_id, None)


async def clean_all_messages(update: Update, context: CallbackContext) -> None:
  chat_id = update.effective_chat.id
  for ctx_id in (CTX_MAINT_MSG_ID, CTX_GRAPH_MSG_ID):
    msg_id = context.user_data.get(ctx_id)
    if msg_id:
      try:
        await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
      except Exception:
        pass
      context.user_data.pop(ctx_id, None)
  cb_msg_id = getattr(getattr(getattr(update, "callback_query", None), "message", None), "message_id", None)
  if cb_msg_id:
    try:
      await context.bot.delete_message(chat_id=chat_id, message_id=cb_msg_id)
    except Exception:
      pass


async def check_user(update: Update, context: CallbackContext) -> bool:
  return await is_allowed_user(context.application.bot_data[CTX_DB], update.effective_user.id)


async def get_host_data(update: Update, context: CallbackContext, conv_type: str) -> Tuple[
  str | None, int | str | None]:
  if conv_type == CONV_TYPE_MAINT:
    host_key = CB_MAINT_HOST
  else:
    host_key = CB_GRAPH_HOST
  if not update.callback_query.data.startswith(f"{host_key}:"):
    return None, None
  host_name = get_cb_data_val(update.callback_query.data)
  allow_hosts = context.application.bot_data[CTX_ALLOW_HOSTS]
  if host_name not in allow_hosts.values():
    return None, None
  hostid = context.application.bot_data[CTX_ITEMS].hostid_by_name(host_name)
  if not hostid:
    return None, None
  return host_name, hostid
