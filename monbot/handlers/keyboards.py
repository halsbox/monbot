import asyncio
from typing import Any, List, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext

from monbot.handlers.common import _chunk
from monbot.handlers.consts import *
from monbot.handlers.texts import *
from monbot.items_index import ItemsIndex
from monbot.maintenance_service import MaintenanceService
from monbot.utils import natural_key


def build_hosts_keyboard(host_names: List[str], conv_type: str) -> InlineKeyboardMarkup:
  host_names = sorted(host_names, key=natural_key)
  cb_key = CB_MAINT_HOST if conv_type == CONV_TYPE_MAINT else CB_GRAPH_HOST
  buttons = [InlineKeyboardButton(h, callback_data=f"{cb_key}:{h}") for h in host_names]
  rows = _chunk(buttons, 3)
  return InlineKeyboardMarkup(rows)


def build_graphs_keyboard(items: List[Tuple[str, str]]) -> InlineKeyboardMarkup:
  items = sorted(items, key=lambda t: natural_key(t[1]))
  buttons = [InlineKeyboardButton(name, callback_data=f"{CB_GRAPH_ITEM}:{itemid}") for itemid, name in items]
  buttons.append(InlineKeyboardButton(BTN_DEVICE, callback_data=CB_RESTART))
  rows = _chunk(buttons, 3)
  return InlineKeyboardMarkup(rows)


def build_time_keyboard_item(itemid: str, host_name: str) -> InlineKeyboardMarkup:
  buttons = [InlineKeyboardButton(tr, callback_data=f"{CB_GRAPH_ITEM}:{itemid}:{tr}") for tr in TIME_RANGES]
  buttons.append(InlineKeyboardButton(BTN_MAINT, callback_data=f"{CB_GO_MAINT}:{itemid}"))
  buttons.append(InlineKeyboardButton(host_name, callback_data=f"{CB_GRAPH_HOST}:{host_name}"))
  rows = _chunk(buttons, 3)
  return InlineKeyboardMarkup(rows)


async def get_maint_items_keyboard(hostid: Any, context: CallbackContext) -> InlineKeyboardMarkup:
  msvc: MaintenanceService = context.application.bot_data[CTX_MAINT_SVC]
  items_idx: ItemsIndex = context.application.bot_data[CTX_ITEMS]
  items = items_idx.items_by_hostid(hostid) if hostid else []
  active_set = await asyncio.to_thread(msvc.active_items_for_host, hostid) if hostid else set()
  buttons = []
  for it in items:
    label = f"{ACTIVE_BULLET if it.itemid in active_set else INACTIVE_BULLET} {it.name}"
    buttons.append(InlineKeyboardButton(label, callback_data=f"{CB_MAINT_ITEM}:{it.itemid}"))
  rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
  rows.append([InlineKeyboardButton(BTN_BACK_HOST, callback_data=CB_MAINT_BACK_HOST)])
  return InlineKeyboardMarkup(rows)


def maint_confirm_kb(itemid: str) -> InlineKeyboardMarkup:
  return InlineKeyboardMarkup([
    [InlineKeyboardButton(BTN_CONFIRM, callback_data=f"{CB_MAINT_CONFIRM}:{itemid}")],
    [InlineKeyboardButton(BTN_CANCEL, callback_data=CB_MAINT_CANCEL)],
  ])


def maint_actions_kb(itemid: str, is_active: bool, with_graph_back: bool = True,
                     can_edit: bool = True) -> InlineKeyboardMarkup:
  rows: List[List[InlineKeyboardButton]] = []
  if can_edit:
    if is_active:
      quick = [InlineKeyboardButton(label, callback_data=f"{CB_MAINT_ADD}:{secs}") for label, secs in
               PRESET_ACTIVE_PROLONG]
      rows.extend(_chunk(quick, 3))
      rows.append([
        InlineKeyboardButton(BTN_END_NOW, callback_data=f"{CB_MAINT_END}:{itemid}"),
        InlineKeyboardButton(BTN_NEW_PERIOD, callback_data=f"{CB_MAINT_NEW}:{itemid}"),
      ])
    else:
      rows.append([
        InlineKeyboardButton(BTN_ADD_DAY_NOW, callback_data=f"{CB_MAINT_FAST}:{itemid}"),
        InlineKeyboardButton(BTN_NEW_PERIOD, callback_data=f"{CB_MAINT_NEW}:{itemid}"),
      ])
  if with_graph_back:
    rows.append([InlineKeyboardButton(BTN_GRAPH, callback_data=f"{CB_GO_GRAPH}:{itemid}")])
  rows.append([InlineKeyboardButton(BTN_BACK_ITEMS, callback_data=CB_MAINT_BACK_ITEMS)])
  return InlineKeyboardMarkup(rows)


def maint_custom_kb() -> InlineKeyboardMarkup:
  buttons = [InlineKeyboardButton(label, callback_data=f"{CB_MAINT_ADD}:{secs}") for label, secs in PRESET_CUSTOM_QUICK]
  rows = _chunk(buttons, 3)
  rows.append([InlineKeyboardButton(BTN_CANCEL, callback_data=CB_MAINT_CANCEL)])
  return InlineKeyboardMarkup(rows)

def build_report_confirm_kb(period_type: str, start_ts: int, end_ts: int) -> InlineKeyboardMarkup:
  data = f"{CB_REPORT_CONFIRM}:{period_type}:{start_ts}:{end_ts}"
  rows = [
    [InlineKeyboardButton(BTN_REPORT_CONFIRM, callback_data=data)],
    [InlineKeyboardButton(BTN_REPORT_CANCEL, callback_data=CB_REPORT_CANCEL)],
  ]
  return InlineKeyboardMarkup(rows)
