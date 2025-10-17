from __future__ import annotations

import re
from typing import Dict

from monbot.config import TIME_RANGES

ROLE_ADMIN = "admin"
ROLE_MAINTAINER = "maintainer"
ROLE_VIEWER = "viewer"

VALID_ROLES = (ROLE_ADMIN, ROLE_MAINTAINER, ROLE_VIEWER)

SELECTING = 1

CONV_TYPE_GRAPH = "graph"
CONV_TYPE_MAINT = "maint"

GRAPH_OVERVIEW_PERIOD = "1h"
DEFAULT_GRAPH_ITEM_PERIOD = "12h"

# Callback data prefixes

CB_GO_MAINT = "go_maint"  # go_maint:{itemid}
CB_GO_GRAPH = "go_graph"  # go_graph:{itemid}

CB_GRAPH_HOST = "graph_host"  # graph_host:{host_name}
CB_GRAPH_ITEM = "graph_item"  # graph_item:{itemid}[:period]

CB_MAINT_HOST = "maint_host"  # maint_host:{host_name}
CB_MAINT_ITEM = "maint_item"  # maint_item:{itemid}
CB_MAINT_FAST = "maint_fast"  # maint_fast:{itemid}
CB_MAINT_END = "maint_end"  # maint_end:{itemid}
CB_MAINT_NEW = "maint_new"  # maint_new:{itemid}
CB_MAINT_ADD = "maint_add"  # maint_add:{seconds}
CB_MAINT_CONFIRM = "maint_confirm"  # maint_confirm:{itemid}
CB_MAINT_CANCEL = "maint_cancel"  # maint_cancel
CB_MAINT_RETRY = "maint_retry"  # maint_retry
CB_MAINT_BACK_HOST = "maint_back_host"  # maint_back_host
CB_MAINT_BACK_ITEMS = "maint_back_items"  # maint_back_items

CB_RESTART = "restart"  # back to host list

CB_MAINT_ITEM_KEYS = (
  f"{CB_MAINT_ITEM}:",
  f"{CB_MAINT_CONFIRM}:",
  f"{CB_MAINT_END}:",
  f"{CB_MAINT_FAST}:",
)

CB_MAINT_ACTION_KEYS = (
  f"{CB_MAINT_ADD}:",
  f"{CB_MAINT_FAST}:",
  f"{CB_MAINT_END}:",
  f"{CB_MAINT_NEW}:"
)

# Context keys (graph)
CTX_GRAPH_MSG_ID = "graph_msg_id"
CTX_GRAPH_GRAPHID = "graph_graphid"
CTX_GRAPH_PERIOD = "graph_period"
CTX_GRAPH_ITEMID = "graph_itemid"
CTX_GRAPH_ITEM_NAME = "graph_item_name"

# Context keys (common)
CTX_HOST_ID = "host_id"
CTX_HOST_NAME = "host_name"
CTX_ITEMS = "items"
CTX_ALLOW_HOSTS = "allow_hosts"
CTX_CACHE2 = "cache2"
CTX_MAINT_SVC = "maint_svc"
CTX_GRAPH_SVC = "graph_svc"
CTX_DB = "db"
CTX_ZBX = "zbx"

# Context keys (maintenance flow)
CTX_MAINT_FLOW_KEY = "maint_flow"  # values: None | "await_period" | "await_confirm"
CTX_MAINT_ITEM_KEY = "maint_itemid"
CTX_MAINT_PENDING_ACTION = "maint_pending_action"
CTX_MAINT_PENDING_START = "maint_pending_start"
CTX_MAINT_PENDING_END = "maint_pending_end"
CTX_MAINT_MSG_ID = "maint_msg_id"
CTX_MAINT_FORCE_MSG_ID = "maint_force_msg_id"
CTX_MAINT_REPLY_MSG_ID = "maint_reply_msg_id"

# Context values (maintenance flow)
MAINT_PENDING_ACTION_EXTEND = "extend"
MAINT_PENDING_ACTION_ADD_NEW = "add_new"
MAINT_FLOW_AWAIT_PERIOD = "await_period"
MAINT_FLOW_AWAIT_CONFIRM = "await_confirm"
# Reports callbacks
CB_REPORT_CONFIRM = "report_confirm"      # report_confirm:{period}:{start_ts}:{end_ts}
CB_REPORT_CANCEL = "report_cancel"        # report_cancel
CB_REPORT_SEND = "report_send"  # report_send:{period}:{start_ts}

PAT_REPORT_SEND = r"^report_send:(week|month):\d+$"
PAT_REPORT_CONFIRM = r"^report_confirm:(week|month):\d+:\d+$"
# Build patterns once, using TIME_RANGES from config
_TR_ALTS = "|".join(map(re.escape, TIME_RANGES))
PAT_GRAPH_ITEM = rf"^{CB_GRAPH_ITEM}:\d+(?::(?:{_TR_ALTS}))?$"
PAT_GRAPH_ITEM_MATCHER = re.compile(rf"^{CB_GRAPH_ITEM}:(\d+)(?::({_TR_ALTS}))?$")
PAT_GO_MAINT = rf"^{CB_GO_MAINT}:\d+$"
PAT_GO_MAINT_MATCHER = re.compile(rf"^{CB_GO_MAINT}:(\d+)$")
PAT_GO_GRAPH = rf"^{CB_GO_GRAPH}:\d+$"

PAT_GRAPH_HOST = rf"^{CB_GRAPH_HOST}:"

PAT_MAINT_HOST = rf"^{CB_MAINT_HOST}:"
PAT_MAINT_ITEM_OR_BACK_HOST = rf"^(?:{CB_MAINT_ITEM}:\d+|{CB_MAINT_BACK_HOST})$"
PAT_MAINT_ACTIONS = rf"^(?:{CB_MAINT_FAST}:\d+|{CB_MAINT_END}:\d+|{CB_MAINT_NEW}:\d+|{CB_MAINT_ADD}:\d+|{CB_MAINT_CONFIRM}:\d+|{CB_MAINT_RETRY}|{CB_MAINT_CANCEL}|{CB_MAINT_BACK_HOST}|{CB_MAINT_BACK_ITEMS})$"


# Host name pattern must be built from ALLOW_HOSTS at runtime in bot.py
def host_names_pattern(allow_hosts: Dict[str, str]) -> str:
  # returns pattern string for CallbackQueryHandler(pattern=..)
  return f"^{CB_GRAPH_HOST}:(" + "|".join(re.escape(v) for v in allow_hosts.values()) + ")$"
