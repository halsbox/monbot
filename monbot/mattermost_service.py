from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from monbot.cache2 import ImageCache2
from monbot.config import (
  ALLOW_HOSTS,
  AUDIT_LIST_LIMIT,
  DEFAULT_TZ,
  IMG_HEIGHT,
  IMG_WIDTH,
  ITEMS_REFRESH_SEC,
  MAINT_LIST_LIMIT,
  MAINT_TAG_KEY,
  MM_BOT_TOKEN,
  MM_COMMAND_TOKEN,
  MM_CACHE_DIR,
  MM_DB_PATH,
  MM_INITIAL_ADMINS,
  MM_PUBLIC_URL,
  MM_TEAM,
  MM_WEBHOOK_SECRET,
  MM_URL,
  REPORT_DASHBOARD_ID,
  REPORT_PREGEN_MONTHS,
  REPORT_PREGEN_WEEKS,
  REPORT_STORAGE_DIR,
  ZABBIX_API_TOKEN,
  ZABBIX_PASS,
  ZABBIX_URL,
  ZABBIX_USER,
  ZABBIX_VERIFY_SSL,
)
from monbot.handlers.common import format_duration, format_periods, parse_date
from monbot.handlers.consts import *
from monbot.handlers.texts import *
from monbot.items_index import ItemsIndex
from monbot.logging_conf import setup_logging
from monbot.mattermost_api import MattermostAPI
from monbot.mattermost_db import MattermostDB
from monbot.maintenance_service import MaintenanceService
from monbot.graph_service import GraphService
from monbot.report_service import ReportPeriod, ReportService
from monbot.render import SkiaRenderer
from monbot.utils import natural_key
from monbot.zabbix import ZabbixWeb
from monbot.zbx_data import ZbxDataClient

logger = logging.getLogger(__name__)

ACTION_COMMAND = "command"
ACTION_INTEGRATION = "integration"


def _mm_user_id(payload: dict[str, Any]) -> str:
  return str(payload.get("user_id") or payload.get("userId") or "")


def _mm_username(payload: dict[str, Any]) -> str:
  return str(payload.get("user_name") or payload.get("username") or payload.get("user_username") or "")


def _split_period_text(raw: str) -> list[str]:
  parts = [p.strip() for p in re.split(r"(?:\n|;|\s+-\s+)", raw or "") if p.strip()]
  return parts


class MattermostIntegration:
  def __init__(self):
    if not MM_URL:
      raise RuntimeError("MM_URL not set")
    if not MM_TEAM:
      logger.warning("MM_TEAM is not set; slash command registration/setup may be manual.")
    if not MM_BOT_TOKEN:
      raise RuntimeError("MM_BOT_TOKEN not set")
    if not MM_PUBLIC_URL:
      raise RuntimeError("MM_PUBLIC_URL/MM_CALLBACK_BASE_URL not set")

    self.secret = MM_WEBHOOK_SECRET
    self.public_url = MM_PUBLIC_URL.rstrip("/")
    self.command_url = self._build_callback_url("command")
    self.action_url = self._build_callback_url("action")
    self.dialog_url = self._build_callback_url("dialog")
    self.command_token = MM_COMMAND_TOKEN
    if not self.command_token:
      logger.warning("MM_COMMAND_TOKEN is not set; slash command requests will not be authenticated")
    if not self.secret:
      logger.warning("MM_WEBHOOK_SECRET is not set; action/dialog requests will not be protected by a shared secret")

    self.api = MattermostAPI(MM_URL, MM_BOT_TOKEN)
    self.db = MattermostDB(MM_DB_PATH)
    self.zbx = ZabbixWeb(
      server=ZABBIX_URL,
      username=ZABBIX_USER,
      password=ZABBIX_PASS,
      api_token=ZABBIX_API_TOKEN,
      verify=ZABBIX_VERIFY_SSL,
    )
    self.mm_cache = ImageCache2(MM_CACHE_DIR)
    self.renderer = SkiaRenderer()
    self.zbx_client = ZbxDataClient(self.zbx)
    self.graph_svc = GraphService(self.zbx_client, self.mm_cache, self.renderer)
    self.maint_svc = MaintenanceService(self.zbx, tag_key=MAINT_TAG_KEY)
    self.items = ItemsIndex(self.zbx, ALLOW_HOSTS)

  def _build_callback_url(self, kind: str) -> str:
    base = self.public_url.rstrip("/")
    if self.secret:
      return f"{base}/mm/{self.secret}/{kind}"
    return f"{base}/mm/{kind}"

  def asset_url(self, token: str, ext: str = "png") -> str:
    return f"{self.public_url}/mm/assets/{token}.{ext}"

  @staticmethod
  def _resp(data: dict[str, Any]) -> dict[str, Any]:
    return data

  def _attachment(self, *, text: str = "", fallback: str = "", pretext: str = "", title: str = "",
                  title_link: str = "", image_url: str = "", color: str = "#1f6feb",
                  actions: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    att: dict[str, Any] = {
      "fallback": fallback or text or title or "monbot",
      "color": color,
    }
    if pretext:
      att["pretext"] = pretext
    if text:
      att["text"] = text
    if title:
      att["title"] = title
    if title_link:
      att["title_link"] = title_link
    if image_url:
      att["image_url"] = image_url
    if actions:
      att["actions"] = actions
    return att

  def _button(self, label: str, action: str, context: dict[str, Any], *, tooltip: str = "",
              style: str = "default") -> dict[str, Any]:
    ctx = dict(context)
    if self.secret:
      ctx.setdefault("auth", self.secret)
    return {
      "id": re.sub(r"[^a-zA-Z0-9]", "", f"{action}_{label}_{context.get('itemid','')}_{context.get('hostid','')}" )[:190] or action,
      "name": label,
      "tooltip": tooltip or label,
      "style": style,
      "integration": {
        "url": self.action_url,
        "context": {"action": action, **ctx},
      },
    }

  async def startup(self) -> None:
    setup_logging()
    await self.db.init()
    if MM_INITIAL_ADMINS:
      await self.db.ensure_admins(MM_INITIAL_ADMINS)
    await asyncio.to_thread(self.zbx.login)
    await self.items.refresh()
    await self._ensure_maintenance_containers()

  async def _ensure_maintenance_containers(self) -> None:
    for hid in ALLOW_HOSTS.keys():
      for it in self.items.items_by_hostid(hid) or []:
        try:
          await asyncio.to_thread(self.maint_svc.ensure_container, it.itemid)
        except Exception:
          logger.exception("Failed to ensure maintenance container for %s", it.itemid)

  async def _ensure_user_record(self, payload: dict[str, Any], *, allow_autocreate: bool = True) -> Optional[str]:
    user_id = _mm_user_id(payload)
    if not user_id:
      return None
    row = await self.db.get_user(user_id)
    if row:
      await self.db.upsert_user_info_throttled({
        "id": user_id,
        "username": _mm_username(payload) or row[2],
        "first_name": payload.get("first_name") or row[3],
        "last_name": payload.get("last_name") or row[4],
      })
      return row[1]
    if not allow_autocreate:
      return None
    role = ROLE_ADMIN if user_id in MM_INITIAL_ADMINS or _mm_username(payload) in MM_INITIAL_ADMINS else ROLE_VIEWER
    await self.db.ensure_user(
      user_id,
      role=role,
      username=_mm_username(payload) or None,
      first_name=payload.get("first_name"),
      last_name=payload.get("last_name"),
    )
    return role

  async def _user_tz(self, user_id: str) -> ZoneInfo:
    tz_name = await self.db.get_timezone(user_id)
    try:
      return ZoneInfo(tz_name)
    except Exception:
      return ZoneInfo(DEFAULT_TZ)

  def _is_dm(self, payload: dict[str, Any]) -> bool:
    ctype = str(payload.get("channel_type") or payload.get("channel_type") or payload.get("channel_type") or "")
    if ctype:
      return ctype in ("D", "direct", "direct_message")
    channel_name = str(payload.get("channel_name") or "")
    return channel_name.startswith("__") or channel_name.startswith("dm")

  def _dm_only_error(self) -> dict[str, Any]:
    return {"response_type": "ephemeral", "text": "Use this bot in a direct message only."}

  def _verify_command_token(self, payload: dict[str, Any], headers: dict[str, Any] | None = None) -> bool:
    if not self.command_token:
      return True
    token = str(payload.get("token") or "")
    if token and token == self.command_token:
      return True
    auth = ""
    if headers:
      auth = str(headers.get("Authorization") or headers.get("authorization") or "")
    if auth.lower().startswith("bearer "):
      auth = auth.split(" ", 1)[1].strip()
    elif auth.lower().startswith("token "):
      auth = auth.split(" ", 1)[1].strip()
    return bool(auth and auth == self.command_token)

  async def handle_command(self, payload: dict[str, Any], headers: dict[str, Any] | None = None) -> dict[str, Any]:
    if not self._is_dm(payload):
      return self._dm_only_error()
    if not self._verify_command_token(payload, headers):
      return {"response_type": "ephemeral", "text": "Unauthorized request."}

    command = str(payload.get("command") or "").lstrip("/")
    text = str(payload.get("text") or "").strip()
    args = [x for x in text.split() if x]
    user_id = _mm_user_id(payload)
    if not user_id:
      return {"response_type": "ephemeral", "text": "Missing user id."}

    if command == "monbot":
      subcommand = args[0].lower() if args else "help"
      subargs = args[1:] if args else []
    else:
      subcommand = command.lower()
      subargs = args

    if subcommand != "start":
      await self._ensure_user_record(payload, allow_autocreate=True)

    if subcommand == "start":
      return await self._cmd_start(payload, subargs)
    if subcommand == "help":
      return await self._cmd_help(payload)
    if subcommand == "graphs":
      return await self._cmd_graphs(payload)
    if subcommand == "maint":
      return await self._cmd_maint(payload)
    if subcommand == "settz":
      return await self._cmd_settz(payload, subargs)
    if subcommand == "refresh":
      return await self._cmd_refresh(payload)
    if subcommand == "report":
      return await self._cmd_report(payload, subargs)
    if subcommand == "audit":
      return await self._cmd_audit(payload, subargs)
    if subcommand in ("invite", "invgen"):
      return await self._cmd_invgen(payload, subargs)
    if subcommand == "adduser":
      return await self._cmd_adduser(payload, subargs)
    if subcommand == "setrole":
      return await self._cmd_setrole(payload, subargs)
    if subcommand == "listusers":
      return await self._cmd_listusers(payload)
    if subcommand == "deluser":
      return await self._cmd_deluser(payload, subargs)
    return {"response_type": "ephemeral", "text": f"Unknown command: /{command} {text}".strip()}

  async def _cmd_start(self, payload: dict[str, Any], args: list[str]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    row = await self.db.get_user(user_id)
    if row:
      await self.db.upsert_user_info_throttled({
        "id": user_id,
        "username": _mm_username(payload) or row[2],
        "first_name": payload.get("first_name") or row[3],
        "last_name": payload.get("last_name") or row[4],
      })
      return {"response_type": "ephemeral", "text": START_EXISTING_USER.format(name=f"@{row[2] or user_id}", role=row[1])}

    if not args:
      return {"response_type": "ephemeral", "text": START_INVITE_REQUIRED}

    otp = args[0]
    role = await self.db.consume_invite(otp)
    if not role:
      return {"response_type": "ephemeral", "text": START_INVITE_INVALID}
    await self.db.ensure_user(
      user_id,
      role=role,
      username=_mm_username(payload) or None,
      first_name=payload.get("first_name"),
      last_name=payload.get("last_name"),
    )
    return {"response_type": "ephemeral", "text": START_INVITE_OK_FMT.format(role=role)}

  async def _cmd_help(self, payload: dict[str, Any]) -> dict[str, Any]:
    role = await self.db.get_role(_mm_user_id(payload)) or ROLE_VIEWER
    if role == ROLE_ADMIN:
      text = HELP_ADMIN
    elif role == ROLE_MAINTAINER:
      text = HELP_MAINTAINER
    else:
      text = HELP_VIEWER
    return {"response_type": "ephemeral", "text": text}

  async def _top_hosts_attachment(self, conv_type: str) -> dict[str, Any]:
    host_names = sorted(list(ALLOW_HOSTS.values()), key=natural_key)
    action_name = "graph_host" if conv_type == CONV_TYPE_GRAPH else "maint_host"
    buttons = [
      self._button(host_name, action_name, {"host_name": host_name, "hostid": hostid, "conv_type": conv_type})
      for hostid, host_name in sorted(ALLOW_HOSTS.items(), key=lambda kv: natural_key(kv[1]))
    ]
    text = DEVICE_SELECT_TITLE if conv_type == CONV_TYPE_GRAPH else HOST_SELECT_TITLE
    return self._attachment(text=text, fallback=text, actions=buttons)

  async def _cmd_graphs(self, payload: dict[str, Any]) -> dict[str, Any]:
    att = await self._top_hosts_attachment(CONV_TYPE_GRAPH)
    return {"response_type": "in_channel", "text": DEVICE_SELECT_TITLE, "props": {"attachments": [att]}}

  async def _cmd_maint(self, payload: dict[str, Any]) -> dict[str, Any]:
    att = await self._top_hosts_attachment(CONV_TYPE_MAINT)
    return {"response_type": "in_channel", "text": HOST_SELECT_TITLE, "props": {"attachments": [att]}}

  async def _cmd_settz(self, payload: dict[str, Any], args: list[str]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not args:
      cur = await self.db.get_timezone(user_id)
      return {"response_type": "ephemeral", "text": SETTZ_CURRENT_FMT.format(tz=cur)}
    tz = args[0]
    try:
      ZoneInfo(tz)
    except Exception:
      return {"response_type": "ephemeral", "text": SETTZ_INVALID}
    await self.db.set_timezone(user_id, tz)
    return {"response_type": "ephemeral", "text": SETTZ_OK_FMT.format(tz=tz)}

  async def _cmd_refresh(self, payload: dict[str, Any]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_admin(user_id):
      return {"response_type": "ephemeral", "text": ACCESS_DENIED}
    await self.items.refresh()
    await self._ensure_maintenance_containers()
    self.graph_svc.clear_signature_cache()
    return {"response_type": "ephemeral", "text": REFRESH_DONE}

  async def _report_bounds(self, tz: ZoneInfo, period_type: str, start_ts: Optional[int] = None) -> tuple[int, int]:
    svc = ReportService(self.zbx, tz=tz)
    if start_ts is None:
      if period_type == "week":
        period = svc.last_week_period()
      else:
        period = svc.last_month_period()
      return period.start_ts, period.end_ts
    d = datetime.fromtimestamp(start_ts, tz).date()
    if period_type == "week":
      s, e, _ = svc.week_bounds_by_any_date(d)
    else:
      s, e, _ = svc.month_bounds_by_any_date(d)
    return s, e

  async def _cmd_report(self, payload: dict[str, Any], args: list[str]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.role_at_least(user_id, ROLE_VIEWER):
      return {"response_type": "ephemeral", "text": ACCESS_DENIED}
    if not args:
      return {"response_type": "ephemeral", "text": REPORT_USAGE}

    kind_raw = (args[0] or "").lower()
    if kind_raw.startswith(("w", "н")):
      period_type = "week"
    elif kind_raw.startswith(("m", "м")):
      period_type = "month"
    elif kind_raw.startswith(("l", "c")):
      return await self._cmd_report_list(payload)
    else:
      return {"response_type": "ephemeral", "text": REPORT_BAD_PERIOD}

    tz = await self._user_tz(user_id)
    svc = ReportService(self.zbx, tz=tz)
    when_text = " ".join(args[1:]).strip() if len(args) > 1 else ""
    if when_text:
      dt = datetime.fromtimestamp(0, tz)
      parsed = parse_date(when_text, tz)
      if not parsed:
        return {"response_type": "ephemeral", "text": REPORT_DATE_PARSE_FAIL}
      d = parsed.date()
      if period_type == "week":
        s, e, _ = svc.week_bounds_by_any_date(d)
      else:
        s, e, _ = svc.month_bounds_by_any_date(d)
      start_ts, end_ts = s, e
    else:
      start_ts, end_ts = await self._report_bounds(tz, period_type)

    title = REPORT_CONFIRM_TITLE_WEEK if period_type == "week" else REPORT_CONFIRM_TITLE_MONTH
    start_s = datetime.fromtimestamp(start_ts, tz).strftime(DT_FMT)
    end_s = datetime.fromtimestamp(end_ts, tz).strftime(DT_FMT)
    att = self._attachment(
      text=f"{title}\n{REPORT_CONFIRM_RANGE_FMT.format(start=start_s, end=end_s)}",
      fallback=title,
      actions=[
        self._button(BTN_REPORT_CONFIRM, "report_confirm", {"period_type": period_type, "start_ts": start_ts, "end_ts": end_ts}),
        self._button(BTN_REPORT_CANCEL, "report_cancel", {"period_type": period_type, "start_ts": start_ts, "end_ts": end_ts}, style="danger"),
      ],
    )
    return {"response_type": "in_channel", "text": title, "props": {"attachments": [att]}}

  async def _cmd_report_list(self, payload: dict[str, Any]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    tz = await self._user_tz(user_id)
    svc = ReportService(self.zbx, tz=tz)
    now = datetime.now(tz)

    monday_this = now.date() - timedelta(days=(now.isoweekday() - 1))
    week_buttons: list[tuple[str, int]] = []
    for i in range(1, max(0, REPORT_PREGEN_WEEKS) + 1):
      monday = monday_this - timedelta(days=7 * i)
      s, e, _ = svc.week_bounds_by_any_date(monday)
      sd = datetime.fromtimestamp(s, tz).date()
      iso_year, iso_week, _w = sd.isocalendar()
      ed = (datetime.fromtimestamp(e, tz).date() - timedelta(days=1))
      label = f"Нед.{iso_week}/{iso_year} {sd.strftime('%d.%m')}–{ed.strftime('%d.%m')}"
      week_buttons.append((label, s))

    y, m = now.year, now.month
    month_buttons: list[tuple[str, int]] = []
    for _ in range(max(0, REPORT_PREGEN_MONTHS)):
      m -= 1
      if m == 0:
        m = 12
        y -= 1
      any_day = date(y, m, 15)
      s, e, _ = svc.month_bounds_by_any_date(any_day)
      sd = datetime.fromtimestamp(s, tz).date()
      label = sd.strftime("%Y-%m")
      month_buttons.append((label, s))

    buttons = [
      self._button(lbl, "report_send", {"period_type": "week", "start_ts": start_ts})
      for lbl, start_ts in week_buttons
    ] + [
      self._button(lbl, "report_send", {"period_type": "month", "start_ts": start_ts})
      for lbl, start_ts in month_buttons
    ]
    att = self._attachment(text=REPORT_LIST_TITLE, fallback=REPORT_LIST_TITLE, actions=buttons)
    return {"response_type": "in_channel", "text": REPORT_LIST_TITLE, "props": {"attachments": [att]}}

  async def _generate_report_and_post(self, payload: dict[str, Any], period_type: str, start_ts: int) -> None:
    tz = await self._user_tz(_mm_user_id(payload))
    svc = ReportService(self.zbx, tz=tz)
    if period_type == "week":
      d = datetime.fromtimestamp(start_ts, tz).date()
      s, e, _ = svc.week_bounds_by_any_date(d)
    else:
      d = datetime.fromtimestamp(start_ts, tz).date()
      s, e, _ = svc.month_bounds_by_any_date(d)
    period = ReportPeriod(start_ts=s, end_ts=e, label="")
    path = await svc.ensure_report_file(self.db, REPORT_DASHBOARD_ID, period_type, period, REPORT_STORAGE_DIR)
    await self.db.upsert_report_path(REPORT_DASHBOARD_ID, period_type, s, e, str(path))

    # Prefer file_id reuse if already known.
    rec = await self.db.get_report_record(REPORT_DASHBOARD_ID, period_type, s)
    cached_file_id = rec[1] if rec else None
    caption = f"{datetime.fromtimestamp(s, tz).strftime(DT_FMT)} — {datetime.fromtimestamp(e, tz).strftime(DT_FMT)}"
    channel_id = str(payload.get("channel_id") or "")
    if cached_file_id:
      self.api.create_post({
        "channel_id": channel_id,
        "message": caption,
        "file_ids": [cached_file_id],
      })
      return

    with open(path, "rb") as fh:
      upload = self.api.upload_file(channel_id, Path(path).name, fh.read(), content_type="application/pdf")
    file_id = None
    if isinstance(upload, dict):
      if "file_infos" in upload and upload["file_infos"]:
        file_id = upload["file_infos"][0].get("id")
      elif "id" in upload:
        file_id = upload.get("id")
    if file_id:
      await self.db.set_report_file_id(REPORT_DASHBOARD_ID, period_type, s, file_id)
      self.api.create_post({"channel_id": channel_id, "message": caption, "file_ids": [file_id]})
    else:
      self.api.create_post({"channel_id": channel_id, "message": f"{caption}\n{path}"})

  async def _cmd_audit(self, payload: dict[str, Any], args: list[str]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_admin(user_id):
      return {"response_type": "ephemeral", "text": ACCESS_DENIED}
    filter_text = " ".join(args).strip() if args else None
    rows = await self.db.list_maint_audit(limit=AUDIT_LIST_LIMIT, filter_text=filter_text or None)
    if not rows:
      return {"response_type": "ephemeral", "text": AUDIT_EMPTY}
    tz = await self._user_tz(user_id)
    lines: list[str] = []
    for ts_str, action, username, item_name, host_name, start_ts, end_ts in rows:
      try:
        dt_utc = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("UTC"))
        dt_loc = dt_utc.astimezone(tz)
        dt_s = dt_loc.strftime(AUDIT_LINE_DT_FMT)
      except Exception:
        dt_s = ts_str
      verb = AUDIT_VERBS.get(action, action)
      if start_ts and end_ts:
        s_local = datetime.fromtimestamp(int(start_ts), tz).strftime(DT_FMT)
        e_local = datetime.fromtimestamp(int(end_ts), tz).strftime(DT_FMT)
        period = f"{s_local} - {e_local}"
      else:
        period = "-"
      lines.append(AUDIT_LINE_FMT.format(
        dt=dt_s,
        item=item_name or "(item)",
        host=host_name or "(host)",
        user=username,
        verb=verb,
        period=period,
      ))
    return {"response_type": "ephemeral", "text": "\n\n".join(lines)}

  async def _cmd_invgen(self, payload: dict[str, Any], args: list[str]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_admin(user_id):
      return {"response_type": "ephemeral", "text": ACCESS_DENIED}
    if not args:
      return {"response_type": "ephemeral", "text": INVGEN_USAGE}
    role = args[0]
    if role not in VALID_ROLES:
      return {"response_type": "ephemeral", "text": INVALID_ROLE}
    max_uses = 1
    ttl_sec = None
    if len(args) >= 2:
      try:
        max_uses = int(args[1])
      except Exception:
        pass
    if len(args) >= 3:
      try:
        ttl_sec = int(args[2])
      except Exception:
        pass
    otp = await self.db.create_invite(role, max_uses=max_uses, ttl_sec=ttl_sec)
    ttl = format_duration(ttl_sec) if ttl_sec else "вечно"
    return {"response_type": "ephemeral", "text": INVITE_REPLY_FMT.format(link=f"/monbot start {otp}", role=role, max_uses=max_uses, ttl=ttl)}

  async def _cmd_adduser(self, payload: dict[str, Any], args: list[str]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_admin(user_id):
      return {"response_type": "ephemeral", "text": ACCESS_DENIED}
    if not args:
      return {"response_type": "ephemeral", "text": ADDUSER_USAGE}
    try:
      uid = args[0]
      role = args[1] if len(args) > 1 else ROLE_VIEWER
      if role not in VALID_ROLES:
        return {"response_type": "ephemeral", "text": INVALID_ROLE}
      await self.db.ensure_user(uid, role=role)
      return {"response_type": "ephemeral", "text": ADDUSER_OK_FMT.format(uid=uid, role=role)}
    except Exception:
      return {"response_type": "ephemeral", "text": INVALID_TELEGRAM_ID}

  async def _cmd_setrole(self, payload: dict[str, Any], args: list[str]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_admin(user_id):
      return {"response_type": "ephemeral", "text": ACCESS_DENIED}
    if len(args) < 2:
      return {"response_type": "ephemeral", "text": SETROLE_USAGE}
    uid = args[0]
    role = args[1]
    if role not in VALID_ROLES:
      return {"response_type": "ephemeral", "text": INVALID_ROLE}
    ok = await self.db.set_role(uid, role)
    return {"response_type": "ephemeral", "text": SETROLE_OK_FMT.format(uid=uid, role=role, ok=ok)}

  async def _cmd_listusers(self, payload: dict[str, Any]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_admin(user_id):
      return {"response_type": "ephemeral", "text": ACCESS_DENIED}
    users = await self.db.list_users()
    if not users:
      return {"response_type": "ephemeral", "text": USERS_EMPTY}
    lines = [LIST_USERS_HEADER]
    for uid, role, username, first_name, last_name in users:
      disp = f"{first_name or ''} {last_name or ''}".strip()
      uname = f"@{username}" if username else ""
      who = disp or uname or str(uid)
      lines.append(f"`{uid}`:{role}: {uname} ({who})")
    return {"response_type": "ephemeral", "text": "\n".join(lines)}

  async def _cmd_deluser(self, payload: dict[str, Any], args: list[str]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_admin(user_id):
      return {"response_type": "ephemeral", "text": ACCESS_DENIED}
    if not args:
      return {"response_type": "ephemeral", "text": DELUSER_USAGE}
    uid = args[0]
    ok = await self.db.delete_user(uid)
    return {"response_type": "ephemeral", "text": DELUSER_OK_FMT.format(uid=uid, ok=ok)}

  async def handle_action(self, payload: dict[str, Any]) -> dict[str, Any]:
    ctx = payload.get("context") or {}
    action = str(ctx.get("action") or "")
    if action == "graph_host":
      return await self._action_graph_host(payload, ctx)
    if action == "graph_item":
      return await self._action_graph_item(payload, ctx)
    if action == "go_maint":
      return await self._action_go_maint(payload, ctx)
    if action == "restart":
      return await self._action_restart(payload, ctx)
    if action == "maint_host":
      return await self._action_maint_host(payload, ctx)
    if action == "maint_item":
      return await self._action_maint_item(payload, ctx)
    if action == "maint_fast":
      return await self._action_maint_fast(payload, ctx)
    if action == "maint_end":
      return await self._action_maint_end(payload, ctx)
    if action == "maint_add":
      return await self._action_maint_add(payload, ctx)
    if action == "maint_new":
      return await self._action_maint_new(payload, ctx)
    if action == "maint_confirm":
      return await self._action_maint_confirm(payload, ctx)
    if action == "maint_cancel":
      return await self._action_maint_cancel(payload, ctx)
    if action == "report_confirm":
      return await self._action_report_confirm(payload, ctx)
    if action == "report_cancel":
      return await self._action_report_cancel(payload, ctx)
    if action == "report_send":
      return await self._action_report_send(payload, ctx)
    return {"error": {"message": f"Unknown action: {action}"}}

  async def _action_graph_host(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    await self._ensure_user_record({"user_id": user_id, "user_name": payload.get("user_name")})
    hostid = str(ctx.get("hostid") or "")
    items = self.items.items_by_hostid(hostid)
    buttons = [self._button(it.name, "graph_item", {"itemid": it.itemid, "hostid": hostid, "host_name": self.items.host_name_by_hostid(hostid) or "", "period": DEFAULT_GRAPH_ITEM_PERIOD})
               for it in items]
    buttons.append(self._button(BTN_DEVICE, "restart", {}, style="default"))
    att = self._attachment(text=ITEM_SELECT_TITLE.format(host=self.items.host_name_by_hostid(hostid) or ""), fallback="items", actions=buttons)
    return {"update": {"message": ITEM_SELECT_TITLE.format(host=self.items.host_name_by_hostid(hostid) or ""), "props": {"attachments": [att]}}}

  async def _action_graph_item(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    await self._ensure_user_record({"user_id": user_id, "user_name": payload.get("user_name")})
    itemid = str(ctx.get("itemid") or "")
    period = str(ctx.get("period") or DEFAULT_GRAPH_ITEM_PERIOD)
    info = self.items.get_item(itemid)
    if not info:
      return {"error": {"message": "Item not found"}}
    tz = await self._user_tz(user_id)
    key_parts, ttl, cache_res = await self.graph_svc.get_item_media_from_item(
      hostid=info.hostid, itemid=info.itemid, name=info.name, color=info.color, units=info.units,
      period_label=period, width=IMG_WIDTH, height=IMG_HEIGHT, tz=tz
    )
    token = self.mm_cache.key_digest(key_parts)
    image_url = self.asset_url(token, "png")
    host_name = self.items.host_name_by_hostid(info.hostid) or ""
    buttons = [self._button(tr, "graph_item", {"itemid": info.itemid, "hostid": info.hostid, "host_name": host_name, "period": tr})
               for tr in TIME_RANGES]
    buttons.append(self._button(BTN_MAINT, "go_maint", {"itemid": info.itemid}, style="primary"))
    buttons.append(self._button(host_name or BTN_BACK_ITEMS, "graph_host", {"hostid": info.hostid, "host_name": host_name}))
    att = self._attachment(
      text=f"{info.name}\n{host_name}",
      fallback=info.name,
      image_url=image_url,
      actions=buttons,
    )
    return {"update": {"message": info.name, "props": {"attachments": [att]}}}

  async def _action_restart(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    att = await self._top_hosts_attachment(CONV_TYPE_GRAPH)
    return {"update": {"message": DEVICE_SELECT_TITLE, "props": {"attachments": [att]}}}

  async def _action_go_maint(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    itemid = str(ctx.get("itemid") or "")
    info = self.items.get_item(itemid)
    if not info:
      return {"error": {"message": "Item not found"}}
    return await self._maint_view_update(payload, info.itemid)

  async def _action_maint_host(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    hostid = str(ctx.get("hostid") or "")
    items = self.items.items_by_hostid(hostid)
    active_set = await asyncio.to_thread(self.maint_svc.active_items_for_host, hostid)
    buttons = []
    for it in items:
      label = f"{ACTIVE_BULLET if it.itemid in active_set else INACTIVE_BULLET} {it.name}"
      buttons.append(self._button(label, "maint_item", {"itemid": it.itemid, "hostid": hostid}))
    buttons.append(self._button(BTN_BACK_HOST, "restart", {}))
    att = self._attachment(text=ITEM_SELECT_TITLE.format(host=self.items.host_name_by_hostid(hostid) or ""), fallback="items", actions=buttons)
    return {"update": {"message": ITEM_SELECT_TITLE.format(host=self.items.host_name_by_hostid(hostid) or ""), "props": {"attachments": [att]}}}

  async def _maint_view_update(self, payload: dict[str, Any], itemid: str) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    role = await self._ensure_user_record({"user_id": user_id, "user_name": payload.get("user_name")})
    text, att = await self._build_maint_view(itemid, user_id, role or ROLE_VIEWER)
    return {"update": {"message": text, "props": {"attachments": [att]}}}

  async def _build_maint_view(self, itemid: str, user_id: str, role: str) -> tuple[str, dict[str, Any]]:
    tz = await self._user_tz(user_id)
    c, periods = await asyncio.to_thread(self.maint_svc.list_periods, itemid)
    now = int(time.time())
    is_active = any(s <= now <= e for s, e in periods)
    title = f"{c.get('name', '')}\n{ITEM_STATUS_ACTIVE if is_active else ITEM_STATUS_INACTIVE}\n"
    parts = format_periods(tz, periods, MAINT_LIST_LIMIT)
    text = "\n".join([title] + (parts or [PERIODS_EMPTY]))
    can_edit = role in (ROLE_MAINTAINER, ROLE_ADMIN)
    buttons: list[dict[str, Any]] = []
    if can_edit:
      if is_active:
        for label, secs in PRESET_ACTIVE_PROLONG:
          buttons.append(self._button(label, "maint_add", {"itemid": itemid, "secs": secs}))
        buttons.append(self._button(BTN_END_NOW, "maint_end", {"itemid": itemid}, style="danger"))
        buttons.append(self._button(BTN_NEW_PERIOD, "maint_new", {"itemid": itemid}, style="primary"))
      else:
        buttons.append(self._button(BTN_ADD_DAY_NOW, "maint_fast", {"itemid": itemid}, style="primary"))
        buttons.append(self._button(BTN_NEW_PERIOD, "maint_new", {"itemid": itemid}, style="primary"))
    buttons.append(self._button(BTN_GRAPH, "graph_item", {"itemid": itemid, "period": DEFAULT_GRAPH_ITEM_PERIOD}))
    buttons.append(self._button(BTN_BACK_ITEMS, "maint_host", {"hostid": c.get("hostid", ""), "host_name": self.items.host_name_by_hostid(c.get("hostid", "")) or ""}))
    att = self._attachment(text=text, fallback=text, actions=buttons)
    return text, att

  async def _action_maint_item(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    itemid = str(ctx.get("itemid") or "")
    return await self._maint_view_update(payload, itemid)

  async def _action_maint_fast(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_maintainer(user_id):
      return {"error": {"message": INSUFFICIENT_PERMISSIONS}}
    itemid = str(ctx.get("itemid") or "")
    now = int(time.time())
    res = await asyncio.to_thread(self.maint_svc.add_period, itemid, now, now + 86400, 0)
    info = self.items.get_item(itemid)
    host_name = self.items.host_name_by_hostid(res.get("hostid", "")) or (self.items.host_name_by_hostid(info.hostid) if info else "")
    await self.db.audit_maint(
      user_id, "create", res["maintenanceid"], itemid, res.get("hostid", ""),
      res["before"], res["after"],
      username=payload.get("user_name"),
      host_name=host_name,
      item_name=(info.name if info else ""),
      start_ts=res.get("start_ts"),
      end_ts=res.get("end_ts"),
    )
    return await self._maint_view_update(payload, itemid)

  async def _action_maint_end(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_maintainer(user_id):
      return {"error": {"message": INSUFFICIENT_PERMISSIONS}}
    itemid = str(ctx.get("itemid") or "")
    res = await asyncio.to_thread(self.maint_svc.end_now, itemid)
    if res:
      info = self.items.get_item(itemid)
      host_name = self.items.host_name_by_hostid(res.get("hostid", "")) or (self.items.host_name_by_hostid(info.hostid) if info else "")
      await self.db.audit_maint(
        user_id, "end", res["maintenanceid"], itemid, res.get("hostid", ""),
        res["before"], res["after"],
        username=payload.get("user_name"),
        host_name=host_name,
        item_name=(info.name if info else ""),
        start_ts=res.get("start_ts"),
        end_ts=res.get("end_ts"),
      )
    return await self._maint_view_update(payload, itemid)

  async def _action_maint_add(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_maintainer(user_id):
      return {"error": {"message": INSUFFICIENT_PERMISSIONS}}
    itemid = str(ctx.get("itemid") or "")
    secs = int(ctx.get("secs") or 0)
    c, periods = await asyncio.to_thread(self.maint_svc.list_periods, itemid)
    now = int(time.time())
    active_period = next(((s, e) for (s, e) in periods if s <= now <= e), None)
    if active_period:
      s, e = active_period
      delta = secs
      res = await asyncio.to_thread(self.maint_svc.extend_active, itemid, delta)
      info = self.items.get_item(itemid)
      host_name = self.items.host_name_by_hostid(res.get("hostid", "")) or (self.items.host_name_by_hostid(info.hostid) if info else "")
      await self.db.audit_maint(
        user_id, "update", res["maintenanceid"], itemid, res.get("hostid", ""),
        res["before"], res["after"],
        username=payload.get("user_name"),
        host_name=host_name,
        item_name=(info.name if info else ""),
        start_ts=res.get("start_ts"),
        end_ts=res.get("end_ts"),
      )
    return await self._maint_view_update(payload, itemid)

  async def _action_maint_new(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    user_id = _mm_user_id(payload)
    if not await self.db.is_maintainer(user_id):
      return {"error": {"message": INSUFFICIENT_PERMISSIONS}}
    itemid = str(ctx.get("itemid") or "")
    dialog = {
      "callback_id": "maint_period",
      "title": "Обслуживание",
      "introduction_text": INPUT_INSTRUCTIONS,
      "elements": [
        {
          "display_name": "Период",
          "name": "period",
          "type": "textarea",
          "placeholder": INPUT_PERIOD_REQUEST_PLACEHOLDER,
          "help_text": "Введите один или два значения периода через новую строку, `;` или ` - `.",
        }
      ],
      "submit_label": BTN_CONFIRM,
      "notify_on_cancel": True,
      "state": json.dumps({
        "action": "maint_new",
        "itemid": itemid,
        "post_id": payload.get("post_id"),
        "channel_id": payload.get("channel_id"),
        "auth": self.secret if self.secret else "",
      }),
    }
    self.api.open_dialog(str(payload.get("trigger_id") or ""), self.dialog_url, dialog)
    return {"ephemeral_text": "Open the dialog in your client."}

  async def _action_maint_cancel(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    itemid = str(ctx.get("itemid") or "")
    return await self._maint_view_update(payload, itemid)

  async def _action_maint_confirm(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    return {"ephemeral_text": "Use the dialog/quick action flow."}

  async def _action_report_confirm(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    period_type = str(ctx.get("period_type") or "week")
    start_ts = int(ctx.get("start_ts") or 0)
    end_ts = int(ctx.get("end_ts") or 0)
    await self._generate_report_and_post(payload, period_type, start_ts)
    return {"update": {"message": REPORT_SENDING, "props": {}}}

  async def _action_report_cancel(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    return {"update": {"message": REPORT_CANCELLED, "props": {}}}

  async def _action_report_send(self, payload: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    period_type = str(ctx.get("period_type") or "week")
    start_ts = int(ctx.get("start_ts") or 0)
    await self._generate_report_and_post(payload, period_type, start_ts)
    return {"update": {"message": REPORT_SENDING, "props": {}}}

  async def handle_dialog(self, payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("cancelled"):
      return {"type": "ok"}
    state = payload.get("state") or "{}"
    try:
      state_obj = json.loads(state) if isinstance(state, str) else dict(state)
    except Exception:
      state_obj = {}
    if state_obj.get("action") != "maint_new":
      return {"type": "ok"}
    if self.secret and str(state_obj.get("auth") or "") != self.secret:
      return {"error": "Unauthorized request"}
    user_id = str(payload.get("user_id") or "")
    if not await self.db.is_maintainer(user_id):
      return {"error": "Insufficient permissions"}
    period_raw = str((payload.get("submission") or {}).get("period") or "").strip()
    itemid = str(state_obj.get("itemid") or "")
    parts = _split_period_text(period_raw)
    tz = await self._user_tz(user_id)
    now_ts = int(time.time())
    if len(parts) == 1:
      start_ts = now_ts
      end_dt = parse_date(parts[0], tz)
      if not end_dt:
        return {"errors": {"period": PARSE_FAIL_END}}
      if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=tz)
      end_ts = int(end_dt.timestamp())
    else:
      s_dt = parse_date(parts[0], tz)
      e_dt = parse_date(parts[1], tz) if len(parts) >= 2 else None
      if not s_dt or not e_dt:
        return {"errors": {"period": PARSE_FAIL_BOTH}}
      if s_dt.tzinfo is None:
        s_dt = s_dt.replace(tzinfo=tz)
      if e_dt.tzinfo is None:
        e_dt = e_dt.replace(tzinfo=tz)
      start_ts = int(s_dt.timestamp())
      end_ts = int(e_dt.timestamp())
    if end_ts <= start_ts or end_ts <= now_ts:
      return {"errors": {"period": INVALID_PERIOD_MSG}}

    res = await asyncio.to_thread(self.maint_svc.add_period, itemid, start_ts, end_ts, 0)
    info = self.items.get_item(itemid)
    host_name = self.items.host_name_by_hostid(res.get("hostid", "")) or (self.items.host_name_by_hostid(info.hostid) if info else "")
    await self.db.audit_maint(
      user_id, "create", res["maintenanceid"], itemid, res.get("hostid", ""),
      res["before"], res["after"],
      username=payload.get("username"),
      host_name=host_name,
      item_name=(info.name if info else ""),
      start_ts=res.get("start_ts"),
      end_ts=res.get("end_ts"),
    )
    post_id = str(state_obj.get("post_id") or "")
    if post_id:
      text, att = await self._build_maint_view(itemid, user_id, ROLE_MAINTAINER)
      self.api.update_post(post_id, {"id": post_id, "message": text, "props": {"attachments": [att]}})
    return {"type": "ok"}

  async def handle_asset(self, token: str, ext: str) -> tuple[int, bytes, str]:
    path = MM_CACHE_DIR / f"{token}.{ext}"
    if path.exists():
      content_type = "image/png" if ext == "png" else "application/pdf"
      return 200, path.read_bytes(), content_type
    return 404, b"not found", "text/plain"
