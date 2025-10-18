from __future__ import annotations

import json
import logging
import time
from typing import Any, List, Optional, Set, Tuple

from monbot.config import MAINT_DEFAULT_PAST_START_SEC, MAINT_MIN_PERIOD_SEC
from monbot.zabbix import ZabbixWeb

MAX_TS_2038 = 2_147_483_647
TAG_OPERATOR_EQUALS = 0
TAG_OPERATOR_CONTAINS = 2  # Zabbix 6.x

logger = logging.getLogger(__name__)


class MaintenanceService:
  def __init__(self, zbx: ZabbixWeb, tag_key: str = "channel"):
    self.zbx = zbx
    self.tag_key = tag_key

  def _api(self, method: str, params: Any) -> Any:
    fn = getattr(self.zbx, "_api_request", None) or getattr(self.zbx, "api_request", None)
    return fn(method, params)

  def get_item(self, itemid: str) -> dict:
    res = self._api("item.get", {"output": ["itemid", "name", "hostid"], "itemids": [itemid]})
    if not res:
      raise ValueError(f"Item not found: {itemid}")
    return res[0]

  def find_container(self, hostid: str, name: str) -> Optional[dict]:
    res = self._api("maintenance.get", {
      "output": "extend",
      "selectTimeperiods": ["timeperiod_type", "start_date", "period"],
      "selectHosts": ["hostid", "name"],
      "selectTags": "extend",
      "hostids": [hostid],
      "filter": {"status": 0},
    })
    for m in res:
      if m.get("name") == name:
        return m
    return None

  def ensure_container(self, itemid: str) -> dict:
    it = self.get_item(itemid)
    hostid, name = it["hostid"], it["name"]
    existing = self.find_container(hostid, name)
    if existing:
      return existing
    past_start = MAINT_DEFAULT_PAST_START_SEC
    past_period = MAINT_MIN_PERIOD_SEC
    params = {
      "name": name,
      "maintenance_type": 0,  # with data collection
      "active_since": past_start,
      "active_till": MAX_TS_2038,
      "description": f"monbot created for {name}",
      "hostids": [hostid],
      "timeperiods": [{
        "timeperiod_type": 0,  # one-time
        "start_date": past_start,
        "period": past_period,
      }],
      "tags": [{
        "tag": self.tag_key,
        "operator": TAG_OPERATOR_CONTAINS,
        "value": name,
      }],
    }
    self._api("maintenance.create", params)
    return self.find_container(hostid, name) or {}

  def list_periods(self, itemid: str) -> Tuple[dict, List[Tuple[int, int]]]:
    c = self.ensure_container(itemid)
    logger.info("Maintenance list_periods: %s", c)
    tps = c.get("timeperiods") or []
    periods: List[Tuple[int, int]] = []
    for tp in tps:
      start = int(tp.get("start_date", 0))
      per = int(tp.get("period", 0))
      if start and per:
        periods.append((start, start + per))
    # Sort desc by start
    periods.sort(key=lambda t: t[0], reverse=True)
    return c, periods

  def add_period(self, itemid: str, start_ts: int, end_ts: int, mtype: int = 0) -> dict:
    if end_ts <= start_ts:
      raise ValueError("end must be after start")
    maint = self.ensure_container(itemid)
    logger.info("Maintenance add_period: %s", maint)
    mid = maint["maintenanceid"]
    tps = maint.get("timeperiods") or []
    # append new one-time period
    tps.append({
      "timeperiod_type": 0,
      "start_date": start_ts,
      "period": end_ts - start_ts,
    })
    params = {
      "maintenanceid": mid,
      "timeperiods": tps,
    }
    before = json.dumps({"timeperiods": maint.get("timeperiods")}, separators=(",", ":"))
    res = self._api("maintenance.update", {"maintenanceid": mid, "timeperiods": tps})
    after = json.dumps({"timeperiods": tps}, separators=(",", ":"))
    return {
      "update": res,
      "before": before,
      "after": after,
      "maintenanceid": mid,
      "hostid": (maint.get("hosts") or [{}])[0].get("hostid", ""),
      "start_ts": start_ts,
      "end_ts": end_ts,
    }

  def end_now(self, itemid: str, now_ts: Optional[int] = None) -> dict | None:
    now = int(time.time()) if now_ts is None else int(now_ts)
    maint = self.ensure_container(itemid)
    mid = maint["maintenanceid"]
    tps = maint.get("timeperiods") or []
    changed = False
    new_tps: List[dict] = []
    affected_start = None
    affected_old_end = None
    for tp in tps:
      start = int(tp.get("start_date", 0))
      per = int(tp.get("period", 0))
      end = start + per
      if start <= now <= end:
        affected_start = start
        affected_old_end = end
        newdur = max(0, now - start)
        if newdur >= MAINT_MIN_PERIOD_SEC:
          logger.info("Maintenance end_now, time lasted more then 5 min, updating: %s", tp)
          new_tps.append({"timeperiod_type": 0, "start_date": start, "period": newdur})
          changed = True
        else:
          logger.info("Maintenance end_now, time lasted less then 5 min, skipping period: %s", tp)
          changed = True # still need to update to remove old periods
      else:
        new_tps.append(tp)
    if not changed:
      logger.info("Periods not changed, skipping update")
      return None
    before = json.dumps({"timeperiods": tps}, separators=(",", ":"))
    res = self._api("maintenance.update", {"maintenanceid": mid, "timeperiods": new_tps})
    after = json.dumps({"timeperiods": new_tps}, separators=(",", ":"))
    return {
      "update": res,
      "before": before,
      "after": after,
      "maintenanceid": mid,
      "hostid": (maint.get("hosts") or [{}])[0].get("hostid", ""),
      "start_ts": affected_start,
      "end_ts": now,
      "old_end_ts": affected_old_end,
    }

  def active_items_for_host(self, hostid: str) -> Set[str]:
    """Return itemids that currently have an active period on this host (now inside any timeperiod),
       restricted by maintenance tag key existence."""
    now = int(time.time())
    res = self._api("maintenance.get", {
      "output": "extend",
      "selectTimeperiods": "extend",
      "selectTags": "extend",
      "selectHosts": ["hostid"],
      "hostids": [hostid],
      "filter": {"status": 0},
      "tags": [{"tag": self.tag_key, "operator": 4}], # 4 = exists
    })
    active: Set[str] = set()
    for m in res or []:
      # check active by window (timeperiods define schedule but active_since/active_till may not reflect all)
      tps = m.get("timeperiods") or []
      if any(
          int(tp.get("start_date", 0)) <= now <= int(tp.get("start_date", 0)) + int(tp.get("period", 0)) for tp in tps):
        # map maintenance name back to item name (we ensured container name=item.name)
        iname = m.get("name") or ""
        # fetch itemids that match channel contains=iname for this host
        # optimization: find items via functions in triggers if needed; simpler: rely on container name==item.name
        # we resolve the single item by name
        # item.get by host + name exact
        it = self._api("item.get",
                       {"output": ["itemid"], "hostids": [hostid], "search": {"name": iname}, "searchByAny": True})
        for row in it or []:
          active.add(str(row["itemid"]))
    return active

  def extend_active(self, itemid: str, delta_sec: int) -> dict:
    now = int(time.time())
    maint = self.ensure_container(itemid)
    mid = maint["maintenanceid"]
    tps = maint.get("timeperiods") or []
    new_tps: List[dict] = []
    changed = False
    affected_start = None
    old_end = None
    new_end = None
    for tp in tps:
      start = int(tp.get("start_date", 0))
      per = int(tp.get("period", 0))
      end = start + per
      if start <= now <= end:
        affected_start = start
        old_end = end
        per = per + max(1, delta_sec)
        new_end = start + per
        changed = True
      new_tps.append({"timeperiod_type": 0, "start_date": start, "period": per})
    before = json.dumps({"timeperiods": tps}, separators=(",", ":"))
    res = self._api("maintenance.update", {"maintenanceid": mid, "timeperiods": new_tps})
    after = json.dumps({"timeperiods": new_tps}, separators=(",", ":"))
    return {
      "update": res,
      "before": before,
      "after": after,
      "maintenanceid": mid,
      "hostid": (maint.get("hosts") or [{}])[0].get("hostid", ""),
      "start_ts": affected_start,
      "end_ts": new_end,
      "old_end_ts": old_end,
      "delta": max(1, delta_sec),
    }
