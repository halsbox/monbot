from __future__ import annotations

import json
import logging
import time
from typing import Any, List, Optional, Set, Tuple

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
    past_start = 86400  # Unix Epoch start (0 is "never" in zabbix)
    past_period = 300  # zabbix minimal allowed period is 300s
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
    res = self._api("maintenance.create", params)
    # Reload container
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
    c = self.ensure_container(itemid)
    logger.info("Maintenance add_period: %s", c)
    mid = c["maintenanceid"]
    tps = c.get("timeperiods") or []
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
    before = json.dumps({"timeperiods": c.get("timeperiods")}, separators=(",", ":"))
    logger.info("Maintenance update: %s", params)
    res = self._api("maintenance.update", params)
    logger.info("Maintenance update result: %s", res)
    after = json.dumps({"timeperiods": tps}, separators=(",", ":"))
    return {"update": res, "before": before, "after": after, "maintenanceid": mid,
            "hostid": (c.get("hosts") or [{}])[0].get("hostid", "")}

  def end_now(self, itemid: str, now_ts: Optional[int] = None) -> dict | None:
    now = int(time.time()) if now_ts is None else int(now_ts)
    c = self.ensure_container(itemid)
    mid = c["maintenanceid"]
    tps = c.get("timeperiods") or []
    changed = False
    new_tps: List[dict] = []
    for tp in tps:
      start = int(tp.get("start_date", 0))
      per = int(tp.get("period", 0))
      if start <= now <= start + per:
        newdur = max(0, now - start)
        if newdur >= 300:
          logger.info("Maintenance end_now, time lasted more then 5 min, updating: %s", tp)
          new_tps.append({"timeperiod_type": 0, "start_date": start, "period": newdur})
        else:
          logger.info("Maintenance end_now, time lasted less then 5 min, skipping period: %s", tp)
        changed = True
      else:
        new_tps.append(tp)
    if not changed:
      logger.info("Periods not changed, skipping update")
      return None
    params = {"maintenanceid": mid, "timeperiods": new_tps}
    logger.info("Maintenance update: %s", params)
    before = json.dumps({"timeperiods": tps}, separators=(",", ":"))
    res = self._api("maintenance.update", params)
    logger.info("Maintenance update result: %s", res)
    after = json.dumps({"timeperiods": new_tps}, separators=(",", ":"))
    return {"update": res, "before": before, "after": after, "maintenanceid": mid,
            "hostid": (c.get("hosts") or [{}])[0].get("hostid", "")}

  def delete_container(self, itemid: str) -> dict:
    c = self.ensure_container(itemid)
    mid = c["maintenanceid"]
    res = self._api("maintenance.delete", [mid])
    return {"delete": res, "maintenanceid": mid, "hostid": (c.get("hosts") or [{}])[0].get("hostid", "")}

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
      "tags": [{"tag": self.tag_key, "operator": 1}],  # exists
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
    c = self.ensure_container(itemid)
    mid = c["maintenanceid"]
    tps = c.get("timeperiods") or []
    new_tps: List[dict] = []
    changed = False
    for tp in tps:
      start = int(tp.get("start_date", 0))
      per = int(tp.get("period", 0))
      end = start + per
      if start <= now <= end:
        per = per + max(1, delta_sec)
        changed = True
      new_tps.append({"timeperiod_type": 0, "start_date": start, "period": per})
    if not changed:
      return {"update": {}, "before": json.dumps({"timeperiods": tps}, separators=(",", ":")),
              "after": json.dumps({"timeperiods": new_tps}, separators=(",", ":")), "maintenanceid": mid,
              "hostid": (c.get("hosts") or [{}])[0].get("hostid", "")}
    before = json.dumps({"timeperiods": tps}, separators=(",", ":"))
    res = self._api("maintenance.update", {"maintenanceid": mid, "timeperiods": new_tps})
    after = json.dumps({"timeperiods": new_tps}, separators=(",", ":"))
    return {"update": res, "before": before, "after": after, "maintenanceid": mid,
            "hostid": (c.get("hosts") or [{}])[0].get("hostid", "")}

  def delete_active(self, itemid: str) -> dict:
    now = int(time.time())
    c = self.ensure_container(itemid)
    mid = c["maintenanceid"]
    tps = c.get("timeperiods") or []
    kept: List[dict] = []
    removed = False
    for tp in tps:
      start = int(tp.get("start_date", 0))
      per = int(tp.get("period", 0))
      end = start + per
      if start <= now <= end:
        removed = True
        continue
      kept.append({"timeperiod_type": 0, "start_date": start, "period": per})
    before = json.dumps({"timeperiods": tps}, separators=(",", ":"))
    res = self._api("maintenance.update", {"maintenanceid": mid, "timeperiods": kept})
    after = json.dumps({"timeperiods": kept}, separators=(",", ":"))
    return {"update": res, "before": before, "after": after, "maintenanceid": mid,
            "hostid": (c.get("hosts") or [{}])[0].get("hostid", "")}
