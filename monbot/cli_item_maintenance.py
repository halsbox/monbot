from __future__ import annotations

import argparse
import re
import time
from typing import Any, Dict, Optional

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=False)

from monbot.config import ZABBIX_URL, ZABBIX_USER, ZABBIX_PASS, ZABBIX_API_TOKEN, ZABBIX_VERIFY_SSL
from monbot.zabbix import ZabbixWeb

# Max 32-bit signed: 2038-01-19T03:14:07Z
MAX_TS_2038 = 2_147_483_647

# Adjust this if your Zabbix build uses different codes for maintenance tag operators.
TAG_OPERATOR_EQUALS = 0
TAG_OPERATOR_CONTAINS = 2


def _api(zbx: ZabbixWeb, method: str, params: Any) -> Any:
  fn = getattr(zbx, "_api_request", None) or getattr(zbx, "api_request", None)
  if fn is None:
    raise RuntimeError("ZabbixWeb has no API request method")
  return fn(method, params)


def parse_when(s: str) -> int:
  s = (s or "").strip().lower()
  if s in ("", "now"):
    return int(time.time())
  if s.isdigit():
    return int(s)
  # YYYY-MM-DD HH:MM or YYYY-MM-DDTHH:MM
  m = re.match(r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$", s)
  if m:
    y, mo, d, h, mi, sec = m.groups()
    tm = time.struct_time((int(y), int(mo), int(d), int(h), int(mi), int(sec) if sec else 0, -1, -1, -1))
    return int(time.mktime(tm))
  raise ValueError(f"Unrecognized datetime: {s}")


def parse_duration(s: str, default_seconds: int = 24 * 3600) -> int:
  if not s:
    return default_seconds
  s = s.strip().lower()
  if s.isdigit():
    return int(s)
  # Mixed like 1d2h30m10s
  total = 0
  for num, unit in re.findall(r"(\d+)([dhms])", s):
    v = int(num)
    if unit == "d":
      total += v * 86400
    elif unit == "h":
      total += v * 3600
    elif unit == "m":
      total += v * 60
    elif unit == "s":
      total += v
  if total == 0:
    raise ValueError(f"Unrecognized duration: {s}")
  return total


def get_item(zbx: ZabbixWeb, itemid: str) -> dict:
  res = _api(zbx, "item.get", {
    "output": ["itemid", "name", "hostid"],
    "itemids": [itemid],
  })
  if not res:
    raise ValueError(f"Item not found: {itemid}")
  return res[0]


def find_maintenance_by_name_for_host(zbx: ZabbixWeb, name: str, hostid: str) -> Optional[dict]:
  # maintenance.get cannot filter by name+host directly; fetch by host and match name client-side
  res = _api(zbx, "maintenance.get", {
    "output": "extend",
    "selectTimeperiods": "extend",
    "selectHosts": ["hostid", "name"],
    "selectTags": "extend",
    "hostids": [hostid],
    "filter": {"status": 0},  # enabled
  })
  for m in res:
    if m.get("name") == name:
      return m
  return None


def list_maintenances(zbx: ZabbixWeb, hostid: Optional[str], name: Optional[str]) -> int:
  params: Dict[str, Any] = {
    "output": "extend",
    "selectTimeperiods": "extend",
    "selectHosts": ["hostid", "name"],
    "selectTags": "extend",
    "sortfield": "maintenanceid",
    "sortorder": "ASC",
  }
  if hostid:
    params["hostids"] = [hostid]
  res = _api(zbx, "maintenance.get", params)
  now = int(time.time())
  for m in res:
    if name and m.get("name") != name:
      continue
    mid = m["maintenanceid"]
    mname = m.get("name", "")
    mtype = int(m.get("maintenance_type", 0))
    since = int(m.get("active_since", 0))
    till = int(m.get("active_till", 0))
    active = (since <= now <= till)
    tags = m.get("tags") or []
    tstr = ", ".join(f"{t.get('tag')}({t.get('operator')}):{t.get('value')}" for t in tags)
    print(f"{mid} name={mname!r} type={'with-data' if mtype == 0 else 'no-data'} active={active} "
          f"window=[{time.strftime('%Y-%m-%d %H:%M', time.localtime(since))} .. {time.strftime('%Y-%m-%d %H:%M', time.localtime(till))}]")
    if tags:
      print(f"  tags: {tstr}")
    hosts = m.get("hosts") or []
    if hosts:
      print("  hosts:", ", ".join(f"{h['hostid']}:{h.get('name', '')}" for h in hosts))
    for tp in (m.get("timeperiods") or []):
      print(
        f"  tp: type={tp.get('timeperiod_type')} start={time.strftime('%Y-%m-%d %H:%M', time.localtime(int(tp.get('start_date', since))))} period={int(tp.get('period', till - since))}s")
  return 0


def ensure_item_maintenance(zbx: ZabbixWeb, itemid: str, tag_key: str = "channel") -> int:
  it = get_item(zbx, itemid)
  hostid = it["hostid"]
  name = it["name"]

  existing = find_maintenance_by_name_for_host(zbx, name, hostid)
  if existing:
    print(f"Exists: maintenanceid={existing['maintenanceid']} name={name!r}")
    return 0

  now = int(time.time())
  params = {
    "name": name,
    "maintenance_type": 0,  # with data collection
    "active_since": now,
    "active_till": MAX_TS_2038,
    "description": f"monbot ensure per-item maintenance for {name}",
    "hostids": [hostid],
    "timeperiods": [{
      "timeperiod_type": 0,
      "start_date": now,
      "period": MAX_TS_2038 - now,
    }],
    # Maintenance tag filter to target only problems whose trigger tags match channel=<item_name>
    "tags": [{
      "tag": tag_key,
      "operator": TAG_OPERATOR_CONTAINS,
      "value": name,
    }],
  }
  res = _api(zbx, "maintenance.create", params)
  print("Created:", res)
  return 0


def create_item_maintenance(
    zbx: ZabbixWeb,
    itemid: str,
    start: str,
    duration: str,
    mtype: str,
    tag_key: str = "channel",
) -> int:
  it = get_item(zbx, itemid)
  hostid = it["hostid"]
  name = it["name"]
  start_ts = parse_when(start)
  dur = parse_duration(duration, default_seconds=24 * 3600)
  till = start_ts + dur
  if mtype not in ("with", "without"):
    raise ValueError("type must be 'with' or 'without'")
  maintenance_type = 0 if mtype == "with" else 1

  params = {
    "name": name,
    "maintenance_type": maintenance_type,
    "active_since": start_ts,
    "active_till": till,
    "description": f"monbot per-item maintenance for {name}",
    "hostids": [hostid],
    "timeperiods": [{
      "timeperiod_type": 0,
      "start_date": start_ts,
      "period": dur,
    }],
    "tags": [{
      "tag": tag_key,
      "operator": TAG_OPERATOR_CONTAINS,
      "value": name,
    }],
  }
  res = _api(zbx, "maintenance.create", params)
  print("Created:", res)
  return 0


def update_maintenance(
    zbx: ZabbixWeb,
    maintenanceid: str,
    start: Optional[str],
    duration: Optional[str],
    mtype: Optional[str],
) -> int:
  # We update active_since/active_till (timeperiods can be left; Zabbix uses them for schedule previews).
  m: Dict[str, Any] = {"maintenanceid": maintenanceid}
  if mtype is not None:
    m["maintenance_type"] = 0 if mtype == "with" else 1
  if start is not None:
    start_ts = parse_when(start)
    m["active_since"] = start_ts
    if duration:
      dur = parse_duration(duration, default_seconds=24 * 3600)
      m["active_till"] = start_ts + dur
  elif duration is not None:
    # If only duration is given, we need existing active_since to compute till
    cur = _api(zbx, "maintenance.get", {"maintenanceids": [maintenanceid], "output": ["active_since", "active_till"]})
    if not cur:
      raise ValueError("Maintenance not found")
    start_ts = int(cur[0].get("active_since", int(time.time())))
    dur = parse_duration(duration, default_seconds=24 * 3600)
    m["active_till"] = start_ts + dur

  res = _api(zbx, "maintenance.update", m)
  print("Updated:", res)
  return 0


def end_now(zbx: ZabbixWeb, maintenanceid: str) -> int:
  now = int(time.time())
  res = _api(zbx, "maintenance.update", {"maintenanceid": maintenanceid, "active_till": now})
  print("Ended:", res)
  return 0


def delete_maintenance(zbx: ZabbixWeb, maintenanceid: str) -> int:
  res = _api(zbx, "maintenance.delete", [maintenanceid])
  print("Deleted:", res)
  return 0


def main(argv=None) -> int:
  p = argparse.ArgumentParser(description="Per-item maintenance (tag-filtered) management for Zabbix")
  sub = p.add_subparsers(dest="cmd", required=True)

  sp_ls = sub.add_parser("list", help="List maintenances")
  sp_ls.add_argument("--hostid", help="Filter by hostid")
  sp_ls.add_argument("--name", help="Filter by maintenance name")

  sp_en = sub.add_parser("ensure", help="Ensure a per-item maintenance exists (till 2038)")
  sp_en.add_argument("--itemid", required=True, help="Target itemid")
  sp_en.add_argument("--tag", default="channel", help="Maintenance tag key (default: channel)")

  sp_cr = sub.add_parser("create", help="Create a per-item maintenance window")
  sp_cr.add_argument("--itemid", required=True, help="Target itemid")
  sp_cr.add_argument("--start", default="now", help="Start datetime (now|epoch|YYYY-MM-DD HH:MM)")
  sp_cr.add_argument("--duration", default="1d", help="Duration (e.g., 1d, 2h30m, seconds)")
  sp_cr.add_argument("--type", dest="mtype", choices=["with", "without"], default="with",
                     help="with=with data collection")
  sp_cr.add_argument("--tag", default="channel", help="Maintenance tag key (default: channel)")

  sp_up = sub.add_parser("update", help="Update maintenance start/duration/type")
  sp_up.add_argument("--id", dest="maintenanceid", required=True, help="Maintenance ID")
  sp_up.add_argument("--start", help="New start (now|epoch|YYYY-MM-DD HH:MM)")
  sp_up.add_argument("--duration", help="New duration (1d, 2h, 90m, ...)")
  sp_up.add_argument("--type", dest="mtype", choices=["with", "without"], help="Change type")

  sp_end = sub.add_parser("end", help="End a maintenance now")
  sp_end.add_argument("--id", dest="maintenanceid", required=True, help="Maintenance ID")

  sp_del = sub.add_parser("delete", help="Delete a maintenance")
  sp_del.add_argument("--id", dest="maintenanceid", required=True, help="Maintenance ID")

  args = p.parse_args(argv)

  zbx = ZabbixWeb(
    server=ZABBIX_URL, username=ZABBIX_USER, password=ZABBIX_PASS,
    api_token=ZABBIX_API_TOKEN, verify=ZABBIX_VERIFY_SSL
  )
  zbx.login()

  if args.cmd == "list":
    return list_maintenances(zbx, args.hostid, args.name)
  if args.cmd == "ensure":
    return ensure_item_maintenance(zbx, args.itemid, args.tag)
  if args.cmd == "create":
    return create_item_maintenance(zbx, args.itemid, args.start, args.duration, args.mtype, args.tag)
  if args.cmd == "update":
    return update_maintenance(zbx, args.maintenanceid, args.start, args.duration, args.mtype)
  if args.cmd == "end":
    return end_now(zbx, args.maintenanceid)
  if args.cmd == "delete":
    return delete_maintenance(zbx, args.maintenanceid)
  return 1


if __name__ == "__main__":
  raise SystemExit(main())
