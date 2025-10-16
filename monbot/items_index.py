from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from monbot.utils import PALETTE_20, natural_key
from monbot.zabbix import ZabbixWeb


@dataclass(frozen=True)
class ItemInfo:
  itemid: str
  hostid: str
  name: str
  units: str
  color: str  # hex without '#'


class ItemsIndex:
  def __init__(self, zbx: ZabbixWeb, allow_hosts: Dict[str, str]):
    self._zbx = zbx
    self._allow_hosts = allow_hosts  # hostid -> display name
    self._host_items: Dict[str, List[ItemInfo]] = {}  # hostid -> items

  async def refresh(self):
    items = await asyncio.to_thread(self._zbx.get_items, self._allow_hosts.keys())
    by_host: Dict[str, List[Tuple[str, str, str]]] = {hid: [] for hid in self._allow_hosts.keys()}
    for it in items or []:
      hostid = str(it.get("hostid") or "")
      if hostid not in by_host:
        continue
      name = str(it.get("name") or "")
      units = str(it.get("units") or "")
      itemid = str(it.get("itemid") or "")
      if not units:
        continue
      by_host[hostid].append((name, itemid, units))

    new_map: Dict[str, List[ItemInfo]] = {}
    for hostid, lst in by_host.items():
      lst.sort(key=lambda t: natural_key(t[0]))
      host_items: List[ItemInfo] = []
      for idx, (name, itemid, units) in enumerate(lst):
        color = PALETTE_20[idx % len(PALETTE_20)]
        host_items.append(ItemInfo(itemid=itemid, hostid=hostid, name=name, units=units, color=color))
      new_map[hostid] = host_items
    self._host_items = new_map

  def items_by_host_name(self, host_name: str) -> List[ItemInfo]:
    hostid = next((hid for hid, disp in self._allow_hosts.items() if disp == host_name), None)
    if not hostid:
      return []
    return self._host_items.get(hostid, [])

  def items_by_hostid(self, hostid: str) -> List[ItemInfo]:
    return self._host_items.get(hostid, [])

  def hostid_by_name(self, host_name: str) -> str | None:
    return next((hid for hid, disp in self._allow_hosts.items() if disp == host_name), None)

  def get_item_name(self, itemid: Any) -> str | None:
    info = self.get_item(itemid)
    return info.name if info else None

  def get_item(self, itemid: Any) -> Optional[ItemInfo]:
    iid = str(itemid)
    for host_items in self._host_items.values():
      for item in host_items:
        if item.itemid == iid:
          return item
    return None

  def host_name_by_hostid(self, hostid: str) -> Optional[str]:
    return self._allow_hosts.get(str(hostid))
