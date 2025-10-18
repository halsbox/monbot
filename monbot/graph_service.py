from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from monbot.cache2 import CacheResult, ImageCache2
from monbot.config import IMAGE_CACHE_RV
from monbot.items_index import ItemInfo
from monbot.render import SkiaRenderer
from monbot.zbx_data import GraphItemSig, GraphSignature, ZbxDataClient, align_window, downsample_for_width


class GraphService:
  def __init__(self, zbx_client: ZbxDataClient, cache: ImageCache2, renderer: Optional[SkiaRenderer] = None):
    self.zbx = zbx_client
    self.cache = cache
    self.renderer = renderer or SkiaRenderer()
    self._sig_cache: Dict[str, GraphSignature] = {}

  def _get_sig_cached(self, graphid: str) -> GraphSignature:
    sig = self._sig_cache.get(graphid)
    if sig is None:
      sig = self.zbx.get_graph_signature(graphid)
      self._sig_cache[graphid] = sig
    return sig

  def clear_signature_cache(self):
    self._sig_cache.clear()

  @staticmethod
  def _sig_hash(graphid: str, items: List[Tuple[str, str, int, int, int]]) -> str:
    h = hashlib.sha1()
    h.update(graphid.encode("utf-8"))
    for it in items:
      h.update(("|".join(map(str, it))).encode("utf-8"))
      h.update(b";")
    return h.hexdigest()

  @staticmethod
  def _trig_hash(lines: List[Tuple[str, float, int]]) -> str:
    # lines as (itemid, value, priority), sorted for stability
    h = hashlib.sha1()
    for iid, v, p in sorted(lines, key=lambda t: (t[0], t[1], t[2])):
      h.update(f"{iid}:{v:.6f}:{p};".encode("utf-8"))
    return h.hexdigest()

  @staticmethod
  def build_signature_from_items(hostid: str, items: List[ItemInfo]) -> GraphSignature:
    sig_items: list[GraphItemSig] = []
    for idx, it in enumerate(items):
      sig_items.append(GraphItemSig(
        itemid=it.itemid, color=it.color, calc_fnc=2, drawtype=0, sortorder=idx,
        name=it.name, units=it.units or "", value_type=0
      ))
    return GraphSignature(graphid=f"ovitems:{hostid}", name="Overview", items=tuple(sig_items))

  async def get_item_media_from_item(
      self, hostid: str, itemid: str, name: str, color: str, units: str,
      period_label: str, width: int, height: int, tz: ZoneInfo = ZoneInfo("UTC"),
  ) -> Tuple[Dict[str, Any], int, CacheResult]:
    t_from, t_to, step = align_window(period_label)
    ttl = step
    # one-item signature
    sig = GraphSignature(
      graphid=f"item:{itemid}",
      name=name,
      items=(
        GraphItemSig(itemid=itemid, color=color, calc_fnc=2, drawtype=0, sortorder=0, name=name, units=units or "°C",
                     value_type=0),)
    )
    sig_items = [(itemid, color, 2, 0, 0, name, units or "°C")]
    sig_items_key = [(itemid, color, 2, 0, 0)]
    shash = self._sig_hash(sig.graphid, sig_items_key)

    # Optional: trigger lines for this one item (use configured tag)
    trig_list = self.zbx.get_trigger_lines_for_items([itemid])
    trig_lines_key: List[Tuple[str, float, int]] = []
    trig_lines_render: List[Tuple[float, int]] = []
    for tl in trig_list:
      trig_lines_render.append((tl.value, tl.priority))
      trig_lines_key.append((tl.itemid, tl.value, tl.priority))

    thr_hash = self._trig_hash(trig_lines_key) if trig_lines_key else ""

    key_parts = {
      "k": "item",
      "hostid": hostid,
      "itemid": itemid,
      "sig": shash,
      "period": period_label,
      "to": t_to,
      "size": f"{width}x{height}",
      "thr": thr_hash,
      "tz": str(tz),
      "rv": IMAGE_CACHE_RV,
    }

    def producer() -> bytes:
      series = self.zbx.fetch_series(sig, t_from, t_to)
      envs = downsample_for_width(sig, series, t_from, t_to, width)
      return self.renderer.render_png(
        sig_graphid=sig.graphid,
        series_list=sig_items,
        envelopes=envs,
        t_from=t_from,
        t_to=t_to,
        width=width,
        height=height,
        trigger_lines=trig_lines_render,
        tz=tz,
      )

    result = await self.cache.get_or_produce(key_parts, ttl, producer)
    return key_parts, ttl, result

  async def get_overview_media_from_items(
      self, hostid: str, items: List[ItemInfo], period_label: str, width: int, height: int, tz: ZoneInfo = ZoneInfo("UTC"),
  ) -> tuple[dict[str, str | int], int, CacheResult]:
    t_from, t_to, step = align_window(period_label)
    ttl = step
    graph_sig = self.build_signature_from_items(hostid, items)
    graph_sig_items: Tuple[GraphItemSig, ...] = graph_sig.items
    sig_items = [(it.itemid, it.color, it.calc_fnc, it.drawtype, it.sortorder, it.name, it.units) for it in graph_sig_items]
    sig_items_key = [(it.itemid, it.color, it.calc_fnc, it.drawtype, it.sortorder) for it in graph_sig_items]
    shash = self._sig_hash(graph_sig.graphid, sig_items_key)
    key_parts = {
      "k": "overview",
      "hostid": hostid,
      "sig": shash,
      "period": period_label,
      "to": t_to,
      "size": f"{width}x{height}",
      "tz": str(tz),
      "rv": IMAGE_CACHE_RV,
    }

    def producer() -> bytes:
      series = self.zbx.fetch_series(graph_sig, t_from, t_to)
      envs = downsample_for_width(graph_sig, series, t_from, t_to, width)
      return self.renderer.render_png(sig_graphid=graph_sig.graphid, series_list=sig_items, envelopes=envs,
                                      t_from=t_from, t_to=t_to, width=width, height=height, trigger_lines=None, tz=tz)

    result = await self.cache.get_or_produce(key_parts, ttl, producer)
    return key_parts, ttl, result
