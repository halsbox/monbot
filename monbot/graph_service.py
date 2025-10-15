from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Any, Dict, List, Optional, Set, Tuple

from monbot.cache2 import CacheResult, ImageCache2
from monbot.render import SkiaRenderer
from monbot.utils import PALETTE_20, natural_key
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

  def build_overview_signature(self, host_graphids: List[str]) -> GraphSignature:
    """
    Combine items from all graphs of a host into a single synthetic signature.
    Deduplicate by itemid; assign distinct colors from a stable palette.
    """
    items_map: Dict[str, GraphItemSig] = {}
    sort_order = 0
    for gid in host_graphids:
      sig = self._get_sig_cached(gid)
      for it in sig.items:
        if it.itemid in items_map:
          continue
        items_map[it.itemid] = GraphItemSig(
          itemid=it.itemid,
          color=it.color,  # temporary; will recolor below
          calc_fnc=it.calc_fnc,
          drawtype=it.drawtype,
          sortorder=sort_order,
          name=it.name,
          units=it.units,
          value_type=it.value_type,
        )
        sort_order += 1

    # Recolor deterministically by the insertion order (sortorder asc)
    items_seq = list(items_map.values())
    items_seq.sort(key=lambda gi: natural_key(gi.name))
    # recolor in this sorted order
    recolored: List[GraphItemSig] = []
    for idx, gi in enumerate(items_seq):
      new_color = PALETTE_20[idx % len(PALETTE_20)]
      recolored.append(replace(gi, color=new_color))
    items = tuple(recolored)
    return GraphSignature(
      graphid=f"ov:{hashlib.sha1('|'.join(host_graphids).encode()).hexdigest()}",
      name="Overview",
      items=items,
    )

  def build_signature_from_items(self, hostid: str, items: List[Tuple[str, str, str]]) -> GraphSignature:
    # items: [(itemid, name, color)]
    sig_items: List[GraphItemSig] = []
    for idx, (itemid, name, color) in enumerate(items):
      sig_items.append(GraphItemSig(
        itemid=itemid, color=color, calc_fnc=2, drawtype=0, sortorder=idx,
        name=name, units="°C", value_type=0
      ))
    return GraphSignature(graphid=f"ovitems:{hostid}", name="Overview", items=tuple(sig_items))

  async def get_item_media_from_item(
      self, hostid: str, itemid: str, name: str, color: str, units: str,
      period_label: str, width: int, height: int
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
    trig_lines = [(tl.value, tl.priority) for tl in trig_list] if trig_list else None

    key_parts = {
      "k": "item",
      "hostid": hostid,
      "itemid": itemid,
      "sig": shash,
      "period": period_label,
      "to": t_to,
      "size": f"{width}x{height}",
      "rv": "r7",
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
        trigger_lines=trig_lines,
      )

    result = await self.cache.get_or_fetch(key_parts, ttl, producer)
    return key_parts, ttl, result

  async def get_overview_media(
      self,
      hostid: str,
      host_graphids: List[str],
      period_label: str,
      width: int,
      height: int,
  ) -> Tuple[Dict[str, Any], int, CacheResult]:
    # Align window
    t_from, t_to, step = align_window(period_label)
    ttl = step

    # Build synthetic signature combining all host items
    ov_sig = self.build_overview_signature(host_graphids)

    sig_items: List[Tuple[str, str, int, int, int, str, str]] = []
    sig_items_key: List[Tuple[str, str, int, int, int]] = []
    itemids: List[str] = []
    for it in ov_sig.items:
      sig_items.append((it.itemid, it.color, it.calc_fnc, it.drawtype, it.sortorder, it.name, it.units))
      sig_items_key.append((it.itemid, it.color, it.calc_fnc, it.drawtype, it.sortorder))
      itemids.append(it.itemid)

    shash = self._sig_hash(ov_sig.graphid, sig_items_key)

    # Triggers for these items (only those with units will be rendered)
    trig_list = self.zbx.get_trigger_lines_for_items(itemids)
    items_with_units = {iid for (iid, _, _, _, _, _, units) in sig_items if units}
    trig_lines_render: List[Tuple[float, int]] = []
    trig_lines_key: List[Tuple[str, float, int]] = []
    seen_trig_values: Set[float] = set()
    for tl in trig_list:
      if (tl.itemid in items_with_units) and (tl.value not in seen_trig_values):
        seen_trig_values.add(tl.value)
        trig_lines_render.append((tl.value, tl.priority))
        trig_lines_key.append((tl.itemid, tl.value, tl.priority))
    thr_hash = self._trig_hash(trig_lines_key) if trig_lines_key else ""

    key_parts = {
      "k": "overview",
      "hostid": hostid,
      "sig": shash,
      "period": period_label,
      "to": t_to,
      "size": f"{width}x{height}",
      "rv": "r5",
      "thr": thr_hash,
    }

    def producer() -> bytes:
      series = self.zbx.fetch_series(ov_sig, t_from, t_to)
      envs = downsample_for_width(ov_sig, series, t_from, t_to, width)
      image = self.renderer.render_png(
        sig_graphid=ov_sig.graphid,
        series_list=sig_items,
        envelopes=envs,
        t_from=t_from,
        t_to=t_to,
        width=width,
        height=height,
        trigger_lines=trig_lines_render,
      )
      return image

    result = await self.cache.get_or_fetch(key_parts, ttl, producer)
    return key_parts, ttl, result

  async def get_media(
      self,
      graphid: str,
      period_label: str,
      width: int,
      height: int,
  ) -> Tuple[Dict[str, Any], int, CacheResult]:
    t_from, t_to, step = align_window(period_label)
    ttl = step

    sig = self._get_sig_cached(graphid)
    items_sorted = sorted(sig.items, key=lambda it: natural_key(it.name))
    sig_items: List[Tuple[str, str, int, int, int, str, str]] = []
    sig_items_key: List[Tuple[str, str, int, int, int]] = []
    itemids: List[str] = []
    for it in items_sorted:
      sig_items.append((it.itemid, it.color, it.calc_fnc, it.drawtype, it.sortorder, it.name, it.units))
      sig_items_key.append((it.itemid, it.color, it.calc_fnc, it.drawtype, it.sortorder))
      itemids.append(it.itemid)

    shash = self._sig_hash(sig.graphid, sig_items_key)

    # Fetch trigger thresholds for these items
    trig_list = self.zbx.get_trigger_lines_for_items(itemids)  # returns List[TriggerLine]
    # Filter to items with non-empty units (render filters too, but keep cache deterministic)
    items_with_units = {iid for (iid, _, _, _, _, _, units) in sig_items if units}
    trig_lines_render: List[Tuple[float, int]] = []
    trig_lines_key: List[Tuple[str, float, int]] = []
    for tl in trig_list:
      if tl.itemid in items_with_units:
        trig_lines_render.append((tl.value, tl.priority))
        trig_lines_key.append((tl.itemid, tl.value, tl.priority))

    thr_hash = self._trig_hash(trig_lines_key) if trig_lines_key else ""

    key_parts = {
      "k": "graph",
      "sig": shash,
      "graphid": sig.graphid,
      "period": period_label,
      "to": t_to,
      "size": f"{width}x{height}",
      "rv": "r3",  # bump renderer version due to trigger lines
      "thr": thr_hash,  # triggers affect image
    }

    def producer() -> bytes:
      series = self.zbx.fetch_series(sig, t_from, t_to)
      envs = downsample_for_width(sig, series, t_from, t_to, width)
      image = self.renderer.render_png(
        sig_graphid=sig.graphid,
        series_list=sig_items,
        envelopes=envs,
        t_from=t_from,
        t_to=t_to,
        width=width,
        height=height,
        trigger_lines=trig_lines_render,
      )
      return image

    result = await self.cache.get_or_fetch(key_parts, ttl, producer)
    return key_parts, ttl, result

  async def prefetch(self, graphid: str, period_label: str, width: int, height: int):
    try:
      await self.get_media(graphid, period_label, width, height)
    except Exception:
      # Silent prefetch failure; avoid spamming logs in handler path
      pass

  async def get_overview_media_from_items(
      self, hostid: str, item_triplets: List[Tuple[str, str, str]], period_label: str, width: int, height: int
  ):
    t_from, t_to, step = align_window(period_label)
    ttl = step
    sig = self.build_signature_from_items(hostid, item_triplets)
    sig_items = [(it.itemid, it.color, it.calc_fnc, it.drawtype, it.sortorder, it.name, it.units) for it in sig.items]
    sig_items_key = [(it.itemid, it.color, it.calc_fnc, it.drawtype, it.sortorder) for it in sig.items]
    shash = self._sig_hash(sig.graphid, sig_items_key)
    key_parts = {
      "k": "overview_items", "hostid": hostid, "sig": shash, "period": period_label, "to": t_to,
      "size": f"{width}x{height}", "rv": "r6",
    }

    def producer() -> bytes:
      series = self.zbx.fetch_series(sig, t_from, t_to)
      envs = downsample_for_width(sig, series, t_from, t_to, width)
      return self.renderer.render_png(sig_graphid=sig.graphid, series_list=sig_items, envelopes=envs,
                                      t_from=t_from, t_to=t_to, width=width, height=height, trigger_lines=None)

    result = await self.cache.get_or_fetch(key_parts, ttl, producer)
    return key_parts, ttl, result
