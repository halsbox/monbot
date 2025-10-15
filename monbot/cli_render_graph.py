from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import List, Set, Tuple

import numpy as np
from dotenv import find_dotenv, load_dotenv

from monbot.utils import fmt_ts

load_dotenv(find_dotenv(), override=False)

from monbot.config import (
  ZABBIX_URL,
  ZABBIX_USER,
  ZABBIX_PASS,
  ZABBIX_API_TOKEN,
  ZABBIX_VERIFY_SSL,
)
from monbot.zabbix import ZabbixWeb
from monbot.zbx_data import ZbxDataClient, align_window, downsample_for_width, GraphSignature, GraphItemSig, \
  _estimate_sample_interval


def _print_debug_series(graph_items, series: dict, tf: int, tt: int, width: int):
  period = tt - tf
  base_bucket = max(1, int(period / max(1, width)))
  print(
    f"DEBUG: window [{fmt_ts(tf)} .. {fmt_ts(tt)}], period={period}s, target_width={width}, base_bucket_seconds={base_bucket}s")

  for it in graph_items:
    s = series.get(it.itemid, {})
    if not s:
      print(f"  itemid={it.itemid} name={it.name!r} units={it.units!r}: NO RAW DATA")
      continue
    is_trend = "value_min" in s
    clock = s["clock"]
    npts = int(clock.size)
    cadence = _estimate_sample_interval(clock, is_trend=is_trend)
    tmin = int(clock.min()) if npts else None
    tmax = int(clock.max()) if npts else None
    print(f"  itemid={it.itemid} name={it.name!r} units={it.units!r} type={'trend' if is_trend else 'history'} "
          f"raw_points={npts} clock=[{fmt_ts(tmin) if tmin else '-'} .. {fmt_ts(tmax) if tmax else '-'}] "
          f"sample_interval≈{cadence}s "
          f"chosen_bucket_seconds>=max(base,{cadence // 2})")

  print("DEBUG END RAW")


def _print_debug_downsample(graph_items, ds: dict, tf: int, tt: int, width: int):
  print(f"DEBUG DS: target_width={width}")
  for it in graph_items:
    env = ds.get(it.itemid)
    if not env:
      print(f"  itemid={it.itemid} name={it.name!r}: NO DS")
      continue
    y = env["y_avg"]
    if y.size == 0:
      print(f"  itemid={it.itemid} name={it.name!r}: DS EMPTY")
      continue
    fin = np.isfinite(y)
    nfin = int(fin.sum())
    if nfin:
      ysel = y[fin]
      print(
        f"  itemid={it.itemid} name={it.name!r}: finite={nfin}/{y.size} y_avg[min={float(np.nanmin(ysel)):.6g}, max={float(np.nanmax(ysel)):.6g}]")
    else:
      print(f"  itemid={it.itemid} name={it.name!r}: finite=0/{y.size} (all NaN)")
  print("DEBUG END DS")


def _probe_direct(zbx: ZabbixWeb, itemids: list[str], tf: int, tt: int, mode: str):
  # mode: 'trend' or 'history' (history tries both float(0) and unsigned(3))
  if mode == "trend":
    for vtype in (0, 3):
      res = zbx.api_request("trend.get", {
        "output": ["itemid", "clock", "num", "value_min", "value_avg", "value_max"],
        "trend": vtype,
        "itemids": itemids,
        "time_from": tf,
        "time_till": tt,
        "sortfield": "clock",
        "sortorder": "ASC",
      })
      print(f"PROBE trend type={vtype}: rows={len(res)}")
  elif mode == "history":
    for vtype in (0, 3):
      res = zbx.api_request("history.get", {
        "output": ["itemid", "clock", "value"],
        "history": vtype,
        "itemids": itemids,
        "time_from": tf,
        "time_till": tt,
        "sortfield": "clock",
        "sortorder": "ASC",
      })
      print(f"PROBE history type={vtype}: rows={len(res)}")
  else:
    print("Unknown probe mode:", mode)


def _overview_palette() -> list[str]:
  return [
    "1f77b4", "ff7f0e", "2ca02c", "d62728", "9467bd",
    "8c564b", "e377c2", "7f7f7f", "bcbd22", "17becf",
    "393b79", "637939", "8c6d31", "843c39", "7b4173",
    "3182bd", "e6550d", "31a354", "dd1c77", "756bb1",
  ]


def _build_overview_signature(zbx: ZabbixWeb, client: ZbxDataClient, hostid: str) -> tuple[GraphSignature, str]:
  # Get all graphids for the host (we only need ids and host name)
  graphs = zbx.api_request("graph.get", {
    "output": ["graphid"],
    "hostids": [hostid],
    "selectHosts": ["hostid", "name"],
  })
  if not graphs:
    return GraphSignature(graphid=f"ov:{hostid}", name="Overview", items=()), ""
  host_name = ""
  if graphs[0].get("hosts"):
    host_name = graphs[0]["hosts"][0].get("name", "")
  # Collect/dedup items across all host graphs
  items_map: dict[str, GraphItemSig] = {}
  order = 0
  for g in graphs:
    gid = str(g["graphid"])
    sig = client.get_graph_signature(gid)
    for it in sig.items:
      if it.itemid in items_map:
        continue
      items_map[it.itemid] = GraphItemSig(
        itemid=it.itemid,
        color=it.color,  # will recolor below
        calc_fnc=it.calc_fnc,
        drawtype=it.drawtype,
        sortorder=order,
        name=it.name,
        units=it.units,
        value_type=it.value_type,
      )
      order += 1
  # Recolor deterministically by sequence index
  pal = _overview_palette()
  recolored: list[GraphItemSig] = []
  i = 0
  for it in items_map.values():
    recolored.append(GraphItemSig(
      itemid=it.itemid,
      color=pal[i % len(pal)],
      calc_fnc=it.calc_fnc,
      drawtype=it.drawtype,
      sortorder=it.sortorder,
      name=it.name,
      units=it.units,
      value_type=it.value_type,
    ))
    i += 1
  sig = GraphSignature(graphid=f"ov:{hostid}", name="Overview", items=tuple(recolored))
  return sig, host_name


def main(argv=None) -> int:
  p = argparse.ArgumentParser(description="Render a Zabbix graph via API data to JPEG")
  g = p.add_mutually_exclusive_group(required=True)
  g.add_argument("--graphid", help="Zabbix graphid")
  g.add_argument("--hostid", help="Zabbix hostid for overview")
  p.add_argument("--period", required=True, choices=["15m", "30m", "1h", "3h", "6h", "12h", "24h", "48h", "1w"])
  p.add_argument("--width", type=int, default=360, help="Image width")
  p.add_argument("--height", type=int, default=240, help="Image height")
  p.add_argument("--quality", type=int, default=82, help="JPEG quality 1-100")
  p.add_argument("--out", type=Path, default=Path("graph.jpg"))
  p.add_argument("--debug", action="store_true", help="Print detailed debug info and probes")
  p.add_argument("--probe-trend", action="store_true", help="Probe trend.get directly (debug)")
  p.add_argument("--probe-history", action="store_true", help="Probe history.get directly (debug)")
  args = p.parse_args(argv)

  t0 = time.time()

  zbx = ZabbixWeb(
    server=ZABBIX_URL,
    username=ZABBIX_USER,
    password=ZABBIX_PASS,
    api_token=ZABBIX_API_TOKEN,
    verify=ZABBIX_VERIFY_SSL,
  )
  zbx.login()
  t_login = time.time()

  client = ZbxDataClient(zbx)
  if args.hostid:
    sig, host_name = _build_overview_signature(zbx, client, args.hostid)
  else:
    sig = client.get_graph_signature(args.graphid)
    host_name = ""
  t_sig = time.time()

  tf, tt, align_step = align_window(args.period)

  # Debug: probe raw data if requested
  itemids = [it.itemid for it in sig.items]
  if args.debug and args.probe_trend:
    _probe_direct(zbx, itemids, tf, tt, mode="trend")
  if args.debug and args.probe_history:
    _probe_direct(zbx, itemids, tf, tt, mode="history")

  series = client.fetch_series(sig, tf, tt)
  t_fetch = time.time()

  if args.debug:
    _print_debug_series(sig.items, series, tf, tt, args.width)

  ds = downsample_for_width(sig, series, tf, tt, args.width)
  t_ds = time.time()

  if args.debug:
    _print_debug_downsample(sig.items, ds, tf, tt, args.width)

  trig_objs = client.get_trigger_lines_for_items(itemids)
  seen_trig_values: Set[float] = set()
  trig_lines: List[Tuple[float, int]] = []
  for t in trig_objs:
    if t.value not in seen_trig_values:
      seen_trig_values.add(t.value)
      trig_lines.append((t.value, t.priority))

  # Prepare renderer inputs
  from render import SkiaRenderer
  renderer = SkiaRenderer()

  series_list: List[Tuple[str, str, int, int, int, str, str]] = []
  for it in sig.items:
    series_list.append((it.itemid, it.color, it.calc_fnc, it.drawtype, it.sortorder, it.name, it.units))

  image = renderer.render_png(
    sig_graphid=sig.graphid,
    series_list=series_list,
    envelopes=ds,
    t_from=tf,
    t_to=tt,
    width=args.width,
    height=args.height,
    trigger_lines=trig_lines,
  )
  t_render = time.time()

  args.out.write_bytes(image)

  t1 = time.time()
  print(
    "login={:.1f}ms sig={:.1f}ms fetch={:.1f}ms downsample={:.1f}ms render+encode={:.1f}ms total={:.1f}ms "
    "size={:.1f}KB align_step={}s".format(
      1000 * (t_login - t0),
      1000 * (t_sig - t_login),
      1000 * (t_fetch - t_sig),
      1000 * (t_ds - t_fetch),
      1000 * (t_render - t_ds),
      1000 * (t1 - t0),
      len(image) / 1024.0,
      align_step,
    )
  )
  print(f"Wrote {args.out}")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
