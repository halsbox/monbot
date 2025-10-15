from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

import numpy as np
from dotenv import find_dotenv, load_dotenv

# Load .env so config picks env vars
load_dotenv(find_dotenv(), override=False)

from monbot.config import (
  ZABBIX_URL,
  ZABBIX_USER,
  ZABBIX_PASS,
  ZABBIX_API_TOKEN,
  ZABBIX_VERIFY_SSL,
)
from monbot.zabbix import ZabbixWeb
from monbot.zbx_data import ZbxDataClient, align_window, downsample_for_width


def _np_to_list(arr: np.ndarray) -> list:
  # Convert numpy arrays to vanilla lists with NaN as None for JSON
  if np.issubdtype(arr.dtype, np.integer):
    return [int(x) for x in arr.tolist()]
  out = []
  for x in arr.tolist():
    if isinstance(x, float) and (np.isnan(x) or np.isinf(x)):
      out.append(None)
    else:
      out.append(float(x))
  return out


def main(argv=None) -> int:
  p = argparse.ArgumentParser(description="Dump downsampled Zabbix graph envelopes to JSON")
  p.add_argument("--graphid", required=True, help="Zabbix graphid")
  p.add_argument("--period", required=True, choices=["15m", "30m", "1h", "3h", "6h", "12h", "24h", "1w"])
  p.add_argument("--width", type=int, default=360, help="Canvas width in pixels (number of buckets)")
  p.add_argument("--out", type=Path, default=Path("graph_dump.json"))
  args = p.parse_args(argv)

  zbx = ZabbixWeb(
    server=ZABBIX_URL,
    username=ZABBIX_USER,
    password=ZABBIX_PASS,
    api_token=ZABBIX_API_TOKEN,
    verify=ZABBIX_VERIFY_SSL,
  )
  zbx.login()

  client = ZbxDataClient(zbx)
  sig = client.get_graph_signature(args.graphid)
  t_from, t_to, step = align_window(args.period)

  series = client.fetch_series(sig, t_from, t_to)
  ds = downsample_for_width(sig, series, t_from, t_to, args.width)

  # Compose JSON
  out: Dict[str, Any] = {
    "graphid": sig.graphid,
    "name": sig.name,
    "period": args.period,
    "t_from": t_from,
    "t_to": t_to,
    "width": args.width,
    "items": [],
  }
  for it in sig.items:
    env = ds.get(it.itemid)
    if not env:
      continue
    out["items"].append(
      {
        "itemid": it.itemid,
        "name": it.name,
        "units": it.units,
        "color": it.color,
        "calc_fnc": it.calc_fnc,
        "drawtype": it.drawtype,
        "value_type": it.value_type,
        "y_min": _np_to_list(env["y_min"]),
        "y_max": _np_to_list(env["y_max"]),
        "y_avg": _np_to_list(env["y_avg"]),
        "count": _np_to_list(env["count"]),
      }
    )

  args.out.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":"), indent=2))
  print(f"Wrote {args.out} (items={len(out['items'])}, period={args.period}, window=[{t_from},{t_to}))")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
