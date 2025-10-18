from __future__ import annotations

import dataclasses
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import numpy.typing as npt

from monbot.downsample import downsample_history, downsample_trend
from monbot.zabbix import ZabbixWeb

# Value types we support numerically: 0=float, 3=unsigned
NUMERIC_VALUE_TYPES = {0, 3}

# Alignment step per period label (seconds)
ALIGNMENT_STEPS = {
  "15m": 60,
  "30m": 60,
  "1h": 60,
  "3h": 300,
  "6h": 300,
  "12h": 300,
  "24h": 300,
  "48h": 300,
  "1w": 900,
}

PERIOD_SECONDS = {
  "15m": 15 * 60,
  "30m": 30 * 60,
  "1h": 60 * 60,
  "3h": 3 * 60 * 60,
  "6h": 6 * 60 * 60,
  "12h": 12 * 60 * 60,
  "24h": 24 * 60 * 60,
  "48h": 48 * 60 * 60,
  "1w": 7 * 24 * 60 * 60,
}

TREND_CUTOVER_SEC = 48 * 60 * 60  # >48h -> trend.get


@dataclasses.dataclass(frozen=True)
class GraphItemSig:
  itemid: str
  color: str
  calc_fnc: int  # 1=min, 2=avg, 4=max (from Zabbix)
  drawtype: int  # optional render hints
  sortorder: int
  name: str
  units: str
  value_type: int  # 0 float, 3 unsigned


@dataclasses.dataclass(frozen=True)
class GraphSignature:
  graphid: str
  name: str
  items: Tuple[GraphItemSig, ...]  # ordered by sortorder ascending

def fmt_dt_chart2(tz: ZoneInfo, ts: int) -> str:
  return datetime.fromtimestamp(ts, tz).strftime("%Y-%m-%d %H:%M:%S")

def fmt_uptime(total_seconds: int|float, locale: str = "ru") -> str:
  """
  Convert seconds -> "N days HH:MM:SS" with simple i18n for 'ru' and 'en'.
  Example (ru): 18945480 -> "219 дней 06:38:00"
  """
  secs = max(0, int(total_seconds))
  days = secs // 86400
  rem = secs % 86400
  hh = rem // 3600
  mm = (rem % 3600) // 60
  ss = rem % 60

  if locale.lower().startswith("ru"):
    # Russian plural forms: день, дня, дней
    def ru_day_word(n: int) -> str:
      n_abs = abs(n)
      if n_abs % 10 == 1 and n_abs % 100 != 11:
        return "день"
      if 2 <= n_abs % 10 <= 4 and not (12 <= n_abs % 100 <= 14):
        return "дня"
      return "дней"

    day_part = f"{days} {ru_day_word(days)}"
  else:
    day_part = f"{days} {'day' if days == 1 else 'days'}"

  return f"{day_part}, {hh:02d}:{mm:02d}:{ss:02d}"

def _estimate_sample_interval(clock: npt.NDArray[np.int64], is_trend: bool) -> int:
  if is_trend:
    return 3600  # Zabbix trends are hourly aggregates
  if clock.size < 2:
    return 300  # fallback: 5 minutes
  diffs = np.diff(clock)
  diffs = diffs[diffs > 0]
  if diffs.size == 0:
    return 300
  med = int(np.median(diffs))
  # clamp to sane range
  return max(1, min(med, 24 * 3600))


def _upscale_to_width(arr: npt.NDArray[np.float64], target_w: int) -> npt.NDArray[np.float64]:
  """Nearest-neighbor resample to target width. Assumes arr.ndim == 1."""
  src_w = int(arr.shape[0])
  if src_w <= 0 or target_w <= 0:
    return np.empty((0,), dtype=np.float64)
  if src_w == target_w:
    return arr
  idx = (np.linspace(0, src_w - 1, num=target_w)).astype(np.int32)
  return arr[idx]


def _upscale_to_width_linear(arr: npt.NDArray[np.float64], target_w: int) -> npt.NDArray[np.float64]:
  """
  Linear interpolation to target width.
  - Interpolates only across finite points.
  - If fewer than 2 finite points, falls back to nearest-neighbor.
  Note: Does not mask gaps; caller should set NaNs for long gaps and/or apply bounded interpolation.
  """
  src_w = int(arr.shape[0])
  if src_w <= 0 or target_w <= 0:
    return np.empty((0,), dtype=np.float64)
  if src_w == target_w:
    return arr

  src_idx = np.arange(src_w, dtype=np.float64)
  dst_idx = np.linspace(0, src_w - 1, num=target_w, dtype=np.float64)

  mask = np.isfinite(arr)
  if mask.sum() >= 2:
    out = np.interp(dst_idx, src_idx[mask], arr[mask]).astype(np.float64, copy=False)
    return out
  else:
    # Nearest-neighbor as last resort
    nn_idx = (np.linspace(0, src_w - 1, num=target_w)).astype(np.int32)
    return arr[nn_idx]


def interpolate_small_gaps(y: npt.NDArray[np.float64], max_gap: int) -> tuple[
  npt.NDArray[np.float64], npt.NDArray[np.bool_]]:
  """
  Linearly interpolate NaN runs of length <= max_gap if bounded on both sides.
  Returns (y_filled, fill_mask).
  """
  y = y.astype(np.float64, copy=True)
  is_nan = ~np.isfinite(y)
  fill_mask = np.zeros_like(is_nan, dtype=bool)
  if not is_nan.any():
    return y, fill_mask
  idx = np.arange(y.shape[0])
  finite = np.isfinite(y)
  if finite.sum() < 2:
    return y, fill_mask
  y_interp = y.copy()
  y_interp[is_nan] = np.interp(idx[is_nan], idx[finite], y[finite])
  n = y.shape[0]
  i = 0
  while i < n:
    if is_nan[i]:
      j = i
      while j < n and is_nan[j]:
        j += 1
      run_len = j - i
      left_ok = i > 0 and np.isfinite(y[i - 1])
      right_ok = j < n and np.isfinite(y[j])  # j == n => open right end, do not fill
      if run_len <= max_gap and left_ok and right_ok:
        y[i:j] = y_interp[i:j]
        fill_mask[i:j] = True
      i = j
    else:
      i += 1
  return y, fill_mask


def _max_gap_buckets(sample_interval_sec: int, period_sec: int, bucket_seconds: int) -> int:
  """
  Compute max gap length in buckets to interpolate:
  - base on cadence (<= 2 * sample interval)
  - also cap to <= 25% of period
  - clamp overall to [60s, 900s] window for stability
  """
  if bucket_seconds <= 0:
    return 1
  g_sec = min(2 * max(1, sample_interval_sec), int(0.25 * period_sec))
  g_sec = max(60, min(g_sec, 900))
  return max(1, int(g_sec / bucket_seconds))


def parse_period(label: str) -> int:
  try:
    return PERIOD_SECONDS[label]
  except KeyError:
    raise ValueError(f"Unsupported period label: {label}")


def alignment_step_for_period(label: str) -> int:
  try:
    return ALIGNMENT_STEPS[label]
  except KeyError:
    raise ValueError(f"No alignment step for period: {label}")


def align_window(period_label: str, now: Optional[int] = None) -> Tuple[int, int, int]:
  """
  Returns (t_from, t_to, step), aligned per spec.
  """
  if now is None:
    now = int(time.time())
  step = alignment_step_for_period(period_label)
  period = parse_period(period_label)
  t_to = (now // step) * step
  t_from = t_to - period
  return t_from, t_to, step


def downsample_for_width(
    sig: GraphSignature,
    series: Dict[str, Dict[str, np.ndarray]],
    t_from: int,
    t_to: int,
    width: int
) -> Dict[str, Dict[str, np.ndarray]]:
  """
  Adaptive bucketing with bounded gap repair:
  - bucket_seconds >= period/width and >= sample_interval (per item) to minimize empty buckets.
  - Downsample to W_eff buckets.
  - Upscale to target width: y_avg -> linear interpolation, y_min/y_max -> NN.
  - After upscaling, mark long gaps as NaN (based on cadence) and fill only smaller runs.
  - For filled indices, set y_min=y_max=y_avg to avoid envelope bridging through gaps.
  """

  period_sec = max(1, int(t_to - t_from))
  base_bucket_seconds = max(1, int(period_sec / max(1, width)))

  out: Dict[str, Dict[str, np.ndarray]] = {}
  for it in sig.items:
    s = series.get(it.itemid)
    if not s:
      continue

    is_trend = "value_min" in s
    sample_interval = _estimate_sample_interval(s["clock"], is_trend=is_trend)

    # Key change: buckets no smaller than the sampling interval
    bucket_seconds = max(base_bucket_seconds, int(max(1, sample_interval)))
    w_eff = max(1, int(period_sec / bucket_seconds))

    # Downsample to effective width
    if "value" in s:
      y_min_eff, y_max_eff, y_avg_eff, count_eff = downsample_history(s["clock"], s["value"], t_from, t_to, w_eff)
    else:
      y_min_eff, y_max_eff, y_avg_eff, count_eff = downsample_trend(
        s["clock"], s["value_min"], s["value_avg"], s["value_max"], t_from, t_to, w_eff
      )

    # Upscale to target width
    y_min = _upscale_to_width(y_min_eff, width)
    y_max = _upscale_to_width(y_max_eff, width)
    y_avg_lin = _upscale_to_width_linear(y_avg_eff, width)

    # Build an upscaled presence mask from effective buckets to preserve long gaps
    eff_mask = np.isfinite(y_avg_eff)
    if w_eff == width:
      mask_up = eff_mask
    else:
      nn_idx = (np.linspace(0, max(1, w_eff) - 1, num=width)).astype(np.int32)
      mask_up = eff_mask[nn_idx]

    # Start with fully interpolated line, then impose NaNs for long gaps
    y_avg = y_avg_lin.copy()
    y_avg[~mask_up] = np.nan

    # Compute temporal gap threshold in destination buckets
    bucket_seconds_out = max(1, int(period_sec / max(1, width)))
    max_gap_buckets = _max_gap_buckets(sample_interval, period_sec, bucket_seconds_out)

    # Fill only small NaN runs (<= max_gap_buckets)
    y_avg_filled, filled_mask = interpolate_small_gaps(y_avg, max_gap_buckets)

    # Keep envelope faithful: where we filled, set y_min=y_max=y_avg
    if filled_mask.any():
      if y_min.size:
        y_min = y_min.copy()
        y_min[filled_mask] = y_avg_filled[filled_mask]
      if y_max.size:
        y_max = y_max.copy()
        y_max[filled_mask] = y_avg_filled[filled_mask]

    # Clamp y_avg to [y_min,y_max] where both finite
    clamp_mask = np.isfinite(y_min) & np.isfinite(y_max) & np.isfinite(y_avg_filled)
    if clamp_mask.any():
      y_avg_filled = y_avg_filled.copy()
      y_avg_filled[clamp_mask] = np.clip(y_avg_filled[clamp_mask], y_min[clamp_mask], y_max[clamp_mask])

    # Simple count: mark buckets with finite avg as "has data"
    count = np.where(np.isfinite(y_avg_filled), 1, 0).astype(np.int32)

    out[it.itemid] = {
      "y_min": y_min,
      "y_max": y_max,
      "y_avg": y_avg_filled,
      "count": count,
    }

  return out


class ZbxDataClient:
  def __init__(self, zbx: ZabbixWeb):
    self.zbx = zbx

  def get_graph_signature(self, graphid: str) -> GraphSignature:
    res = self.zbx.api_request(
      "graph.get",
      {
        "output": ["graphid", "name", "width", "height"],
        "graphids": [graphid],
        "selectGraphItems": "extend",
      },
    )
    if not res:
      raise RuntimeError(f"Graph {graphid} not found")
    g = res[0]
    gitems = g.get("gitems", [])

    # Gather itemids and sortorder
    itemids = [gi["itemid"] for gi in gitems]
    sortorder_map = {gi["itemid"]: int(gi.get("sortorder", 0)) for gi in gitems}
    color_map = {gi["itemid"]: gi.get("color", "000000") for gi in gitems}
    calc_map = {gi["itemid"]: int(gi.get("calc_fnc", 2)) for gi in gitems}
    draw_map = {gi["itemid"]: int(gi.get("drawtype", 0)) for gi in gitems}

    # Fetch item metadata
    items_meta = self.zbx.api_request(
      "item.get",
      {
        "output": ["itemid", "name", "value_type", "units"],
        "itemids": itemids,
      },
    )
    imap = {it["itemid"]: it for it in items_meta}

    sig_items: List[GraphItemSig] = []
    for itemid in itemids:
      it = imap.get(itemid)
      if not it:
        # Skip unknown item
        continue
      value_type = int(it.get("value_type", 0))
      sig_items.append(
        GraphItemSig(
          itemid=itemid,
          color=color_map.get(itemid, "000000"),
          calc_fnc=calc_map.get(itemid, 2),
          drawtype=draw_map.get(itemid, 0),
          sortorder=sortorder_map.get(itemid, 0),
          name=it.get("name", itemid),
          units=it.get("units", ""),
          value_type=value_type,
        )
      )

    # Order by sortorder
    sig_items.sort(key=lambda s: s.sortorder)
    return GraphSignature(graphid=str(g["graphid"]), name=str(g.get("name") or ""), items=tuple(sig_items))

  def _fetch_history_batch(
      self, itemids: List[str], value_type: int, t_from: int, t_to: int
  ) -> List[dict]:
    # history type equals value_type for API call semantics
    return self.zbx.api_request(
      "history.get",
      {
        "output": ["itemid", "clock", "value"],
        "history": value_type,
        "itemids": itemids,
        "time_from": t_from,
        "time_till": t_to,
        "sortfield": "clock",
        "sortorder": "ASC",
      },
    )

  def _fetch_trend_batch(
      self, itemids: List[str], value_type: int, t_from: int, t_to: int
  ) -> List[dict]:
    # trend type: 0 for float, 3 for unsigned
    return self.zbx.api_request(
      "trend.get",
      {
        "output": ["itemid", "clock", "num", "value_min", "value_avg", "value_max"],
        "trend": value_type,
        "itemids": itemids,
        "time_from": t_from,
        "time_till": t_to,
        "sortfield": "clock",
        "sortorder": "ASC",
      },
    )

  def fetch_series(
      self, sig: GraphSignature, t_from: int, t_to: int
  ) -> Dict[str, Dict[str, np.ndarray]]:
    """
    Fetch time series for all items in signature between [t_from, t_to].
    Returns dict per itemid:
      - For history items: {"clock": int64[], "value": float64[]}
      - For trend items:   {"clock": int64[], "value_min": float64[], "value_avg": float64[], "value_max": float64[]}
    Uses trend.get when period > 24h, but falls back to history.get if trend data is missing.
    """
    period_sec = max(1, int(t_to - t_from))
    use_trend = period_sec > TREND_CUTOVER_SEC

    # Split itemids by value_type
    by_type: Dict[int, List[str]] = {}
    for it in sig.items:
      if it.value_type in NUMERIC_VALUE_TYPES:
        by_type.setdefault(it.value_type, []).append(it.itemid)

    result: Dict[str, Dict[str, np.ndarray]] = {}

    for vtype, ids in by_type.items():
      if not ids:
        continue

      if use_trend:
        # First attempt trend for this value_type group
        t_rows = self._fetch_trend_batch(ids, vtype, t_from, t_to)
        # Group by itemid
        trend_by_item: Dict[str, List[dict]] = {}
        for r in t_rows:
          trend_by_item.setdefault(r["itemid"], []).append(r)

        # Determine which items are missing trend rows
        missing = [iid for iid in ids if iid not in trend_by_item or len(trend_by_item[iid]) == 0]

        # Fill result for items that had trend rows
        for itemid, lst in trend_by_item.items():
          if not lst:
            continue
          lst.sort(key=lambda k: int(k["clock"]))
          clock = np.array([int(r["clock"]) for r in lst], dtype=np.int64)
          vmin = np.array([float(r["value_min"]) for r in lst], dtype=np.float64)
          vavg = np.array([float(r["value_avg"]) for r in lst], dtype=np.float64)
          vmax = np.array([float(r["value_max"]) for r in lst], dtype=np.float64)
          result[itemid] = {"clock": clock, "value_min": vmin, "value_avg": vavg, "value_max": vmax}

        # Fallback to history for missing items (if any)
        if missing:
          h_rows = self._fetch_history_batch(missing, vtype, t_from, t_to)
          hist_by_item: Dict[str, List[dict]] = {}
          for r in h_rows:
            hist_by_item.setdefault(r["itemid"], []).append(r)
          for itemid, lst in hist_by_item.items():
            if not lst:
              continue
            lst.sort(key=lambda k: int(k["clock"]))
            clock = np.array([int(r["clock"]) for r in lst], dtype=np.int64)
            value = np.array([float(r["value"]) for r in lst], dtype=np.float64)
            result[itemid] = {"clock": clock, "value": value}

        continue  # next vtype

      # Not using trend → history only
      h_rows = self._fetch_history_batch(ids, vtype, t_from, t_to)
      hist_by_item: Dict[str, List[dict]] = {}
      for r in h_rows:
        hist_by_item.setdefault(r["itemid"], []).append(r)
      for itemid, lst in hist_by_item.items():
        if not lst:
          continue
        lst.sort(key=lambda k: int(k["clock"]))
        clock = np.array([int(r["clock"]) for r in lst], dtype=np.int64)
        value = np.array([float(r["value"]) for r in lst], dtype=np.float64)
        result[itemid] = {"clock": clock, "value": value}

    return result

  def get_trigger_lines_for_items(self, itemids: List[str]) -> List[TriggerLine]:
    """
    Fetch triggers for given itemids and extract horizontal line thresholds.
    Rules:
      - Only consider triggers where all functions reference a single itemid, and that itemid is in the input set.
      - Use expanded expression to resolve macros, then parse numeric thresholds adjacent to the function.
      - Return one TriggerLine per threshold number.
    """
    if not itemids:
      return []
    res = self.zbx.api_request(
      "trigger.get",
      {
        "output": ["triggerid", "priority", "expression"],
        "expandExpression": True,
        "itemids": itemids,  # only triggers referencing these items
        "filter": {"status": 0},  # enabled
        "selectFunctions": ["itemid"],
        "tags": [{"tag": "channel"}],
      },
    )
    lines: List[TriggerLine] = []
    s_itemids = set(itemids)
    for tr in res:
      funcs = tr.get("functions") or []
      # unique itemids referenced in this trigger
      ids = {f.get("itemid") for f in funcs if f.get("itemid")}
      if len(ids) != 1:
        continue
      (iid,) = tuple(ids)
      if iid not in s_itemids:
        continue
      expr = tr.get("expression") or ""
      thresholds = _parse_thresholds_from_expression(expr)
      prio = int(tr.get("priority", 0))
      for v in thresholds:
        try:
          fv = float(v)
        except Exception:
          continue
        lines.append(TriggerLine(itemid=str(iid), value=fv, priority=prio))
    return lines


@dataclasses.dataclass(frozen=True)
class TriggerLine:
  itemid: str
  value: float
  priority: int  # 0..5


_NUM_RE = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"
_FUNC_ITEM_RE = re.compile(
  r"""\b
        [A-Za-z_]\w*          # function name
        \s*\(
            \s*/[^,)\s]+      # item reference starting with slash: /Host/key...
            (?:\s*,[^)]*)?    # optional additional params
        \)
    """,
  re.VERBOSE,
)
_BRACED_REF_RE = re.compile(r"\{[^}]+}")  # legacy/alternate form


def _parse_thresholds_from_expression(expanded_expr: str) -> list[float]:
  """
  Collapse any item function call (avg(/host/key,...), last(/host/key), etc.) to 'F',
  and extract numeric constants compared to F:
    F < number, F <= number, number >= F, etc.
  Returns unique thresholds in order of appearance.
  """
  if not expanded_expr:
    return []
  expr = expanded_expr

  # 1) Replace function-style item refs with 'F'
  expr = _FUNC_ITEM_RE.sub("F", expr)
  # 2) Replace braced refs (if present) with 'F'
  expr = _BRACED_REF_RE.sub("F", expr)
  # 3) Normalize whitespace
  expr = re.sub(r"\s+", " ", expr).strip()

  # Comparators: >=, <=, <>, >, <, =
  pat_f_op_num = re.compile(rf"""F\s*(>=|<=|<>|>|<|=)\s*({_NUM_RE})""")
  pat_num_op_f = re.compile(rf"""({_NUM_RE})\s*(>=|<=|<>|>|<|=)\s*F""")

  vals: list[float] = []
  for m in pat_f_op_num.finditer(expr):
    vals.append(float(m.group(2)))
  for m in pat_num_op_f.finditer(expr):
    vals.append(float(m.group(1)))

  # Deduplicate while preserving order
  out: list[float] = []
  seen: set[float] = set()
  for v in vals:
    if v not in seen:
      out.append(v)
      seen.add(v)
  return out
