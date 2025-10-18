import math
import re
import time


PALETTE_20 = [
  "1f77b4", "ff7f0e", "2ca02c", "d62728", "9467bd",
  "8c564b", "756bb1", "7f7f7f", "bcbd22", "17becf",
  "393b79", "637939", "8c6d31", "843c39", "7b4173",
  "3182bd", "e6550d", "31a354", "dd1c77", "e377c2",
]


def natural_key(s: str):
  return [int(t) if t.isdigit() else t.lower() for t in re.findall(r"\d+|\D+", s or "")]


def nice_floor_step(raw: float) -> float:
  if not math.isfinite(raw) or raw <= 0:
    return 1.0
  exp = math.floor(math.log10(raw))
  base = 10.0 ** exp
  factor = raw / base
  ladder = (1.0, 2.0, 2.5, 5.0, 10.0)
  prev = ladder[0]
  for m in ladder:
    if factor < m - 1e-12:
      break
    prev = m
  return prev * base


def next_nice_step(step: float) -> float:
  if step <= 0 or not math.isfinite(step):
    return 1.0
  exp = math.floor(math.log10(step))
  base = 10.0 ** exp
  factor = step / base
  for m in (1.0, 2.0, 2.5, 5.0, 10.0):
    if factor < m - 1e-12:
      return m * base
  return 1.0 * (10.0 ** (exp + 1))


def prev_nice_step(step: float) -> float:
  if step <= 0 or not math.isfinite(step):
    return 1.0
  exp = math.floor(math.log10(step))
  base = 10.0 ** exp
  factor = step / base
  prev = None
  for m in (1.0, 2.0, 2.5, 5.0, 10.0):
    if factor <= m + 1e-12:
      break
    prev = m
  if prev is None:
    return 0.5 * base  # next smaller across decade boundary
  return prev * base


def fmt_ts(ts: int) -> str:
  return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
