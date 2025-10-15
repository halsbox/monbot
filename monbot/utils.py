import math
import re
import time
from typing import List, Tuple

from telegram import Update
from telegram.ext import CallbackContext

PALETTE_20 = [
  "1f77b4", "ff7f0e", "2ca02c", "d62728", "9467bd",
  "8c564b", "756bb1", "7f7f7f", "bcbd22", "17becf",
  "393b79", "637939", "8c6d31", "843c39", "7b4173",
  "3182bd", "e6550d", "31a354", "dd1c77", "e377c2",
]


async def safe_delete_query_message(update: Update, context: CallbackContext):
  q = update.callback_query
  if q and q.message:
    try:
      await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=q.message.message_id)
    except Exception:
      pass


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


def compute_y_axis(ymin_data: float, ymax_data: float, min_ticks: int = 8, max_ticks: int = 14) -> Tuple[
  float, float, float, List[float]]:
  if not (math.isfinite(ymin_data) and math.isfinite(ymax_data)):
    # fallback axis
    return 0.0, 1.0, 1.0, [0.0, 1.0]
  if ymax_data < ymin_data:
    ymin_data, ymax_data = ymax_data, ymin_data
  if ymax_data == ymin_data:
    ymin_data -= 1.0
    ymax_data += 1.0

  min_ticks = max(int(min_ticks), 2)
  max_ticks = max(int(max_ticks), min_ticks)

  span = ymax_data - ymin_data
  # Start from step targeting min_ticks intervals
  raw_for_min = span / (min_ticks - 1)
  step = nice_floor_step(raw_for_min)

  # Align axis to this step
  axis_min = math.floor(ymin_data / step) * step
  axis_max = math.ceil(ymax_data / step) * step
  cnt = int(round((axis_max - axis_min) / step)) + 1

  # Too many ticks -> coarsen
  while cnt > max_ticks:
    step = next_nice_step(step)
    axis_min = math.floor(ymin_data / step) * step
    axis_max = math.ceil(ymax_data / step) * step
    cnt = int(round((axis_max - axis_min) / step)) + 1

  # Too few ticks -> finer
  guard = 0
  while cnt < min_ticks and guard < 50:
    new_step = prev_nice_step(step)
    if new_step <= 0 or abs(new_step - step) < 1e-12:
      break
    step = new_step
    axis_min = math.floor(ymin_data / step) * step
    axis_max = math.ceil(ymax_data / step) * step
    cnt = int(round((axis_max - axis_min) / step)) + 1
    guard += 1

  # Build ticks inclusive from aligned bounds
  if step != 0:
    decimals = max(0, -int(math.floor(math.log10(abs(step)))))
  else:
    decimals = 0
  decimals = min(decimals, 6)

  ticks: List[float] = []
  v = axis_min
  for _ in range(2000):
    if v > axis_max + 1e-12:
      break
    vv = 0.0 if abs(v) < 1e-12 else v
    ticks.append(round(vv, decimals + 1))
    v += step

  return axis_min, axis_max, step, ticks


def fmt_ts(ts: int) -> str:
  return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
