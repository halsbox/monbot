from __future__ import annotations

from typing import Tuple

import numpy as np
import numpy.typing as npt


def _make_bins(t0: int, t1: int, width: int) -> npt.NDArray[np.int64]:
  if t1 <= t0:
    raise ValueError("t1 must be greater than t0")
  if width <= 0:
    raise ValueError("width must be positive")
  # width+1 edges, inclusive of t0 and t1
  return np.linspace(t0, t1, num=width + 1, dtype=np.int64)


def _init_envelope(
    width: int,
) -> Tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.int32]]:
  y_min = np.full((width,), np.nan, dtype=np.float64)
  y_max = np.full((width,), np.nan, dtype=np.float64)
  y_avg = np.full((width,), np.nan, dtype=np.float64)
  count = np.zeros((width,), dtype=np.int32)
  return y_min, y_max, y_avg, count


def downsample_history(
    clock: npt.NDArray[np.int64],
    value: npt.NDArray[np.float64],
    t0: int,
    t1: int,
    width: int,
) -> Tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.int32]]:
  """
  Downsample raw history points into width buckets over [t0, t1).
  Returns (y_min, y_max, y_avg, count) each length 'width'.
  """
  if clock.size == 0:
    return _init_envelope(width)

  edges = _make_bins(t0, t1, width)
  mask = (clock >= t0) & (clock < t1) & np.isfinite(value)
  if not np.any(mask):
    return _init_envelope(width)

  c = clock[mask]
  v = value[mask].astype(np.float64, copy=False)

  idx = np.digitize(c, edges, right=False) - 1
  idx = np.clip(idx, 0, width - 1)

  y_min, y_max, y_avg, count = _init_envelope(width)

  uniq = np.unique(idx)
  for b in uniq:
    members = v[idx == b]
    if members.size == 0:
      continue
    # Compute per-bucket aggregates
    local_min = float(np.nanmin(members))
    local_max = float(np.nanmax(members))
    local_avg = float(np.nanmean(members))
    n = int(np.count_nonzero(np.isfinite(members)))

    # Update min
    prev_min = y_min[b]
    if np.isnan(prev_min) or local_min < float(prev_min):
      y_min[b] = local_min
    # Update max
    prev_max = y_max[b]
    if np.isnan(prev_max) or local_max > float(prev_max):
      y_max[b] = local_max
    # Average (overwrite with this bucket's mean)
    y_avg[b] = local_avg
    count[b] = n

  return y_min, y_max, y_avg, count


def downsample_trend(
    clock: npt.NDArray[np.int64],
    vmin: npt.NDArray[np.float64],
    vavg: npt.NDArray[np.float64],
    vmax: npt.NDArray[np.float64],
    t0: int,
    t1: int,
    width: int,
) -> Tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.int32]]:
  """
  Downsample trend triplets (per-hour aggregates) into width buckets.
  Returns (y_min, y_max, y_avg, count).
  """
  if clock.size == 0:
    return _init_envelope(width)

  edges = _make_bins(t0, t1, width)
  finite_mask = np.isfinite(vmin) | np.isfinite(vavg) | np.isfinite(vmax)
  mask = (clock >= t0) & (clock < t1) & finite_mask
  if not np.any(mask):
    return _init_envelope(width)

  c = clock[mask]
  vmin = vmin[mask].astype(np.float64, copy=False)
  vavg = vavg[mask].astype(np.float64, copy=False)
  vmax = vmax[mask].astype(np.float64, copy=False)

  idx = np.digitize(c, edges, right=False) - 1
  idx = np.clip(idx, 0, width - 1)

  y_min, y_max, y_avg, count = _init_envelope(width)

  uniq = np.unique(idx)
  for b in uniq:
    sel = idx == b
    if not np.any(sel):
      continue
    # Envelope from vmin/vmax; mean from vavg
    local_min = float(np.nanmin(vmin[sel]))
    local_max = float(np.nanmax(vmax[sel]))
    local_avg = float(np.nanmean(vavg[sel]))
    n = int(np.count_nonzero(np.isfinite(vavg[sel]) | np.isfinite(vmin[sel]) | np.isfinite(vmax[sel])))

    prev_min = y_min[b]
    if np.isnan(prev_min) or local_min < float(prev_min):
      y_min[b] = local_min

    prev_max = y_max[b]
    if np.isnan(prev_max) or local_max > float(prev_max):
      y_max[b] = local_max

    y_avg[b] = local_avg
    count[b] = n

  return y_min, y_max, y_avg, count
