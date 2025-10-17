from __future__ import annotations

import dataclasses
import hashlib
import math
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import numpy.typing as npt
import skia

from monbot.utils import next_nice_step, nice_floor_step, prev_nice_step

TRIGGER_COLORS = {
  0: skia.ColorSetARGB(255, 153, 153, 153),  # Not classified (gray)
  1: skia.ColorSetARGB(255, 51, 163, 220),  # Information (blue)
  2: skia.ColorSetARGB(255, 255, 153, 0),  # Warning (orange)
  3: skia.ColorSetARGB(255, 255, 0, 0),  # Average (red)
  4: skia.ColorSetARGB(255, 225, 0, 0),  # High (dark red)
  5: skia.ColorSetARGB(255, 204, 0, 0),  # Disaster (darker red)
}


def _color_for_priority(prio: int) -> int:
  return TRIGGER_COLORS.get(int(prio), TRIGGER_COLORS[4])


@dataclasses.dataclass(frozen=True)
class RenderTheme:
  # Colors
  bg_color: int = skia.ColorWHITE
  plot_bg_color: int = skia.ColorWHITE
  grid_color: int = skia.ColorSetARGB(255, 200, 200, 200)
  axis_color: int = skia.ColorSetARGB(255, 130, 130, 130)
  text_color: int = skia.ColorSetARGB(255, 0, 0, 0)
  legend_bg: int = skia.ColorSetARGB(255, 255, 255, 255)
  legend_text: int = skia.ColorSetARGB(255, 10, 10, 10)

  # Fonts
  font_family: str = "DejaVu Sans Mono"
  font_size: float = 10.0
  legend_font_size: float = 10.0
  legend_value_font_delta: float = -1.0  # value font size relative to legend font

  # Line styles
  envelope_alpha: int = 160
  line_width: float = 1.6
  trigger_line_width: float = 1.2
  grid_width: float = 1.0
  trigger_dash_on: float = 6.0
  trigger_dash_off: float = 4.0

  # Layout paddings
  padding_left: int = 36
  padding_right: int = 4
  padding_top: int = 2
  padding_bottom: int = 18

  # Legend layout
  legend_row_h: int = 26
  legend_x_start: int = 4
  legend_chip_w: int = 10
  legend_chip_h: int = 10
  legend_chip_to_name_dx: int = 4  # spacing between chip and name
  legend_name_min_w: int = 45  # minimum name width
  legend_name_spacing_dx: int = 8  # spacing between entries
  legend_right_margin: int = 4  # keep some free space on the right
  legend_name_baseline_dy: int = 8  # from row top
  legend_last_baseline_dy: int = 12  # below name baseline
  legend_units_dx: int = -12  # draw units at legend_rect.left() + dx
  legend_units_dy: int = -6  # draw units at legend_rect.bottom() + dy

  # X axis ticks/labels
  tick_length: float = 4.0  # tick mark length
  x_label_offset_dy: float = 10.0  # baseline below tick

  # Y axis labels offsets
  y_label_pad_left: float = 2.0  # left padding from plot left
  y_label_baseline_dy: float = 5.0  # baseline adjustment

  # X-axis tick density target
  x_ticks_target: int = 8


@dataclasses.dataclass(frozen=True)
class Layout:
  width: int
  height: int
  padding_left: int
  padding_right: int
  padding_top: int
  padding_bottom: int
  legend_height: int

  @property
  def plot_rect(self) -> skia.Rect:
    l = self.padding_left
    t = self.padding_top + self.legend_height
    r = self.width - self.padding_right
    b = self.height - self.padding_bottom
    return skia.Rect().MakeLTRB(float(l), float(t), float(r), float(b))

  @property
  def legend_rect(self) -> skia.Rect:
    l = self.padding_left
    t = self.padding_top
    r = self.width - self.padding_right
    b = self.padding_top + self.legend_height
    return skia.Rect().MakeLTRB(float(l), float(t), float(r), float(b))


@dataclasses.dataclass
class GraphTemplate:
  key: str
  image: skia.Image
  layout: Layout
  font: skia.Font
  legend_font: skia.Font


class SkiaRenderer:
  def __init__(self, theme: Optional[RenderTheme] = None, font_family: str = "DejaVu Sans"):
    base = theme or RenderTheme()
    # honor explicit font_family argument by replacing theme’s font_family
    if font_family and font_family != base.font_family:
      base = dataclasses.replace(base, font_family=font_family)
    self.theme = base
    self._templates: Dict[str, GraphTemplate] = {}
    self._renderer_version = "v3"

  @staticmethod
  def _sig_hash(graphid: str, items: Tuple[Tuple[str, str, int, int, int], ...]) -> str:
    h = hashlib.sha1()
    h.update(graphid.encode("utf-8"))
    for it in items:
      h.update(("|".join(map(str, it))).encode("utf-8"))
      h.update(b";")
    return h.hexdigest()

  def _template_key(self, graphid: str, sig_items: List[Tuple[str, str, int, int, int]], width: int,
                    height: int) -> str:
    return f"{self._renderer_version}:{width}x{height}:{self._sig_hash(graphid, tuple(sig_items))}"

  def _make_layout(self, width: int, height: int, legend_height: int = 24) -> Layout:
    t = self.theme
    return Layout(
      width=width,
      height=height,
      padding_left=t.padding_left,
      padding_right=t.padding_right,
      padding_top=t.padding_top,
      padding_bottom=t.padding_bottom,
      legend_height=legend_height,
    )

  def _build_template(
      self,
      graphid: str,
      series_meta: List[Tuple[str, str, str]],  # (display_name, color_hex, units)
      width: int,
      height: int,
  ) -> GraphTemplate:
    t = self.theme

    # Fonts
    tf = skia.Typeface(t.font_family).MakeDefault()
    font = skia.Font(tf, t.font_size)
    legend_font = skia.Font(tf, t.legend_font_size)

    # Legend packing (rows) using theme metrics
    chip_advance = float(t.legend_chip_w + t.legend_chip_to_name_dx)
    legend_inner_w = width - (t.padding_left + t.padding_right)

    rows: List[List[Tuple[str, str]]] = []  # list of (name, color_hex)
    x = float(t.legend_x_start)
    current_row: List[Tuple[str, str]] = []
    for name, color_hex, _ in series_meta:
      name_w = legend_font.measureText(name)
      if name_w < t.legend_name_min_w:
        name_w = t.legend_name_min_w
      need = chip_advance + name_w + float(t.legend_name_spacing_dx)
      if current_row and (x + need > (legend_inner_w - t.legend_right_margin)):
        rows.append(current_row)
        current_row = []
        x = float(t.legend_x_start)
      current_row.append((name, color_hex))
      x += need
    if current_row:
      rows.append(current_row)

    legend_height = max(t.legend_row_h, t.legend_row_h * len(rows))

    # Layout and surface
    layout = self._make_layout(width, height, legend_height=legend_height)
    surface = skia.Surface(width, height)
    canvas = surface.getCanvas()
    canvas.clear(t.bg_color)

    # Legend background
    legend_rect = layout.legend_rect
    paint = skia.Paint(Color=t.legend_bg)
    canvas.drawRect(legend_rect, paint)  # type: ignore[arg-type]

    # Units (if unified)
    text_paint = skia.Paint(AntiAlias=True, Color=t.legend_text)
    uniq_units = {u for _, _, u in series_meta if u}
    units_str = next(iter(uniq_units)) if len(uniq_units) == 1 else ""
    if units_str:
      canvas.drawString(
        units_str,
        legend_rect.left() + float(t.legend_units_dx),
        legend_rect.bottom() + float(t.legend_units_dy),
        legend_font,
        text_paint
      )

    # Legend series rows
    for row_idx, row in enumerate(rows):
      x = legend_rect.left() + float(t.legend_x_start)
      y_name = legend_rect.top() + float(row_idx * t.legend_row_h + t.legend_name_baseline_dy)
      y_last = y_name + float(t.legend_last_baseline_dy)
      for name, color_hex in row:
        try:
          r = int(color_hex[0:2], 16)
          g = int(color_hex[2:4], 16)
          b = int(color_hex[4:6], 16)
        except Exception:
          r = g = b = 0
        box_paint = skia.Paint(Color=skia.ColorSetARGB(255, r, g, b))
        # chip rect: 10px high centered at y_name-2 .. y_name+8 (per current code)
        canvas.drawRect(
          skia.Rect().MakeLTRB(x, y_name - 2.0, x + float(t.legend_chip_w), y_name + 8.0),
          box_paint
        )  # type: ignore[arg-type]
        x += chip_advance

        canvas.drawString(name, x, y_name, legend_font, text_paint)
        name_w = legend_font.measureText(name)
        if name_w < t.legend_name_min_w:
          name_w = t.legend_name_min_w
        canvas.drawString('last:', x, y_last, legend_font, text_paint)

        x += name_w + float(t.legend_name_spacing_dx)
        if x > legend_rect.right() - float(t.legend_right_margin):
          break

    # Plot area and frame
    plot_bg_paint = skia.Paint(Color=t.plot_bg_color)
    canvas.drawRect(layout.plot_rect, plot_bg_paint)  # type: ignore[arg-type]
    axis_paint = skia.Paint(Style=skia.Paint.kStroke_Style, Color=t.axis_color, StrokeWidth=t.grid_width)
    canvas.drawRect(layout.plot_rect, axis_paint)  # type: ignore[arg-type]

    image = surface.makeImageSnapshot()
    key = self._template_key(graphid, [], width, height)
    return GraphTemplate(key=key, image=image, layout=layout, font=font, legend_font=legend_font)

  def get_or_create_template(
      self,
      graphid: str,
      sig_items: List[Tuple[str, str, int, int, int]],
      series_meta: List[Tuple[str, str, str]],
      width: int,
      height: int,
  ) -> GraphTemplate:
    tkey = self._template_key(graphid, sig_items, width, height)
    tmpl = self._templates.get(tkey)
    if tmpl is not None:
      return tmpl
    tmpl = self._build_template(graphid, series_meta, width, height)
    tmpl.key = tkey  # type: ignore[attr-defined]
    self._templates[tkey] = tmpl
    return tmpl

  def _draw_y_ticks_and_labels_precomputed(self, canvas: skia.Canvas, rect: skia.Rect, axis_min: float, axis_max: float,
                                           ticks: List[float], font: skia.Font):
    if not ticks:
      return
    grid_paint = skia.Paint(Style=skia.Paint.kStroke_Style, Color=self.theme.grid_color,
                            StrokeWidth=self.theme.grid_width)
    text_paint = skia.Paint(AntiAlias=True, Color=self.theme.text_color)

    # derive decimals from step if possible
    if len(ticks) >= 2:
      step = ticks[1] - ticks[0]
      decimals = max(0, -int(math.floor(math.log10(abs(step)))) if step != 0 else 0)
    else:
      decimals = 0
    decimals = min(decimals, 6)

    for v in ticks:
      y = self._value_to_y(v, rect, axis_min, axis_max)
      canvas.drawLine(rect.left(), y, rect.right(), y, grid_paint)
      label = f"{v:.{decimals}f}"
      canvas.drawString(
        label,
        rect.left() - float(self.theme.y_label_pad_left) - font.measureText(label),
        y + float(self.theme.y_label_baseline_dy),
        font,
        text_paint
      )

  @staticmethod
  def _value_to_y(val: float, rect: skia.Rect, ymin: float, ymax: float) -> float:
    if ymax <= ymin:
      return rect.bottom() - 1
    norm = (val - ymin) / (ymax - ymin)
    return float(rect.bottom() - norm * rect.height())

  @staticmethod
  def _time_step(t_from: int, t_to: int, target: int = 5) -> int:
    span = max(t_to - t_from, 1)
    # candidate steps in seconds
    candidates = [60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400]
    # choose smallest step producing <= target*2 ticks (avoid clutter)
    for s in candidates:
      if span / s <= target:
        return s
    return candidates[-1]

  def _draw_x_ticks_and_labels(self, canvas: skia.Canvas, rect: skia.Rect, t_from: int, t_to: int, font: skia.Font, tz: ZoneInfo):
    step = self._time_step(t_from, t_to, self.theme.x_ticks_target)
    if step <= 0:
      return

    # Build tick list aligned to step
    first = (t_from // step) * step
    if first < t_from:
      first += step
    ticks: List[int] = []
    x = first
    for _ in range(200):
      if x > t_to:
        break
      ticks.append(x)
      x += step
    if not ticks:
      return

    text_paint = skia.Paint(AntiAlias=True, Color=self.theme.text_color)
    tick_paint = skia.Paint(Style=skia.Paint.kStroke_Style, Color=self.theme.axis_color, StrokeWidth=1.0)

    # label format by span
    span = t_to - t_from
    fmt = "%H:%M" if span <= 3 * 86400 else "%d.%m"

    xw = rect.width()
    tspan = max(t_to - t_from, 1)

    for i, tx in enumerate(ticks):
      u = (tx - t_from) / tspan
      px = float(rect.left() + u * xw)
      y1 = rect.bottom()
      y2 = y1 + self.theme.tick_length
      canvas.drawLine(px, y1, px, y2, tick_paint)

      label = datetime.fromtimestamp(tx, tz).strftime(fmt)
      w = font.measureText(label)

      if (i == len(ticks) - 1) and ((px - w / 2.0) > (rect.right() - w)):
        # Right-align the last label to plot right edge
        lx = rect.right() - w
      else:
        lx = px - w / 2.0

      canvas.drawString(label, lx, y2 + float(self.theme.x_label_offset_dy), font, text_paint)

  def _draw_series_envelope(
      self,
      canvas: skia.Canvas,
      rect: skia.Rect,
      x_coords: npt.NDArray[np.float32],
      y_min: npt.NDArray[np.float32],
      y_max: npt.NDArray[np.float32],
      y_line: npt.NDArray[np.float32],
      color_hex: str,
      ymin: float,
      ymax: float,
  ):
    try:
      r = int(color_hex[0:2], 16)
      g = int(color_hex[2:4], 16)
      b = int(color_hex[4:6], 16)
    except Exception:
      r = g = b = 0

    # Enforce a minimum envelope thickness of ~1 y-pixel to make alpha visible on sparse data
    # Convert 1 pixel in plot to value delta
    plot_h = max(1.0, rect.height())
    min_val_thickness = (ymax - ymin) / plot_h  # ~1 pixel in Y
    if not np.isfinite(min_val_thickness) or min_val_thickness <= 0:
      min_val_thickness = 0.0

    # Prepare working copies and expand collapsed buckets around the avg line
    y_min_draw = y_min.astype(np.float64, copy=True)
    y_max_draw = y_max.astype(np.float64, copy=True)

    if min_val_thickness > 0:
      # Where envelope is too thin or invalid, replace by [avg - d/2, avg + d/2]
      d = min_val_thickness
      span = y_max_draw - y_min_draw
      thin = (~np.isfinite(span)) | (span < d)
      if thin.any():
        # fall back to avg line for those indices
        y_avg64 = y_line.astype(np.float64, copy=False)
        lo = y_avg64 - 0.5 * d
        hi = y_avg64 + 0.5 * d
        # clamp to axis range
        lo = np.clip(lo, ymin, ymax)
        hi = np.clip(hi, ymin, ymax)
        y_min_draw[thin] = lo[thin]
        y_max_draw[thin] = hi[thin]

    fill_color = skia.ColorSetARGB(self.theme.envelope_alpha, r, g, b)
    fill_paint = skia.Paint(Style=skia.Paint.kFill_Style, Color=fill_color, AntiAlias=False)

    path = skia.Path()
    first = True
    # Top edge (y_max_draw)
    for i in range(x_coords.shape[0]):
      x = float(rect.left() + x_coords[i])
      yv = float(self._value_to_y(float(y_max_draw[i]), rect, ymin, ymax))
      if not np.isfinite(yv):
        continue
      if first:
        path.moveTo(x, yv)
        first = False
      else:
        path.lineTo(x, yv)

    # Bottom edge reversed (y_min_draw)
    for i in range(x_coords.shape[0] - 1, -1, -1):
      x = float(rect.left() + x_coords[i])
      yv = float(self._value_to_y(float(y_min_draw[i]), rect, ymin, ymax))
      if not np.isfinite(yv):
        continue
      path.lineTo(x, yv)

    if not first:
      path.close()
      canvas.drawPath(path, fill_paint)  # type: ignore[arg-type]

    # Stroke avg line as before
    stroke_paint = skia.Paint(
      Style=skia.Paint.kStroke_Style,
      Color=skia.ColorSetARGB(255, r, g, b),
      StrokeWidth=self.theme.line_width,
      AntiAlias=True,
    )
    line_path = skia.Path()
    moved = False
    for i in range(x_coords.shape[0]):
      yv = float(y_line[i])
      if not np.isfinite(yv):
        moved = False
        continue
      x = float(rect.left() + x_coords[i])
      y = float(self._value_to_y(yv, rect, ymin, ymax))
      if not moved:
        line_path.moveTo(x, y)
        moved = True
      else:
        line_path.lineTo(x, y)
    canvas.drawPath(line_path, stroke_paint)  # type: ignore[arg-type]

  def _draw_trigger_lines(self, canvas: skia.Canvas, rect: skia.Rect, axis_min: float, axis_max: float,
                          lines: List[Tuple[float, int]]):
    if not lines:
      return
    paint = skia.Paint(Style=skia.Paint.kStroke_Style, StrokeWidth=self.theme.trigger_line_width, AntiAlias=True)
    dash = skia.DashPathEffect.Make([float(self.theme.trigger_dash_on), float(self.theme.trigger_dash_off)], 0.0)
    paint.setPathEffect(dash)
    for val, prio in lines:
      if not np.isfinite(val):
        continue
      if val < axis_min or val > axis_max:
        continue
      y = self._value_to_y(val, rect, axis_min, axis_max)
      paint.setColor(_color_for_priority(prio))
      canvas.drawLine(rect.left(), y, rect.right(), y, paint)

  def _render_image_core(
      self,
      sig_graphid: str,
      series_list: List[Tuple[str, str, int, int, int, str, str]],
      envelopes: Dict[str, Dict[str, npt.NDArray[np.float64]]],
      t_from: int,
      t_to: int,
      width: int,
      height: int,
      trigger_lines: Optional[List[Tuple[float, int]]] = None,
      tz: ZoneInfo = ZoneInfo("UTC")
  ) -> Tuple[skia.Image, GraphTemplate, float, float, List[Tuple[str, str, int, int, int, str, str]]]:
    # Filter items with non-empty units; if none remain, keep all to avoid empty plot
    filtered = [t for t in series_list if t[6]]
    use_series = filtered if filtered else series_list

    # Build template (legend) using upstream-provided order
    sig_items_key = [(i, c, cf, dt, so) for (i, c, cf, dt, so, _, _) in use_series]
    series_meta = [(disp, color, units) for (_, color, _, _, _, disp, units) in use_series]
    tmpl = self.get_or_create_template(sig_graphid, sig_items_key, series_meta, width, height)

    # Compute y-range across included series
    ymin = np.inf
    ymax = -np.inf
    for itemid, *_rest in use_series:
      env = envelopes.get(itemid)
      if not env:
        continue
      if env["y_min"].size:
        ymn = np.nanmin(env["y_min"])
        if np.isfinite(ymn):
          ymin = min(ymin, float(ymn))
      if env["y_max"].size:
        ymx = np.nanmax(env["y_max"])
        if np.isfinite(ymx):
          ymax = max(ymax, float(ymx))
    if not np.isfinite(ymin) or not np.isfinite(ymax) or ymax <= ymin:
      ymin, ymax = 0.0, 1.0

    # Align axis and ticks
    axis_min, axis_max, y_step, y_ticks = compute_y_axis(ymin, ymax, min_ticks=8, max_ticks=12)

    # Prepare surface and draw template
    surface = skia.Surface(tmpl.layout.width, tmpl.layout.height)
    canvas = surface.getCanvas()
    canvas.clear(self.theme.bg_color)
    canvas.drawImage(tmpl.image, 0, 0)

    # Y grid + labels
    self._draw_y_ticks_and_labels_precomputed(canvas, tmpl.layout.plot_rect, axis_min, axis_max, y_ticks, tmpl.font)
    # X ticks + labels
    self._draw_x_ticks_and_labels(canvas, tmpl.layout.plot_rect, t_from, t_to, tmpl.font, tz)

    # Trigger lines under series
    if trigger_lines:
      self._draw_trigger_lines(canvas, tmpl.layout.plot_rect, axis_min, axis_max, trigger_lines)

    # X coords
    plot_rect = tmpl.layout.plot_rect
    W = int(plot_rect.width())
    if W <= 0:
      W = max(1, width - int(tmpl.layout.padding_left + tmpl.layout.padding_right))
    x_coords = np.linspace(0.5, W - 0.5, num=W, dtype=np.float32)

    # Draw series in upstream order
    for itemid, color, calc_fnc, drawtype, sortorder, disp_name, units in use_series:
      env = envelopes.get(itemid)
      if not env:
        continue

      # Resample to width if needed
      if env["y_min"].shape[0] != W:
        srcW = env["y_min"].shape[0]
        if srcW <= 0:
          continue
        idx = (np.linspace(0, srcW - 1, num=W)).astype(np.int32)
        y_min = env["y_min"][idx]
        y_max = env["y_max"][idx]
        y_avg = env["y_avg"][idx]
      else:
        y_min = env["y_min"]
        y_max = env["y_max"]
        y_avg = env["y_avg"]

      self._draw_series_envelope(
        canvas,
        plot_rect,
        x_coords,
        y_min.astype(np.float32, copy=False),
        y_max.astype(np.float32, copy=False),
        y_avg.astype(np.float32, copy=False),
        color,
        axis_min,
        axis_max,
      )

    # Dynamic legend "last:" overlay, using theme metrics
    decimals = 2  # keep per your current behavior
    legend_rect = tmpl.layout.legend_rect
    legend_font = tmpl.legend_font
    value_font = legend_font.makeWithSize(legend_font.getSize() + self.theme.legend_value_font_delta)
    text_paint = skia.Paint(AntiAlias=True, Color=self.theme.legend_text)

    x_start = float(self.theme.legend_x_start)
    chip_advance = float(self.theme.legend_chip_w + self.theme.legend_chip_to_name_dx)
    name_spacing = float(self.theme.legend_name_spacing_dx)
    right_margin = float(self.theme.legend_right_margin)
    row_h = float(self.theme.legend_row_h)

    last_label = "last:"
    last_w = legend_font.measureText(last_label)

    # Pack items into rows exactly like the template
    avail_w = legend_rect.width()
    row_items: List[List[Tuple[str, str]]] = []
    cur_row: List[Tuple[str, str]] = []
    x = x_start
    for itemid, color, calc_fnc, drawtype, sortorder, disp_name, units in use_series:
      name_w = legend_font.measureText(disp_name)
      if name_w < self.theme.legend_name_min_w:
        name_w = self.theme.legend_name_min_w
      need = chip_advance + name_w + name_spacing
      if cur_row and (x + need > (avail_w - right_margin)):
        row_items.append(cur_row)
        cur_row = []
        x = x_start
      cur_row.append((itemid, disp_name))
      x += need
    if cur_row:
      row_items.append(cur_row)

    for row_idx, row in enumerate(row_items):
      x = legend_rect.left() + x_start
      y_name = legend_rect.top() + float(row_idx) * row_h + float(self.theme.legend_name_baseline_dy)
      y_last = y_name + float(self.theme.legend_last_baseline_dy)
      for itemid, disp_name in row:
        # Advance to text after chip
        x_name = x + chip_advance
        name_w = legend_font.measureText(disp_name)
        if name_w < self.theme.legend_name_min_w:
          name_w = self.theme.legend_name_min_w

        # Compute last value from envelopes (last finite y_avg)
        val_str = "-"
        env = envelopes.get(itemid)
        if env is not None and env["y_avg"].size:
          finite_idx = np.where(np.isfinite(env["y_avg"]))[0]
          if finite_idx.size:
            last_val = float(env["y_avg"][finite_idx[-1]])
            val_str = f"{last_val:.{decimals}f}"

        val_x = x_name + last_w + 2.0
        canvas.drawString(val_str, val_x, y_last, value_font, text_paint)

        # Advance x for next entry
        x = x_name + name_w + name_spacing
        if x > legend_rect.right() - right_margin:
          break

    image = surface.makeImageSnapshot()
    return image, tmpl, axis_min, axis_max, use_series

  def render_jpeg(
      self,
      sig_graphid: str,
      series_list: List[Tuple[str, str, int, int, int, str, str]],
      envelopes: Dict[str, Dict[str, npt.NDArray[np.float64]]],
      t_from: int,
      t_to: int,
      width: int,
      height: int,
      quality: int = 82,
      trigger_lines: Optional[List[Tuple[float, int]]] = None,
      tz: ZoneInfo = ZoneInfo("UTC"),
  ) -> bytes:
    image, _tmpl, _amin, _amax, _ordered = self._render_image_core(
      sig_graphid, series_list, envelopes, t_from, t_to, width, height, trigger_lines, tz
    )
    data = image.encodeToData(skia.kJPEG, quality)
    return bytes(data) if data is not None else b""

  def render_png(
      self,
      sig_graphid: str,
      series_list: List[Tuple[str, str, int, int, int, str, str]],
      envelopes: Dict[str, Dict[str, npt.NDArray[np.float64]]],
      t_from: int,
      t_to: int,
      width: int,
      height: int,
      trigger_lines: Optional[List[Tuple[float, int]]] = None,
      tz: ZoneInfo = ZoneInfo("UTC"),
  ) -> bytes:
    image, _tmpl, _amin, _amax, _ordered = self._render_image_core(
      sig_graphid, series_list, envelopes, t_from, t_to, width, height, trigger_lines, tz
    )
    data = image.encodeToData(skia.kPNG, 100)
    return bytes(data) if data is not None else b""


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
