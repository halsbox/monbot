from __future__ import annotations

import asyncio
import io
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

from pathlib import Path
from monbot.db import UserDB
from monbot.handlers.texts import DT_FMT
from monbot.config import DEFAULT_TZ, REPORT_META_TTL_SEC, REPORT_WIDGETS_TTL_SEC, REPORT_STORAGE_DIR, REPORT_DASHBOARD_ID
from monbot.zabbix import ZabbixWeb
from monbot.zbx_data import fmt_dt_chart2, fmt_uptime

logger = logging.getLogger(__name__)

GRID_COLS = 24
F_REG = "DejaVuSans"
F_BOLD = "DejaVuSans-Bold"

# Zabbix-like widget header bg and severity colors
W_HEADER_BG = colors.Color(0.95, 0.97, 0.98)  # light gray-blue
SEV_COLORS = {
  3: colors.Color(0.20, 0.64, 0.86),
  4: colors.Color(1.00, 0.60, 0.00),
  5: colors.Color(0.95, 0.00, 0.00),
}


@dataclass(frozen=True)
class ReportPeriod:
  start_ts: int  # inclusive UTC
  end_ts: int    # exclusive UTC
  label: str


@dataclass(frozen=True)
class WidgetInfo:
  widgetid: str
  type: str
  name: str
  x: int
  y: int
  width: int
  height: int
  view_mode: int
  params: Dict[str, Any]


@dataclass(frozen=True)
class DashboardPage:
  pageid: str
  name: str
  display_period: int
  widgets: List[WidgetInfo]


class ReportService:
  def __init__(self, zbx: ZabbixWeb, tz: Optional[ZoneInfo] = None):
    self.zbx = zbx
    self.tz = tz or ZoneInfo(DEFAULT_TZ)
    self._ensure_fonts()
    self._meta_cache: dict[int, tuple[float, dict]] = {}
    self._pages_cache: dict[int, tuple[float, List[DashboardPage]]] = {}

  @staticmethod
  def _ensure_fonts() -> None:
    # If already registered, skip
    if F_REG in pdfmetrics.getRegisteredFontNames() and F_BOLD in pdfmetrics.getRegisteredFontNames():
      return
    # Try common system locations
    candidates = [
      ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
       "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
      ("/usr/local/share/fonts/DejaVuSans.ttf",
       "/usr/local/share/fonts/DejaVuSans-Bold.ttf"),
      (os.path.expanduser("~/.fonts/DejaVuSans.ttf"),
       os.path.expanduser("~/.fonts/DejaVuSans-Bold.ttf")),
    ]
    for reg, bold in candidates:
      if os.path.exists(reg) and os.path.exists(bold):
        pdfmetrics.registerFont(TTFont(F_REG, reg))
        pdfmetrics.registerFont(TTFont(F_BOLD, bold))
        return
    # Last resort: register if env points provided
    reg_env = os.getenv("REPORTLAB_FONT_REG")
    bold_env = os.getenv("REPORTLAB_FONT_BOLD")
    if reg_env and bold_env and os.path.exists(reg_env) and os.path.exists(bold_env):
      pdfmetrics.registerFont(TTFont(F_REG, reg_env))
      pdfmetrics.registerFont(TTFont(F_BOLD, bold_env))
      return
    # Fallback to built-in Helvetica (may show tofu for Cyrillic)
    # No exception; let it run, but advise switching fonts if needed.

  @staticmethod
  def _px_dims(w_pts: float, h_pts: float, pad_top_pts: float = 0.0) -> tuple[int, int, float, float]:
    # Inner area in points (remove tiny borders)
    avail_w_pts = max(1.0, w_pts - 2.0)  # ~0.7 mm side gutters
    avail_h_pts = max(1.0, h_pts - pad_top_pts - 2.0)  # top header + tiny bottom gutter
    # Points (72 dpi) -> pixels (96 dpi)
    px_w = max(320, int(round(avail_w_pts * (96.0 / 72.0))))
    px_h = max(120, int(round(avail_h_pts * (96.0 / 72.0))))
    return px_w, px_h, avail_w_pts, avail_h_pts
  # ---------- period helpers ----------

  @staticmethod
  def _to_utc_ts(dt: datetime) -> int:
    if dt.tzinfo is None:
      dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())

  @staticmethod
  def _draw_image_fill(c: canvas.Canvas, img_bytes: bytes, x: float, y_top: float,
                       inner_w_pts: float, inner_h_pts: float, pad_top: float):
    img = ImageReader(io.BytesIO(img_bytes))
    # Draw image exactly to the inner area (no extra scaling/letterboxing)
    cx = x + 0.0
    cy = (y_top - pad_top) - inner_h_pts
    c.drawImage(img, cx, cy, width=inner_w_pts, height=inner_h_pts, preserveAspectRatio=False, mask='auto')

  def month_bounds_by_any_date(self, d: date) -> Tuple[int, int, str]:
    first_this = date(d.year, d.month, 1)
    first_next = date(d.year + (1 if d.month == 12 else 0), (1 if d.month == 12 else d.month + 1), 1)
    start_local = datetime(first_this.year, first_this.month, first_this.day, tzinfo=self.tz)
    end_local = datetime(first_next.year, first_next.month, first_next.day, tzinfo=self.tz)
    return self._to_utc_ts(start_local), self._to_utc_ts(end_local), first_this.strftime("%d.%m.%Y")

  def last_month_period(self, now: Optional[datetime] = None) -> ReportPeriod:
    now = now or datetime.now(self.tz)
    first_this = date(now.year, now.month, 1)
    last_prev_date = first_this - timedelta(days=1)
    s, e, label = self.month_bounds_by_any_date(last_prev_date)
    return ReportPeriod(s, e, f"{label}")

  def week_bounds_by_any_date(self, d: date) -> Tuple[int, int, str]:
    monday = d - timedelta(days=(d.isoweekday() - 1))
    next_monday = monday + timedelta(days=7)
    start_local = datetime(monday.year, monday.month, monday.day, tzinfo=self.tz)
    end_local = datetime(next_monday.year, next_monday.month, next_monday.day, tzinfo=self.tz)
    iso_year, iso_week, _ = monday.isocalendar()
    return self._to_utc_ts(start_local), self._to_utc_ts(end_local), f"Неделя {iso_week}, {iso_year}"

  def last_week_period(self, now: Optional[datetime] = None) -> ReportPeriod:
    now = now or datetime.now(self.tz)
    monday_this = now.date() - timedelta(days=(now.isoweekday() - 1))
    monday_prev = monday_this - timedelta(days=7)
    s, e, label = self.week_bounds_by_any_date(monday_prev)
    return ReportPeriod(s, e, f"{label}")

  # ---------- dashboard pages + widgets ----------

  def _dashboard_meta(self, dashboard_id: int) -> dict:
    res = self.zbx.api_request(
      "dashboard.get",
      {"output": ["dashboardid", "name"], "dashboardids": [dashboard_id]},
    )
    if not res:
      raise RuntimeError(f"Dashboard {dashboard_id} not found")
    return res[0]

  def _pages_with_widgets(self, dashboard_id: int) -> List[DashboardPage]:
    res = self.zbx.api_request(
      "dashboard.get",
      {
        "output": ["dashboardid", "name"],
        "dashboardids": [dashboard_id],
        "selectPages": ["dashboard_pageid", "name", "display_period", "widgets"],
      },
    )
    if not res:
      return []
    pages_api = res[0].get("pages") or []
    out: List[DashboardPage] = []
    for p in pages_api:
      widgets: List[WidgetInfo] = []
      for w in (p.get("widgets") or []):
        params: Dict[str, Any] = {}
        for f in (w.get("fields") or []):
          n = f.get("name")
          if n:
            params[n] = f.get("value")
        widgets.append(
          WidgetInfo(
            widgetid=str(w.get("widgetid") or ""),
            type=str(w.get("type") or ""),
            name=str(w.get("name") or ""),
            x=int(w.get("x", 0) or 0),
            y=int(w.get("y", 0) or 0),
            width=int(w.get("width", 1) or 1),
            height=int(w.get("height", 2) or 2),
            view_mode=int(w.get("view_mode", 0) or 0),
            params=params,
          )
        )
      out.append(DashboardPage(
        pageid=str(p.get("dashboard_pageid") or ""),
        name=str(p.get("name") or ""),
        display_period=int(p.get("display_period", 0) or 0),
        widgets=widgets
      ))
    return out

  # ---------- chart images (per 6.4 chart.php / chart2.php) ----------

  def _chart2_png(self, graphid: str, t_from: int, t_to: int, width: int, height: int) -> bytes:
    url = self.zbx.server + "chart2.php"
    params = {
      "graphid": graphid,
      "from": fmt_dt_chart2(self.tz, t_from),
      "to": fmt_dt_chart2(self.tz, t_to),
      "width": width,
      "height": height,
      "legend": 1,
      "outer": 1,
      "profileIdx": "web.dashboard.filter",
      "widget_view": 1,  # match dashboard appearance
    }
    print(params)
    r = self.zbx.session.get(url, params=params, timeout=30)
    r.raise_for_status()
    ctype = (r.headers.get("content-type") or "").lower()
    if "image/" not in ctype:
      raise RuntimeError(f"chart2.php returned {ctype}: {r.text[:200]}")
    return r.content

  def _chart_items_png(self, itemids: List[str], t_from: int, t_to: int, width: int, height: int) -> bytes:
    url = self.zbx.server + "chart.php"
    params = [
      ("from", fmt_dt_chart2(self.tz, t_from)),
      ("to", fmt_dt_chart2(self.tz, t_to)),
      ("width", width),
      ("height", height),
      ("legend", 1),
      ("outer", 1),
      ("widget_view", 1),
    ]
    for iid in itemids:
      params.append(("itemids[]", str(iid)))
    r = self.zbx.session.get(url, params=params, timeout=30)
    r.raise_for_status()
    ctype = (r.headers.get("content-type") or "").lower()
    if "image/" not in ctype:
      raise RuntimeError(f"chart.php returned {ctype}: {r.text[:200]}")
    return r.content

  # ---------- API helpers for various widgets ----------

  def _get_item_stats(self, itemids: Iterable[str], t_from: int, t_to: int) -> Dict[str, Dict[str, Any]]:
    ids = [str(x) for x in itemids]
    out: Dict[str, Dict[str, Any]] = {}
    if not ids:
      return out
    meta = self.zbx.api_request("item.get", {"output": ["itemid", "value_type", "units", "name"], "itemids": ids})
    vtype = {it["itemid"]: int(it.get("value_type", 0)) for it in meta}
    for it in meta:
      out[it["itemid"]] = {"units": it.get("units") or "", "name": it.get("name") or ""}
    period_sec = max(1, t_to - t_from)
    use_trend = period_sec > 48 * 3600
    if use_trend:
      for iid in ids:
        vt = vtype.get(iid, 0)
        rows = self.zbx.api_request("trend.get", {
          "output": ["clock", "num", "value_min", "value_avg", "value_max"],
          "trend": vt, "itemids": [iid], "time_from": t_from, "time_till": t_to,
          "sortfield": "clock", "sortorder": "ASC",
        })
        if not rows:
          continue
        mins = [float(r["value_min"]) for r in rows]
        avgs = [float(r["value_avg"]) for r in rows]
        maxs = [float(r["value_max"]) for r in rows]
        out[iid].update(
          last=avgs[-1],
          avg=(sum(avgs) / len(avgs)),
          min=min(mins),
          max=max(maxs),
          last_clock=int(rows[-1]["clock"])
        )
    else:
      for iid in ids:
        vt = vtype.get(iid, 0)
        rows = self.zbx.api_request("history.get", {
          "output": ["clock", "value"], "history": vt, "itemids": [iid],
          "time_from": t_from, "time_till": t_to, "sortfield": "clock", "sortorder": "ASC",
        })
        if not rows:
          continue
        vals = [float(r["value"]) for r in rows]
        out[iid].update(
          last=vals[-1],
          avg=(sum(vals) / len(vals)),
          min=min(vals),
          max=max(vals),
          last_clock=int(rows[-1]["clock"]),
        )
    return out

  def _problems_totals(self, params: Dict[str, Any]) -> Dict[int, int]:
    """Return counts per severity (0..5) per widget filter (Totals mode)."""
    groupids = [v for k, v in params.items() if str(k) == "groupids"]
    # Collect multiple entries if fields repeated.
    if not groupids:
      groupids = [v for k, v in params.items() if str(k).startswith("groupids")]
    hostids = [v for k, v in params.items() if str(k) == "hostids"] or \
              [v for k, v in params.items() if str(k).startswith("hostids")]
    # Tags with index
    tags: List[Dict[str, Any]] = []
    evaltype = int(params.get("evaltype", 0) or 0)
    idxs = set()
    for k in params.keys():
      m = re.match(r"^tags\.tag\.(\d+)$", str(k))
      if m:
        idxs.add(int(m.group(1)))
    for i in sorted(idxs):
      tag = str(params.get(f"tags.tag.{i}", "") or "")
      op = int(params.get(f"tags.operator.{i}", 0) or 0)
      val = str(params.get(f"tags.value.{i}", "") or "")
      # Zabbix problem.get tag operator mapping:
      # 0 contains, 1 equals, 2 not contains, 3 not equals, 4 exists, 5 not exists
      tags.append({"tag": tag, "operator": op, "value": val})
    show_suppressed = int(params.get("show_suppressed", 0) or 0)

    req: Dict[str, Any] = {
      "output": ["eventid", "severity"],
      "sortfield": "eventid",
      "sortorder": "DESC",
      "countOutput": False,
    }
    if groupids:
      req["groupids"] = groupids
    if hostids:
      req["hostids"] = hostids
    if tags:
      req["tags"] = tags
      req["evaltype"] = evaltype
    if show_suppressed:
      req["suppressed"] = True

    rows = self.zbx.api_request("problem.get", req)
    counts = {i: 0 for i in range(0, 6)}
    for r in rows or []:
      sev = int(r.get("severity", 0) or 0)
      if 0 <= sev <= 5:
        counts[sev] += 1
    return counts

  # ---------- drawing helpers ----------

  def _draw_header(self, c: canvas.Canvas, period: ReportPeriod, top: float, page_w: float):
    c.setFont(F_BOLD, 16)
    s_local = datetime.fromtimestamp(period.start_ts, self.tz).strftime(DT_FMT)
    e_local = datetime.fromtimestamp(period.end_ts, self.tz).strftime(DT_FMT)
    c.drawCentredString(page_w / 2, top, f"{s_local} — {e_local}")

  @staticmethod
  def _draw_widget_header(c: canvas.Canvas, x: float, y_top: float, w: float, h: float, title: str, font_main: str):
    # Header height: min(6mm, 12% of widget height), not less than 4mm
    header_h = max(4 * mm, min(6 * mm, 0.12 * h))
    c.setFillColor(W_HEADER_BG)
    c.setStrokeColor(W_HEADER_BG)
    c.rect(x, y_top - header_h, w, header_h, stroke=0, fill=1)
    if title:
      c.setFillColor(colors.black)
      c.setFont(font_main, 10)
      c.drawString(x + 3 * mm, y_top - (header_h * 0.65), title)
    return header_h

  @staticmethod
  def _draw_widget_frame(c: canvas.Canvas, x: float, y_top: float, w: float, h: float):
    c.setStrokeColor(W_HEADER_BG)
    c.rect(x, y_top - h, w, h, stroke=1, fill=0)

  @staticmethod
  def _draw_image_into_rect(c: canvas.Canvas, img_bytes: bytes,
                            x: float, y_top: float, w: float, h: float, pad_top: float):
    img = ImageReader(io.BytesIO(img_bytes))
    iw, ih = img.getSize()
    avail_w = max(1.0, w - 2.0 * mm)
    avail_h = max(1.0, h - pad_top - 2.0 * mm)
    scale = min(avail_w / iw, avail_h / ih)
    dw = iw * scale
    dh = ih * scale
    cx = x + (w - dw) * 0.5
    cy = (y_top - pad_top) - dh
    c.drawImage(img, cx, cy, width=dw, height=dh, preserveAspectRatio=True, mask='auto')

  @staticmethod
  def _draw_text_into_rect(c: canvas.Canvas, lines: List[str],
                           x: float, y_top: float, w: float, h: float, pad_top: float):
    line_h = 4.8 * mm
    max_lines = int((h - pad_top - 2 * mm) // line_h)
    visible = lines[:max_lines] if max_lines > 0 else []
    c.setFont(F_REG, 9)
    y = y_top - pad_top - 2.0 * mm
    for ln in visible:
      c.drawString(x + 2.0 * mm, y, ln)
      y -= line_h

  @staticmethod
  def _draw_problems_bars(c: canvas.Canvas, counts: Dict[int, int], total: int,
                          x: float, y_top: float, w: float, h: float, pad_top: float, horizontal: bool = True):
    order = [5, 4, 3]
    ru = {5: "Чрезвычайная", 4: "Высокая", 3: "Средняя"}
    items = [(sev, counts.get(sev, 0)) for sev in order]
    if horizontal:
      cell_h = max(1.0, h - pad_top - 2 * mm)
      cell_w = (w - 2 * mm) / len(items)
      y = y_top - pad_top - cell_h
      for i, (sev, cnt) in enumerate(items):
        cx = x + 1 * mm + i * cell_w
        c.setFillColor(SEV_COLORS.get(sev, colors.gray))
        c.setStrokeColor(colors.black)
        c.rect(cx, y, cell_w - 2.0, cell_h, stroke=0, fill=1)
        c.setFillColor(colors.white)
        c.setFont(F_BOLD, 16)
        c.drawCentredString(cx + (cell_w - 2.0) / 2, y + cell_h - 32, ru[sev])
        c.setFont(F_REG, 18)
        c.drawCentredString(cx + (cell_w - 2.0) / 2, y + cell_h/2 - 10, f"{cnt} из {total}")
      return
    else:
      # unchanged vertical variant, but RU labels + “из”
      cell_h = (h - pad_top - 2 * mm) / len(items)
      cell_w = w - 2 * mm
      y = y_top - pad_top - cell_h
      for i, (sev, cnt) in enumerate(items):
        cx = x + 1 * mm
        c.setFillColor(SEV_COLORS.get(sev, colors.gray))
        c.setStrokeColor(colors.black)
        c.rect(cx, y, cell_w, cell_h - 2.0, stroke=1, fill=1)
        c.setFillColor(colors.white)
        c.setFont(F_BOLD, 10)
        c.drawString(cx + 3, y + cell_h - 14, ru[sev])
        c.setFont(F_REG, 9)
        c.drawRightString(cx + cell_w - 3, y + 6, f"{cnt} из {total}")
        y -= cell_h

  @staticmethod
  def _draw_item_value_widget(c: canvas.Canvas, item_name: str, units: str,
                              last_value: Optional[float], ts_label: str,
                              x: float, y_top: float, w: float, h: float, pad_top: float):
    # Top time
    c.setFont(F_REG, 14)
    c.setFillColor(colors.black)
    c.drawCentredString(x + w / 2, y_top - pad_top - 20, ts_label)

    # Decide integer vs float
    whole_txt, dec_txt = "-", ""
    if last_value is not None:
      if units == "uptime":
        whole_txt = fmt_uptime(last_value)
        dec_txt = ""
      elif abs(last_value - round(last_value)) < 1e-6:
        whole_txt = f"{int(round(last_value))}"
        dec_txt = ""
      else:
        whole_txt, dec_txt = f"{last_value:.2f}".split(".")

    # Baseline y for big number
    base_y = y_top - pad_top - (h * 0.42)

    # Measure and center whole + decimals
    big_size = max(12, int(min(h, w) / 4))
    small_size = max(9, int(big_size * 0.55))

    c.setFont(F_BOLD, big_size)
    whole_w = c.stringWidth(whole_txt, F_BOLD, big_size)

    dot_w = dec_w = 0.0
    if dec_txt:
      c.setFont(F_BOLD, small_size)
      dot_w = c.stringWidth(".", F_BOLD, small_size)
      dec_w = c.stringWidth(dec_txt, F_BOLD, small_size)

    total_w = whole_w + (dot_w + dec_w if dec_txt else 0.0)
    start_x = x + (w - total_w) / 2.0

    # Draw whole
    c.setFont(F_BOLD, big_size)
    c.drawString(start_x, base_y - big_size/2, whole_txt)
    # Draw decimals aligned to top of digits
    if dec_txt:
      c.setFont(F_BOLD, small_size)
      dy = (big_size - small_size) * 0.35
      c.drawString(start_x + whole_w, base_y + dy, ".")
      c.drawString(start_x + whole_w + dot_w, base_y + dy, dec_txt)

    # Units
    if units:
      if units != "uptime":
        c.setFont(F_REG, 9)
        c.drawCentredString(x + w / 2, base_y - 12, units)

    # Bottom description
    c.setFont(F_REG, 14)
    c.drawCentredString(x + w / 2, (y_top - h) + 12, item_name)

    # Cache helpers
  def dashboard_meta_cached(self, dashboard_id: int, ttl_sec: int = REPORT_META_TTL_SEC) -> dict:
    now = datetime.now(timezone.utc).timestamp()
    cached = self._meta_cache.get(dashboard_id)
    if cached and (now - cached[0] <= ttl_sec):
      return cached[1]
    data = self._dashboard_meta(dashboard_id)
    self._meta_cache[dashboard_id] = (now, data)
    return data

  def pages_with_widgets_cached(self, dashboard_id: int, ttl_sec: int = REPORT_WIDGETS_TTL_SEC) -> List[
    DashboardPage]:
    now = datetime.now(timezone.utc).timestamp()
    cached = self._pages_cache.get(dashboard_id)
    if cached and (now - cached[0] <= ttl_sec):
      return cached[1]
    data = self._pages_with_widgets(dashboard_id)
    self._pages_cache[dashboard_id] = (now, data)
    return data

  # Persistent path helpers
  def _report_filename(self, period_type: str, period: ReportPeriod) -> str:
    start_local = datetime.fromtimestamp(period.start_ts, self.tz)
    if period_type == "month":
      return f"month_{start_local.strftime('%Y-%m')}.pdf"
    if period_type == "week":
      iso_year, iso_week, _ = start_local.isocalendar()
      return f"week_{iso_year}-W{iso_week:02d}.pdf"
    return f"period_{period.start_ts}-{period.end_ts}.pdf"

  def resolve_report_path(self, storage_dir: Path, dashboard_id: int, period_type: str, period: ReportPeriod) -> Path:
    base = Path(storage_dir) / f"dash_{dashboard_id}" / period_type
    base.mkdir(parents=True, exist_ok=True)
    return base / self._report_filename(period_type, period)

  def generate_dashboard_pdf(self, dashboard_id: int, period: ReportPeriod, output_path: str) -> None:
    meta = self.dashboard_meta_cached(dashboard_id)
    # dash_name = str(meta.get("name") or f"Dashboard {dashboard_id}")
    pages = self.pages_with_widgets_cached(dashboard_id)
    if not pages:
      pages = [DashboardPage("0", "", 0, [])]

    page_w = 508 * mm
    page_h = 818 * mm
    pagesize = (page_w, page_h)
    margin = 2 * mm
    header_h = 14 * mm
    content_left = margin
    content_right = page_w - margin
    content_top = page_h - margin
    content_bottom = margin

    c = canvas.Canvas(output_path, pagesize=pagesize)

    for page in pages:
      # header
      self._draw_header(c, period, content_top - header_h/2, page_w)
      # content area
      content_w = content_right - content_left
      content_h = (content_top - header_h) - content_bottom

      # compute grid units BEFORE using them
      max_row = 0
      for w in page.widgets:
        max_row = max(max_row, int(w.y + w.height))

      # at least 1 row to avoid division by zero (Zabbix grid is 24 columns)
      total_rows = max(1, max_row)
      col_unit = content_w / GRID_COLS
      row_unit = content_h / total_rows

      for w in page.widgets:
        wx = content_left + w.x * col_unit
        wy_top = (content_top - header_h) - w.y * row_unit
        ww = w.width * col_unit
        wh = w.height * row_unit

        self._draw_widget_frame(c, wx, wy_top, ww, wh)

        pad_top = 0.0
        title = w.name or ""
        if w.view_mode != 1 and title:
          pad_top = float(self._draw_widget_header(c, wx, wy_top, ww, wh, title, F_BOLD))
        else:
          # No header (hidden or empty title). Keep a minimal inner padding.
          pad_top = 0.0

        wtype = (w.type or "").strip().lower()
        try:
          if wtype == "graph":
            source_type = int(w.params.get("source_type", 0) or 0)
            req_w, req_h, inner_w, inner_h = self._px_dims(ww, wh, pad_top)
            if source_type == 0:
              graphid = str(w.params.get("graphid") or "")
              if graphid:
                img = self._chart2_png(graphid, period.start_ts, period.end_ts, req_w, req_h)
                self._draw_image_fill(c, img, wx, wy_top, inner_w, inner_h, pad_top)
                continue
            else:
              itemid = str(w.params.get("itemid") or "")
              if itemid:
                img = self._chart_items_png([itemid], period.start_ts, period.end_ts, req_w, req_h)
                self._draw_image_fill(c, img, wx, wy_top, inner_w, inner_h, pad_top)
                continue

          if wtype in ("problemsbysv", "problems_severity"):
            counts = self._problems_totals(w.params)
            total = sum(counts.get(k, 0) for k in range(0, 6))
            layout = int(w.params.get("layout", 0) or 0)  # 0 horiz
            self._draw_problems_bars(c, counts, total, wx, wy_top, ww, wh, pad_top, horizontal=(layout == 0))
            continue

          if wtype in ("item", "single_item", "widget.item"):
            raw = w.params.get("itemids") or w.params.get("itemid")
            itemids = [str(raw)] if raw and not isinstance(raw, list) else [str(x) for x in (raw or [])]
            stats = self._get_item_stats(itemids, period.start_ts, period.end_ts)
            iid = itemids[0] if itemids else None
            st = stats.get(iid) if iid else None
            ts = st.get("last_clock") if st else None
            ts_label = datetime.fromtimestamp(ts or period.end_ts, self.tz).strftime(DT_FMT)
            self._draw_item_value_widget(
              c,
              (st or {}).get("name") or (title or "Item"),
              (st or {}).get("units") or "",
              (st or {}).get("last"),
              ts_label,
              wx, wy_top, ww, wh, pad_top
            )
            continue

          if wtype in ("plaintext", "widget.plaintext", "plain_text"):
            text = str(w.params.get("text") or "").strip()
            lines = [ln for ln in text.splitlines() if ln.strip()]
            self._draw_text_into_rect(c, lines or ["(empty)"], wx, wy_top, ww, wh, pad_top)
            continue

          self._draw_text_into_rect(c, [f"(type '{w.type}' not rendered)"], wx, wy_top, ww, wh, pad_top)

        except Exception as e:
          self._draw_text_into_rect(c, [f"Render error: {e}"], wx, wy_top, ww, wh, pad_top)


      c.showPage()

    c.save()

  async def ensure_report_file(self, db: UserDB, dashboard_id: int, period_type: str,
                               period: ReportPeriod, storage_dir: Path = REPORT_STORAGE_DIR) -> Path:
    # DB fast-path
    existing = await db.get_report_record(dashboard_id, period_type, period.start_ts)
    if existing:
      logger.info(f"Found report record for {dashboard_id} {period_type} {period.start_ts}")
      path, _file_id, _end_ts = existing
      p = Path(path)
      if p.exists():
        logger.info(f"Found existing report file for {dashboard_id} {period_type} {period.start_ts}")
        return p
    out_path = self.resolve_report_path(storage_dir, dashboard_id, period_type, period)
    if out_path.exists():
      logger.info(f"Resolved report path for {dashboard_id} {period_type} {period.start_ts}: {out_path}")
      await db.upsert_report_path(dashboard_id, period_type, period.start_ts, period.end_ts, str(out_path))
      return out_path

    # Generate in a thread to keep bot responsive
    out_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Generating report for {dashboard_id} {period_type} {period.start_ts}")
    await asyncio.to_thread(self.generate_dashboard_pdf, dashboard_id, period, str(out_path))
    logger.info(f"Generated report for {dashboard_id} {period_type} {period.start_ts}")
    await db.upsert_report_path(dashboard_id, period_type, period.start_ts, period.end_ts, str(out_path))
    logger.info(f"Upserted report record for {dashboard_id} {period_type} {period.start_ts}")
    return out_path
