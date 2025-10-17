import logging

from dotenv import find_dotenv, load_dotenv
# Load .env before importing monbot.config
load_dotenv(find_dotenv(), override=False)

import asyncio

from datetime import date, datetime, timedelta
from datetime import time as dtime
from zoneinfo import ZoneInfo
from telegram.ext import (
  Application,
  CallbackContext,
  CallbackQueryHandler,
  CommandHandler,
  ConversationHandler,
  MessageHandler,
  filters,
)

from monbot.db import UserDB
from monbot.cache2 import ImageCache2
from monbot.config import *
from monbot.graph_service import GraphService
from monbot.handlers.commands import (
  adduser, deluser, report_send_action, setrole,
  help_cmd, invgen, listusers,
  refresh, report_cmd, report_confirm_action,
  settz, start_graphs, start_maint,
  start_register,
)
from monbot.handlers.common import error_handler
from monbot.handlers.consts import *
from monbot.handlers.graphs import (
  open_maint_from_graph,
  host_handler as graph_host_handler,
  item_handler,
  start_over
)
from monbot.handlers.maintenance import (
  maint_action,
  host_handler as maint_host_handler,
  maint_select_item,
  maint_handle_text,
  open_graph_from_maint
)
from monbot.items_index import ItemsIndex
from monbot.logging_conf import setup_logging
from monbot.maintenance_service import MaintenanceService
from monbot.render import SkiaRenderer
from monbot.zabbix import ZabbixWeb
from monbot.zbx_data import ZbxDataClient
from monbot.report_service import ReportPeriod, ReportService

logger = logging.getLogger(__name__)

async def post_init(application: Application) -> None:
  # Initialize shared services and store in bot_data
  db = UserDB(DB_PATH)
  await db.init()
  await db.ensure_admins(INITIAL_ADMINS)

  zbx = ZabbixWeb(
    server=ZABBIX_URL,
    username=ZABBIX_USER,
    password=ZABBIX_PASS,
    api_token=ZABBIX_API_TOKEN,
    verify=ZABBIX_VERIFY_SSL,
  )
  # Blocking zabbix login off the loop
  await asyncio.to_thread(zbx.login)

  cache2 = ImageCache2(CACHE_DIR)
  zbx_client = ZbxDataClient(zbx)
  renderer = SkiaRenderer()
  gsvc = GraphService(zbx_client, cache2, renderer)
  msvc = MaintenanceService(zbx, tag_key=MAINT_TAG_KEY)

  application.bot_data[CTX_MAINT_SVC] = msvc
  application.bot_data[CTX_CACHE2] = cache2
  application.bot_data[CTX_GRAPH_SVC] = gsvc
  application.bot_data[CTX_DB] = db
  application.bot_data[CTX_ZBX] = zbx
  application.bot_data[CTX_ALLOW_HOSTS] = ALLOW_HOSTS
  items = ItemsIndex(zbx, ALLOW_HOSTS)
  await items.refresh()
  application.bot_data[CTX_ITEMS] = items

  async def _refresh_items_job(_: CallbackContext):
    try:
      await items.refresh()
    except Exception:
      pass

  async def _reports_job(ctx: CallbackContext):
    db: UserDB = ctx.application.bot_data[CTX_DB]
    zbx = ctx.application.bot_data[CTX_ZBX]
    tz = ZoneInfo(DEFAULT_TZ)
    svc = ReportService(zbx, tz=tz)

    # Last completed week: [start_of_this_week - 7d, start_of_this_week)
    now = datetime.now(tz)
    monday_this = (now.date() - timedelta(days=(now.isoweekday() - 1)))
    prev_monday = monday_this - timedelta(days=7)
    s_w, e_w, _ = svc.week_bounds_by_any_date(prev_monday)
    period_w = ReportPeriod(s_w, e_w, "")
    await svc.ensure_report_file(db, REPORT_DASHBOARD_ID, "week", period_w, REPORT_STORAGE_DIR)

    # Last completed month: if today is 1st generate previous month; run daily, the upsert will no-op if exists
    first_this = date(now.year, now.month, 1)
    last_prev_date = first_this - timedelta(days=1)
    s_m, e_m, _ = svc.month_bounds_by_any_date(last_prev_date)
    period_m = ReportPeriod(s_m, e_m, "")
    await svc.ensure_report_file(db, REPORT_DASHBOARD_ID, "month", period_m, REPORT_STORAGE_DIR)

  async def _pregen_reports_job(ctx: CallbackContext):
    db: UserDB = ctx.application.bot_data[CTX_DB]
    zbx = ctx.application.bot_data[CTX_ZBX]
    tz = ZoneInfo(DEFAULT_TZ)
    svc = ReportService(zbx, tz=tz)

    # Weeks: last REPORT_PREGEN_WEEKS completed weeks
    now = datetime.now(tz)
    monday_this = now.date() - timedelta(days=(now.isoweekday() - 1))
    for i in range(1, max(0, REPORT_PREGEN_WEEKS) + 1):
      monday = monday_this - timedelta(days=7 * i)
      s, e, _ = svc.week_bounds_by_any_date(monday)
      period = ReportPeriod(start_ts=s, end_ts=e, label="")
      try:
        await svc.ensure_report_file(db, REPORT_DASHBOARD_ID, "week", period, REPORT_STORAGE_DIR)
      except Exception:
        pass

    # Months: last REPORT_PREGEN_MONTHS completed months
    y, m = now.year, now.month
    for i in range(REPORT_PREGEN_MONTHS):
      m -= 1
      if m == 0:
        m, y = 12, y - 1
      s, e, _ = svc.month_bounds_by_any_date(date(y, m, 15))
      period = ReportPeriod(start_ts=s, end_ts=e, label="")
      try:
        await svc.ensure_report_file(db, REPORT_DASHBOARD_ID, "month", period, REPORT_STORAGE_DIR)
      except Exception:
        pass

  application.job_queue.run_once(_pregen_reports_job, when=1)
  application.job_queue.run_repeating(
    _refresh_items_job,
    interval=ITEMS_REFRESH_SEC, first=ITEMS_REFRESH_SEC
  )

  application.job_queue.run_daily(
    _reports_job, time=dtime(hour=0, minute=10, tzinfo=ZoneInfo(DEFAULT_TZ))
  )


def main() -> None:
  setup_logging()
  if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN not set")

  application = (
    Application.builder()
    .token(TELEGRAM_TOKEN)
    .post_init(post_init)  # <- will run before polling starts
    .build()
  )
  # commands
  application.add_handler(CommandHandler("start", start_register))
  application.add_handler(CommandHandler("help", help_cmd))
  application.add_handler(CommandHandler("graphs", start_graphs))
  application.add_handler(CommandHandler("settz", settz))
  application.add_handler(CommandHandler("maint", start_maint))
  application.add_handler(CommandHandler("invgen", invgen))
  application.add_handler(CommandHandler("adduser", adduser))
  application.add_handler(CommandHandler("deluser", deluser))
  application.add_handler(CommandHandler("setrole", setrole))
  application.add_handler(CommandHandler("listusers", listusers))
  application.add_handler(CommandHandler("report", report_cmd))

  application.add_handler(CommandHandler("refresh", refresh))
  conv_handler = ConversationHandler(
    entry_points=[
      CallbackQueryHandler(graph_host_handler, pattern=PAT_GRAPH_HOST),
      CallbackQueryHandler(item_handler, pattern=PAT_GRAPH_ITEM),
      CallbackQueryHandler(start_over, pattern=rf"^{CB_RESTART}$"),
      CallbackQueryHandler(open_maint_from_graph, pattern=PAT_GO_MAINT),
      CallbackQueryHandler(open_graph_from_maint, pattern=PAT_GO_GRAPH),
      CallbackQueryHandler(maint_host_handler, pattern=PAT_MAINT_HOST),
      CallbackQueryHandler(maint_select_item, pattern=PAT_MAINT_ITEM_OR_BACK_HOST),
      CallbackQueryHandler(maint_action, pattern=PAT_MAINT_ACTIONS),
      CallbackQueryHandler(report_confirm_action, pattern=PAT_REPORT_CONFIRM),
      CallbackQueryHandler(report_confirm_action, pattern=rf"^{CB_REPORT_CANCEL}$"),
      CallbackQueryHandler(report_send_action, pattern=PAT_REPORT_SEND),
    ],
    states={
      SELECTING: [
        CallbackQueryHandler(graph_host_handler, pattern=PAT_GRAPH_HOST),
        CallbackQueryHandler(item_handler, pattern=PAT_GRAPH_ITEM),
        CallbackQueryHandler(start_over, pattern=rf"^{CB_RESTART}$"),
        CallbackQueryHandler(open_maint_from_graph, pattern=PAT_GO_MAINT),
        CallbackQueryHandler(open_graph_from_maint, pattern=PAT_GO_GRAPH),
        CallbackQueryHandler(maint_host_handler, pattern=PAT_MAINT_HOST),
        CallbackQueryHandler(maint_select_item, pattern=PAT_MAINT_ITEM_OR_BACK_HOST),
        CallbackQueryHandler(maint_action, pattern=PAT_MAINT_ACTIONS),
        CallbackQueryHandler(report_confirm_action, pattern=PAT_REPORT_CONFIRM),
        CallbackQueryHandler(report_confirm_action, pattern=rf"^{CB_REPORT_CANCEL}$"),
        CallbackQueryHandler(report_send_action, pattern=PAT_REPORT_SEND),
      ],
    },
    fallbacks=[CallbackQueryHandler(start_over, pattern=rf"^{CB_RESTART}$")],
    per_message=True,
  )
  application.add_handler(conv_handler)
  application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, maint_handle_text))
  application.add_error_handler(error_handler)
  application.run_polling()


if __name__ == "__main__":
  main()
