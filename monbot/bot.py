from dotenv import find_dotenv, load_dotenv

# Load .env before importing monbot.config
load_dotenv(find_dotenv(), override=False)
# isort: split

import asyncio

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
  adduser, deluser,
  help_cmd, invgen, listusers,
  refresh, setrole, settz, start_graphs, start_maint, start_register,
)
from monbot.handlers.common import error_handler
from monbot.handlers.consts import *
from monbot.handlers.graphs import go_graph_from_maint, go_maint_from_graph, host_handler, item_handler, start_over
from monbot.handlers.maintenance import (
  maint_action,
  maint_select_host,
  maint_select_item,
  maint_text_input,
)
from monbot.items_index import ItemsIndex
from monbot.logging_conf import setup_logging
from monbot.maintenance_service import MaintenanceService
from monbot.render import SkiaRenderer
from monbot.zabbix import ZabbixWeb
from monbot.zbx_data import ZbxDataClient


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

  l1_mb = int(os.getenv("L1_CACHE_MB", "128"))
  cache2 = ImageCache2(CACHE_DIR, l1_max_bytes=l1_mb * 1024 * 1024, l2_max_bytes=1_000 * 1024 * 1024)
  zbx_client = ZbxDataClient(zbx)
  renderer = SkiaRenderer()
  gsvc = GraphService(zbx_client, cache2, renderer)
  msvc = MaintenanceService(zbx, tag_key=MAINT_TAG_KEY)

  application.bot_data[CTX_MSVC] = msvc
  application.bot_data[CTX_CACHE2] = cache2
  application.bot_data[CTX_GSVC] = gsvc
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

  application.job_queue.run_repeating(
    _refresh_items_job,
    interval=ITEMS_REFRESH_SEC, first=ITEMS_REFRESH_SEC
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
  application.add_handler(CommandHandler("refresh", refresh))
  host_pat = host_names_pattern(ALLOW_HOSTS)
  conv_handler = ConversationHandler(
    entry_points=[
      CallbackQueryHandler(host_handler, pattern=host_pat),
      CallbackQueryHandler(item_handler, pattern=PAT_GRAPH_ITEM),
      CallbackQueryHandler(start_over, pattern=rf"^{CB_RESTART}$"),
      CallbackQueryHandler(go_maint_from_graph, pattern=PAT_GO_MAINT),
      CallbackQueryHandler(go_graph_from_maint, pattern=PAT_GO_GRAPH),
      CallbackQueryHandler(maint_select_host, pattern=PAT_MAINT_HOST),
      CallbackQueryHandler(maint_select_item, pattern=PAT_MAINT_ITEM_OR_BACK_HOST),
      CallbackQueryHandler(maint_action, pattern=PAT_MAINT_ACTIONS),
    ],
    states={
      SELECTING: [
        CallbackQueryHandler(host_handler, pattern=host_pat),
        CallbackQueryHandler(item_handler, pattern=PAT_GRAPH_ITEM),
        CallbackQueryHandler(start_over, pattern=rf"^{CB_RESTART}$"),
        CallbackQueryHandler(go_maint_from_graph, pattern=PAT_GO_MAINT),
        CallbackQueryHandler(go_graph_from_maint, pattern=PAT_GO_GRAPH),
        CallbackQueryHandler(maint_select_host, pattern=PAT_MAINT_HOST),
        CallbackQueryHandler(maint_select_item, pattern=PAT_MAINT_ITEM_OR_BACK_HOST),
        CallbackQueryHandler(maint_action, pattern=PAT_MAINT_ACTIONS),
      ],
    },
    fallbacks=[CallbackQueryHandler(start_over, pattern=rf"^{CB_RESTART}$")],
    per_message=True,
  )
  application.add_handler(conv_handler)
  application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, maint_text_input))
  application.add_error_handler(error_handler)
  application.run_polling()


if __name__ == "__main__":
  main()
