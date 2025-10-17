import json
import os
from pathlib import Path


def _bool(val: str, default: bool = True) -> bool:
  if val is None:
    return default
  return val.lower() in ("1", "true", "yes", "on")


BASE_DIR = Path(os.getenv("MONBOT_BASE_DIR", ".")).resolve()
CACHE_DIR = Path(os.getenv("MONBOT_CACHE_DIR", ".cache")).resolve()
DB_PATH = Path(os.getenv("MONBOT_DB_PATH", BASE_DIR / "monbot.db")).resolve()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")  # required

# Zabbix
ZABBIX_URL = os.getenv("ZABBIX_URL", "http://zabbix-web:8080/")
ZABBIX_USER = os.getenv("ZABBIX_USER", "api_tg")
ZABBIX_PASS = os.getenv("ZABBIX_PASS", "change-me")
ZABBIX_API_TOKEN = os.getenv("ZABBIX_API_TOKEN", "")  # recommended to set
ZABBIX_VERIFY_SSL = _bool(os.getenv("ZABBIX_VERIFY_SSL", "true"), True)

# How to send API token: 'auto' (try header then body), 'header', 'body'
ZABBIX_TOKEN_MODE = os.getenv("ZABBIX_TOKEN_MODE", "auto").lower()

# Suppress urllib3 InsecureRequestWarning if VERIFY_SSL is false
SUPPRESS_TLS_WARN = _bool(os.getenv("SUPPRESS_TLS_WARN", "true"), True)

# Allowed hosts mapping: {"hostid":"DisplayName", ...}
ALLOW_HOSTS = os.getenv("ALLOW_HOSTS", '{"10263":"RT","10266":"Freez"}')
try:
  ALLOW_HOSTS = json.loads(ALLOW_HOSTS)
except Exception:
  ALLOW_HOSTS = {"10263": "RT", "10266": "Freez"}

# Initial admins (comma-separated Telegram user ids)
INITIAL_ADMINS = os.getenv("INITIAL_ADMINS", "395544470,839618968,226090226")
INITIAL_ADMINS = [int(x.strip()) for x in INITIAL_ADMINS.split(",") if x.strip().isdigit()]

# UI and graph defaults
TIME_RANGES = ["1w", "48h", "24h", "12h", "6h", "3h", "1h", "30m", "15m"]
IMG_WIDTH = int(os.getenv("IMG_WIDTH", "512"))
IMG_HEIGHT = int(os.getenv("IMG_HEIGHT", "512"))

MAINT_LIST_LIMIT = int(os.getenv("MAINT_LIST_LIMIT", "5"))
DEFAULT_TZ = os.getenv("DEFAULT_TZ", "Europe/Moscow")
MAINT_TAG_KEY = os.getenv("MAINT_TAG_KEY", "channel")
ITEMS_REFRESH_SEC = int(os.getenv("ITEMS_REFRESH_SEC", "3600"))

# Report storage and cache
REPORT_STORAGE_DIR = Path(os.getenv("MONBOT_REPORTS_DIR", "/reports")).resolve()
REPORT_META_TTL_SEC = int(os.getenv("REPORT_META_TTL_SEC", "3600"))
REPORT_WIDGETS_TTL_SEC = int(os.getenv("REPORT_WIDGETS_TTL_SEC", "3600"))

# Default dashboard for reports
REPORT_DASHBOARD_ID = int(os.getenv("REPORT_DASHBOARD_ID", "19"))

# Reports pre-generation (how many completed periods back to ensure)
REPORT_PREGEN_WEEKS = int(os.getenv("REPORT_PREGEN_WEEKS", "8"))
REPORT_PREGEN_MONTHS = int(os.getenv("REPORT_PREGEN_MONTHS", "12"))
