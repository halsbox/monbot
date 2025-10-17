from __future__ import annotations
import json
from zoneinfo import ZoneInfo
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=False)

from monbot.config import DEFAULT_TZ, REPORT_DASHBOARD_ID
from monbot.zabbix import ZabbixWeb

def main():
  zbx = ZabbixWeb(
    server=os.getenv("ZABBIX_URL", ""),
    username=os.getenv("ZABBIX_USER", ""),
    password=os.getenv("ZABBIX_PASS", ""),
    api_token=os.getenv("ZABBIX_API_TOKEN", ""),
    verify=os.getenv("ZABBIX_VERIFY_SSL", "true").lower() in ("1","true","yes","on"),
  )
  zbx.login()

  # Fetch pages with widgets and fields
  res = zbx.api_request(
    "dashboard.get",
    {
      "output": ["dashboardid", "name"],
      "dashboardids": [REPORT_DASHBOARD_ID],
      "selectPages": ["dashboard_pageid", "name", "display_period", "widgets"],
    },
  )
  print(json.dumps(res, ensure_ascii=False, indent=2))

if __name__ == "__main__":
  import os
  main()
