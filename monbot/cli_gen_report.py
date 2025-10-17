from __future__ import annotations

import logging
import os
from zoneinfo import ZoneInfo

from dotenv import find_dotenv, load_dotenv

# Load env early
load_dotenv(find_dotenv(), override=False)

from monbot.config import DEFAULT_TZ, REPORT_DASHBOARD_ID, ZABBIX_API_TOKEN, ZABBIX_PASS, ZABBIX_URL, ZABBIX_USER, ZABBIX_VERIFY_SSL
from monbot.report_service import ReportService
from monbot.zabbix import ZabbixWeb

logger = logging.getLogger(__name__)

def main():
  zbx = ZabbixWeb(
    server=ZABBIX_URL,
    username=ZABBIX_USER,
    password=ZABBIX_PASS,
    api_token=ZABBIX_API_TOKEN,
    verify=ZABBIX_VERIFY_SSL,
  )
  zbx.login()

  svc = ReportService(zbx, tz=ZoneInfo(DEFAULT_TZ))
  period = svc.last_month_period()
  out = os.path.abspath(f"zbx_report_{period.label.replace(' ', '_')}.pdf")
  svc.generate_dashboard_pdf(REPORT_DASHBOARD_ID, period, out)
  print(f"Report generated: {out}")


if __name__ == "__main__":
  main()
