from __future__ import annotations

import argparse
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
  parser = argparse.ArgumentParser(description="Generate dashboard PDF report")
  parser.add_argument("--period", choices=["week", "month"], default="week")
  parser.add_argument("--dashboard-id", type=int, default=REPORT_DASHBOARD_ID)
  parser.add_argument("--out", default="", help="Optional output PDF path")
  args = parser.parse_args()

  zbx = ZabbixWeb(
    server=ZABBIX_URL,
    username=ZABBIX_USER,
    password=ZABBIX_PASS,
    api_token=ZABBIX_API_TOKEN,
    verify=ZABBIX_VERIFY_SSL,
  )
  zbx.login()

  svc = ReportService(zbx, tz=ZoneInfo(DEFAULT_TZ))
  period = svc.last_week_period() if args.period == "week" else svc.last_month_period()
  if args.out:
    out = os.path.abspath(args.out)
  else:
    out = os.path.abspath(f"zbx_report_{args.period}_{period.label.replace(' ', '_')}.pdf")
  svc.generate_dashboard_pdf(args.dashboard_id, period, out)
  print(f"Report generated: {out}")


if __name__ == "__main__":
  main()
