import json
import logging
from typing import Any, Iterable, List, Optional

import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning

from monbot.config import SUPPRESS_TLS_WARN, ZABBIX_HTTP_TIMEOUT, ZABBIX_TOKEN_MODE, ZABBIX_VERIFY_SSL

logger = logging.getLogger(__name__)


class ZabbixWeb:
  def __init__(self, server: str, username: str, password: str, api_token: str,
               verify: bool = ZABBIX_VERIFY_SSL, proxies: Optional[dict] = None):
    self.server = server.rstrip("/") + "/"
    self.username = username
    self.password = password
    self.api_token = api_token
    self.verify = verify
    self.proxies = proxies or {}
    self.session = requests.Session()
    self.session.verify = verify
    self.session.proxies.update(self.proxies)
    self.timeout = ZABBIX_HTTP_TIMEOUT
    if not self.verify and SUPPRESS_TLS_WARN:
      urllib3.disable_warnings(category=InsecureRequestWarning)

  def login(self):
    data = {"name": self.username, "password": self.password, "enter": "Sign in"}
    r = self.session.post(self.server, data=data, allow_redirects=True, timeout=self.timeout)
    if not r.cookies:
      logger.error("Zabbix login failed: status=%s", r.status_code)
      raise RuntimeError("Zabbix login failed")

  def api_request(self, method: str, params: dict) -> Any:
    url = self.server + "api_jsonrpc.php"
    base_headers = {"Content-Type": "application/json-rpc"}
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}

    def do_request(headers: dict, body: dict) -> requests.Response:
      return self.session.post(url, data=json.dumps(body), headers=headers, timeout=self.timeout)

    tried = []

    # Try modes in order depending on config
    modes = {
      "header": ["header"],
      "body": ["body"],
      "auto": ["header", "body"],
    }.get(ZABBIX_TOKEN_MODE, ["header", "body"])

    last_exc: Optional[Exception] = None
    for mode in modes:
      headers = dict(base_headers)
      body = dict(payload)
      if self.api_token:
        if mode == "header":
          headers["Authorization"] = f"Bearer {self.api_token}"
        elif mode == "body":
          body["auth"] = self.api_token
      tried.append(mode)
      try:
        r = do_request(headers, body)
        if r.status_code >= 400:
          body_preview = (r.text or "")[:200]
          if r.status_code in (401, 403):
            body_preview = "<suppressed>"
          logger.warning("Zabbix API HTTP %s mode=%s; body=%s", r.status_code, mode, body_preview)
          if ZABBIX_TOKEN_MODE == "auto" and mode == "header":
            continue
          r.raise_for_status()
        data = r.json()
        if "error" in data:
          # Zabbix-level error (200 OK but API error)
          raise RuntimeError(f"Zabbix API error: {data['error']}")
        return data["result"]
      except requests.HTTPError as e:
        last_exc = e
        if ZABBIX_TOKEN_MODE == "auto" and mode == "header":
          continue
        raise
      except requests.Timeout as e:
        last_exc = e
        if ZABBIX_TOKEN_MODE == "auto" and mode == "header":
          continue
        raise
      except ValueError as e:
        last_exc = e
        if ZABBIX_TOKEN_MODE == "auto" and mode == "header":
          continue
        raise

    raise RuntimeError(f"Zabbix API request failed after trying modes {tried}") from last_exc

  def get_items(self, host_ids: Iterable[str]) -> List[dict]:
    params = {
      "output": "extend",
      "hostids": list(host_ids),
      "search": {"units": "\u00b0C"},
      "startSearch": True,
    }
    return self.api_request("item.get", params)
