from __future__ import annotations

import asyncio
import json
import logging
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from monbot.config import MM_BIND_HOST, MM_BIND_PORT
from monbot.mattermost_service import MattermostIntegration

logger = logging.getLogger(__name__)


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict | bytes, content_type: str = "application/json"):
  handler.send_response(status)
  handler.send_header("Content-Type", content_type)
  handler.end_headers()
  if isinstance(payload, bytes):
    handler.wfile.write(payload)
  else:
    handler.wfile.write(json.dumps(payload, ensure_ascii=False).encode("utf-8"))


def _parse_body(handler: BaseHTTPRequestHandler) -> tuple[dict, bytes]:
  length = int(handler.headers.get("Content-Length") or 0)
  body = handler.rfile.read(length) if length > 0 else b""
  ctype = (handler.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
  if ctype == "application/json":
    if not body:
      return {}, body
    return json.loads(body.decode("utf-8")), body
  if ctype == "application/x-www-form-urlencoded" or ctype == "multipart/form-data":
    raw = parse_qs(body.decode("utf-8"))
    data = {k: v[-1] if len(v) == 1 else v for k, v in raw.items()}
    if "payload" in data:
      return json.loads(data["payload"]), body
    return data, body
  if not body:
    return {}, body
  try:
    return json.loads(body.decode("utf-8")), body
  except Exception:
    raw = parse_qs(body.decode("utf-8"))
    data = {k: v[-1] if len(v) == 1 else v for k, v in raw.items()}
    if "payload" in data:
      return json.loads(data["payload"]), body
    return data, body


def _make_handler(integration: MattermostIntegration):
  class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
      logger.info("%s - %s", self.address_string(), fmt % args)

    def do_GET(self):
      parsed = urlparse(self.path)
      if parsed.path == "/healthz":
        return _json_response(self, 200, {"ok": True})
      m = re.match(r"^/mm/assets/([a-f0-9]+)\.(png|pdf)$", parsed.path)
      if m:
        token, ext = m.group(1), m.group(2)
        status, data, ctype = asyncio.run(integration.handle_asset(token, ext))
        return _json_response(self, status, data, ctype)
      _json_response(self, 404, {"error": "not found"})

    def do_POST(self):
      parsed = urlparse(self.path)
      path = parsed.path.rstrip("/")
      if integration.secret:
        prefix = f"/mm/{integration.secret}"
        if not path.startswith(prefix):
          return _json_response(self, 404, {"error": "not found"})
        sub = path[len(prefix):] or "/"
      else:
        if not path.startswith("/mm"):
          return _json_response(self, 404, {"error": "not found"})
        sub = path[len("/mm"):] or "/"

      try:
        payload, _raw = _parse_body(self)
      except Exception as exc:
        logger.exception("Failed to parse request body")
        return _json_response(self, 400, {"error": str(exc)})

      try:
        if sub == "/command":
          resp = asyncio.run(integration.handle_command(payload))
          return _json_response(self, 200, resp)
        if sub == "/action":
          resp = asyncio.run(integration.handle_action(payload))
          return _json_response(self, 200, resp)
        if sub == "/dialog":
          resp = asyncio.run(integration.handle_dialog(payload))
          return _json_response(self, 200, resp)
      except Exception as exc:
        logger.exception("Mattermost request failed")
        return _json_response(self, 200, {"error": {"message": str(exc)}})

      return _json_response(self, 404, {"error": "not found"})

  return Handler


def main() -> None:
  logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
  integration = MattermostIntegration()
  asyncio.run(integration.startup())
  server = ThreadingHTTPServer((MM_BIND_HOST, MM_BIND_PORT), _make_handler(integration))
  logger.info("Mattermost integration listening on %s:%s", MM_BIND_HOST, MM_BIND_PORT)
  try:
    server.serve_forever()
  except KeyboardInterrupt:
    pass
  finally:
    server.server_close()


if __name__ == "__main__":
  main()
