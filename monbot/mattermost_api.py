from __future__ import annotations

import json
from typing import Any, Optional

import requests


class MattermostAPI:
  def __init__(self, base_url: str, bot_token: str, timeout: int = 30):
    self.base_url = base_url.rstrip("/")
    self.session = requests.Session()
    self.session.headers.update({
      "Authorization": f"Bearer {bot_token}",
      "Content-Type": "application/json",
    })
    self.timeout = timeout

  def request(self, method: str, path: str, *, json_body: Any | None = None, data: Any | None = None,
              params: Any | None = None, files: Any | None = None) -> requests.Response:
    url = f"{self.base_url}/api/v4{path}"
    kwargs: dict[str, Any] = {"timeout": self.timeout}
    if params is not None:
      kwargs["params"] = params
    if json_body is not None:
      kwargs["data"] = json.dumps(json_body)
    elif data is not None:
      kwargs["data"] = data
    if files is not None:
      kwargs["files"] = files
      kwargs.pop("data", None)
      self.session.headers.pop("Content-Type", None)
    try:
      resp = self.session.request(method, url, **kwargs)
      resp.raise_for_status()
      return resp
    finally:
      if files is not None:
        self.session.headers["Content-Type"] = "application/json"

  def get_json(self, path: str, *, params: Optional[dict[str, Any]] = None) -> Any:
    resp = self.request("GET", path, json_body=None, data=None, params=params)
    return resp.json()

  def get_me(self) -> dict[str, Any]:
    return self.request("GET", "/users/me").json()

  def get_user(self, user_id: str) -> dict[str, Any]:
    return self.request("GET", f"/users/{user_id}").json()

  def get_team_by_name(self, team_name: str) -> dict[str, Any]:
    return self.request("GET", f"/teams/name/{team_name}").json()

  def get_team(self, team_id: str) -> dict[str, Any]:
    return self.request("GET", f"/teams/{team_id}").json()

  def get_channel(self, channel_id: str) -> dict[str, Any]:
    return self.request("GET", f"/channels/{channel_id}").json()

  def list_commands(self, team_id: str, custom_only: bool = True) -> list[dict[str, Any]]:
    return self.request("GET", "/commands", params={"team_id": team_id, "custom_only": str(custom_only).lower()}).json()

  def create_command(self, payload: dict[str, Any]) -> dict[str, Any]:
    return self.request("POST", "/commands", json_body=payload).json()

  def update_command(self, command_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return self.request("PUT", f"/commands/{command_id}", json_body=payload).json()

  def delete_command(self, command_id: str) -> dict[str, Any]:
    return self.request("DELETE", f"/commands/{command_id}").json()

  def search_users(self, term: str, in_team: str | None = None) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {"term": term}
    if in_team:
      payload["in_team"] = in_team
    return self.request("POST", "/users/search", json_body=payload).json()

  def upload_file(self, channel_id: str, filename: str, data: bytes, content_type: str = "image/png") -> dict[str, Any]:
    files = {"files": (filename, data, content_type)}
    resp = self.request("POST", f"/files?channel_id={channel_id}", files=files)
    return resp.json()

  def create_post(self, post: dict[str, Any]) -> dict[str, Any]:
    return self.request("POST", "/posts", json_body=post).json()

  def update_post(self, post_id: str, post: dict[str, Any]) -> dict[str, Any]:
    return self.request("PUT", f"/posts/{post_id}", json_body=post).json()

  def create_direct_channel(self, user_ids: list[str]) -> dict[str, Any]:
    return self.request("POST", "/channels/direct", json_body=user_ids).json()

  def open_dialog(self, trigger_id: str, url: str, dialog: dict[str, Any]) -> dict[str, Any]:
    return self.request("POST", "/actions/dialogs/open", json_body={
      "trigger_id": trigger_id,
      "url": url,
      "dialog": dialog,
    }).json()
