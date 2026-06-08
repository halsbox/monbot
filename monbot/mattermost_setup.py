from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Any

from monbot.config import MM_BOT_TOKEN, MM_PUBLIC_URL, MM_TEAM, MM_URL, MM_WEBHOOK_SECRET
from monbot.mattermost_api import MattermostAPI


LEGACY_TRIGGERS = {
  "start",
  "help",
  "graphs",
  "maint",
  "settz",
  "refresh",
  "report",
  "audit",
  "invgen",
  "invite",
  "adduser",
  "setrole",
  "listusers",
  "deluser",
}


@dataclass(frozen=True)
class CommandSpec:
  trigger: str
  method: str
  url: str
  display_name: str
  description: str
  auto_complete_desc: str
  auto_complete_hint: str
  auto_complete: bool = True

  def create_payload(self, team_id: str) -> dict[str, Any]:
    return {
      "team_id": team_id,
      "method": self.method,
      "trigger": self.trigger,
      "url": self.url,
    }

  def update_payload(self, base: dict[str, Any]) -> dict[str, Any]:
    payload = dict(base)
    payload.update({
      "display_name": self.display_name,
      "description": self.description,
      "auto_complete": self.auto_complete,
      "auto_complete_desc": self.auto_complete_desc,
      "auto_complete_hint": self.auto_complete_hint,
      "url": self.url,
      "trigger": self.trigger,
      "method": self.method,
    })
    return payload


def _env_value(*names: str) -> str:
  for name in names:
    val = os.getenv(name, "").strip()
    if val:
      return val
  return ""


def _build_service_urls() -> dict[str, str]:
  base = MM_PUBLIC_URL.rstrip("/")
  if not base:
    raise RuntimeError("MM_PUBLIC_URL is required for the Mattermost service URLs")
  secret = MM_WEBHOOK_SECRET.strip()
  prefix = f"/mm/{secret}" if secret else "/mm"
  return {
    "command_url": f"{base}{prefix}/command",
    "action_url": f"{base}{prefix}/action",
    "dialog_url": f"{base}{prefix}/dialog",
    "asset_url": f"{base}/mm/assets/<token>.png",
  }


def _command_spec(url: str) -> CommandSpec:
  return CommandSpec(
    trigger="monbot",
    method="P",
    url=url,
    display_name="monbot",
    description="DM-only Monbot integration commands",
    auto_complete_desc="Monbot DM-only integration commands",
    auto_complete_hint="help | graphs | sensors | maint | settz | refresh | report | audit | start | invite | adduser | setrole | listusers | deluser",
  )


def _print_summary(team: dict[str, Any], urls: dict[str, str], bot: dict[str, Any]) -> None:
  print("Mattermost setup summary")
  print(f"  Mattermost server: {MM_URL}")
  print(f"  Team: {team.get('name')} ({team.get('id')})")
  print(f"  Bot user: {bot.get('username') or bot.get('id')}")
  print(f"  MM_PUBLIC_URL: {MM_PUBLIC_URL}")
  print(f"  Command endpoint: {urls['command_url']}")
  print(f"  Action endpoint: {urls['action_url']}")
  print(f"  Dialog endpoint: {urls['dialog_url']}")
  print(f"  Asset endpoint pattern: {urls['asset_url']}")
  print("  Slash command to register: /monbot")
  print("  Runtime token to set: MM_COMMAND_TOKEN=<token from the command object>")
  print("  Suggested usage:")
  print("    /monbot help")
  print("    /monbot graphs")
  print("    /monbot maint")
  print("    /monbot invite <role> [max_uses] [ttl_sec]")
  print("    /monbot start <otp>")


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(description="Register and sync Mattermost commands for Monbot")
  parser.add_argument("--token", help="Mattermost personal access token for a user account with manage_slash_commands permission")
  parser.add_argument("--team", help="Mattermost team name or team ID")
  parser.add_argument("--keep-legacy", action="store_true", help="Do not delete old Monbot slash commands")
  args = parser.parse_args(argv)

  setup_token = (args.token or _env_value("MM_SETUP_TOKEN", "MM_ADMIN_TOKEN", "MM_BOT_TOKEN")).strip()
  if not setup_token:
    print("Missing Mattermost setup token. Set MM_SETUP_TOKEN or pass --token.", file=sys.stderr)
    return 2

  if not MM_URL.strip():
    print("Missing MM_URL. Set it to the Mattermost server URL, for example https://mm.csuas.ru.", file=sys.stderr)
    return 2

  if not MM_PUBLIC_URL.strip():
    print("Missing MM_PUBLIC_URL. Set it to the public URL of the Monbot service, not the Mattermost URL.", file=sys.stderr)
    return 2

  team_value = (args.team or MM_TEAM).strip()
  if not team_value:
    print("Missing MM_TEAM. Set MM_TEAM to the Mattermost team name (slug) or pass --team.", file=sys.stderr)
    return 2

  try:
    urls = _build_service_urls()
  except Exception as exc:
    print(str(exc), file=sys.stderr)
    return 2

  api = MattermostAPI(MM_URL, setup_token)

  try:
    me = api.get_me()
  except Exception as exc:
    print(
      "Setup token could not authenticate against Mattermost. "
      "Use a personal access token from a real Mattermost user account, not the bot token or the slash-command token.",
      file=sys.stderr,
    )
    print(f"Original error: {exc}", file=sys.stderr)
    return 2

  team = None
  try:
    team = api.get_team_by_name(team_value)
  except Exception:
    try:
      team = api.get_team(team_value)
    except Exception as exc:
      print(f"Unable to resolve team '{team_value}': {exc}", file=sys.stderr)
      return 2

  try:
    bot = MattermostAPI(MM_URL, MM_BOT_TOKEN).get_me() if MM_BOT_TOKEN else {"username": "(not configured)"}
  except Exception as exc:
    print(f"Warning: could not verify bot token with /users/me: {exc}", file=sys.stderr)
    bot = {"username": "(verification failed)"}

  spec = _command_spec(urls["command_url"])

  try:
    existing = api.list_commands(str(team["id"]), custom_only=True)
  except Exception as exc:
    print(f"Unable to list commands for team {team.get('name')}: {exc}", file=sys.stderr)
    return 2

  existing_by_trigger = {str(cmd.get("trigger") or ""): cmd for cmd in existing}
  existing_monbot = existing_by_trigger.get("monbot")

  if existing_monbot:
    try:
      updated = api.update_command(str(existing_monbot["id"]), spec.update_payload(existing_monbot))
    except Exception as exc:
      print(f"Warning: keeping existing /monbot command without optional metadata: {exc}", file=sys.stderr)
      updated = existing_monbot
    print(f"Updated slash command /{updated.get('trigger')} ({updated.get('id')})")
  else:
    created = api.create_command(spec.create_payload(str(team["id"])))
    try:
      updated = api.update_command(str(created["id"]), spec.update_payload(created))
    except Exception as exc:
      print(f"Warning: created /monbot but could not apply optional metadata: {exc}", file=sys.stderr)
      updated = created
    print(f"Created slash command /{updated.get('trigger')} ({updated.get('id')})")

  command_token = ""
  if existing_monbot and existing_monbot.get("token"):
    command_token = str(existing_monbot.get("token"))
  elif updated.get("token"):
    command_token = str(updated.get("token"))

  if not args.keep_legacy:
    legacy = [
      cmd for cmd in existing
      if str(cmd.get("trigger") or "") in LEGACY_TRIGGERS
      and str(cmd.get("url") or "").startswith(urls["command_url"].rsplit("/", 1)[0])
      and str(cmd.get("trigger") or "") != "monbot"
    ]
    for cmd in legacy:
      try:
        api.delete_command(str(cmd["id"]))
        print(f"Removed legacy slash command /{cmd.get('trigger')} ({cmd.get('id')})")
      except Exception as exc:
        print(f"Warning: failed to remove legacy command /{cmd.get('trigger')}: {exc}", file=sys.stderr)

  _print_summary(team, urls, bot)
  print(f"  Setup identity: {me.get('username') or me.get('id')}")
  if command_token:
    print(f"  MM_COMMAND_TOKEN={command_token}")
  print()
  print("Next step: configure Mattermost to point the /monbot slash command at the URL above, then restart the monbot-mm service.")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
