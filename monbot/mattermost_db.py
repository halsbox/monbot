from __future__ import annotations

import secrets
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence

import aiosqlite

from monbot.handlers.consts import ROLE_ADMIN, ROLE_MAINTAINER, ROLE_VIEWER

ROLE_LEVEL = {ROLE_VIEWER: 1, ROLE_MAINTAINER: 2, ROLE_ADMIN: 3}

CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS mm_users
(
  mattermost_id TEXT PRIMARY KEY,
  role TEXT NOT NULL CHECK (role IN ('admin', 'maintainer', 'viewer')),
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  tz TEXT DEFAULT 'Europe/Moscow',
  last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
  info_refreshed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_INVITES_SQL = """
CREATE TABLE IF NOT EXISTS mm_invites
(
  otp TEXT PRIMARY KEY,
  role TEXT NOT NULL CHECK (role IN ('admin', 'maintainer', 'viewer')),
  max_uses INTEGER NOT NULL DEFAULT 1,
  used_count INTEGER NOT NULL DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  expires_at DATETIME
);
"""

CREATE_MAINT_AUDIT_SQL = """
CREATE TABLE IF NOT EXISTS mm_maint_audit
(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts DATETIME DEFAULT CURRENT_TIMESTAMP,
  user_id TEXT NOT NULL,
  username TEXT,
  action TEXT NOT NULL CHECK (action IN ('create','update','delete','end')),
  maintenanceid TEXT,
  itemid TEXT,
  item_name TEXT,
  hostid TEXT,
  host_name TEXT,
  start_ts INTEGER,
  end_ts INTEGER,
  before_json TEXT,
  after_json TEXT
);
"""

CREATE_REPORTS_SQL = """
CREATE TABLE IF NOT EXISTS mm_reports (
  dashboard_id INTEGER NOT NULL,
  period_type  TEXT NOT NULL CHECK (period_type IN ('week','month')),
  start_ts     INTEGER NOT NULL,
  end_ts       INTEGER NOT NULL,
  path         TEXT NOT NULL,
  mm_file_id   TEXT,
  PRIMARY KEY (dashboard_id, period_type, start_ts)
);
"""


class MattermostDB:
  def __init__(self, db_path: Path):
    self.db_path = db_path

  async def init(self) -> None:
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute(CREATE_USERS_SQL)
      await db.execute(CREATE_INVITES_SQL)
      await db.execute(CREATE_MAINT_AUDIT_SQL)
      await db.execute(CREATE_REPORTS_SQL)
      await db.commit()

  async def ensure_user(
      self,
      user_id: str,
      role: str = ROLE_VIEWER,
      username: str | None = None,
      first_name: str | None = None,
      last_name: str | None = None,
  ) -> None:
    if role not in ROLE_LEVEL:
      raise ValueError("Invalid role")
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute(
        """
        INSERT INTO mm_users (mattermost_id, role, username, first_name, last_name, updated_at, last_seen, info_refreshed_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(mattermost_id) DO UPDATE SET
          role=excluded.role,
          username=excluded.username,
          first_name=excluded.first_name,
          last_name=excluded.last_name,
          updated_at=CURRENT_TIMESTAMP,
          last_seen=CURRENT_TIMESTAMP,
          info_refreshed_at=CURRENT_TIMESTAMP
        """,
        (str(user_id), role, username, first_name, last_name),
      )
      await db.commit()

  async def upsert_user_info_throttled(self, user: dict, min_interval_sec: int = 3600) -> None:
    user_id = str(user.get("id") or "")
    if not user_id:
      return
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute("UPDATE mm_users SET last_seen=CURRENT_TIMESTAMP WHERE mattermost_id=?", (user_id,))
      async with db.execute("SELECT info_refreshed_at FROM mm_users WHERE mattermost_id=?", (user_id,)) as cur:
        row = await cur.fetchone()
      need_refresh = True
      if row and row[0]:
        try:
          prev = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
          need_refresh = (datetime.utcnow() - prev).total_seconds() >= min_interval_sec
        except Exception:
          need_refresh = True
      if need_refresh:
        await db.execute(
          """
          UPDATE mm_users
          SET username=?,
              first_name=?,
              last_name=?,
              info_refreshed_at=CURRENT_TIMESTAMP,
              updated_at=CURRENT_TIMESTAMP
          WHERE mattermost_id=?
          """,
          (user.get("username"), user.get("first_name"), user.get("last_name"), user_id),
        )
      await db.commit()

  async def get_user(self, user_id: str) -> Optional[tuple]:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute(
          "SELECT mattermost_id, role, username, first_name, last_name, tz FROM mm_users WHERE mattermost_id=?",
          (str(user_id),),
      ) as cur:
        return await cur.fetchone()

  async def get_role(self, user_id: str) -> Optional[str]:
    row = await self.get_user(user_id)
    return row[1] if row else None

  async def role_at_least(self, user_id: str, required: str) -> bool:
    role = await self.get_role(user_id)
    if not role:
      return False
    return ROLE_LEVEL.get(role, 0) >= ROLE_LEVEL.get(required, 99)

  async def is_admin(self, user_id: str) -> bool:
    return await self.role_at_least(user_id, ROLE_ADMIN)

  async def is_maintainer(self, user_id: str) -> bool:
    return await self.role_at_least(user_id, ROLE_MAINTAINER)

  async def set_role(self, user_id: str, role: str) -> bool:
    if role not in ROLE_LEVEL:
      return False
    async with aiosqlite.connect(self.db_path) as db:
      cur = await db.execute(
        "UPDATE mm_users SET role=?, updated_at=CURRENT_TIMESTAMP WHERE mattermost_id=?",
        (role, str(user_id)),
      )
      await db.commit()
      return cur.rowcount > 0

  async def delete_user(self, user_id: str) -> bool:
    async with aiosqlite.connect(self.db_path) as db:
      cur = await db.execute("DELETE FROM mm_users WHERE mattermost_id=?", (str(user_id),))
      await db.commit()
      return cur.rowcount > 0

  async def list_users(self) -> list[tuple]:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute(
          """
          SELECT mattermost_id, role, username, first_name, last_name
          FROM mm_users
          ORDER BY CASE role WHEN 'admin' THEN 3 WHEN 'maintainer' THEN 2 ELSE 1 END DESC, mattermost_id ASC
          """
      ) as cur:
        return await cur.fetchall()

  async def set_timezone(self, user_id: str, tz: str) -> None:
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute(
        "UPDATE mm_users SET tz=?, updated_at=CURRENT_TIMESTAMP WHERE mattermost_id=?",
        (tz, str(user_id)),
      )
      await db.commit()

  async def get_timezone(self, user_id: str) -> str:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute("SELECT tz FROM mm_users WHERE mattermost_id=?", (str(user_id),)) as cur:
        row = await cur.fetchone()
        return row[0] if row and row[0] else "Europe/Moscow"

  async def ensure_admins(self, user_ids: Sequence[str]) -> None:
    for user_id in user_ids:
      existing = await self.get_user(user_id)
      if existing is None:
        await self.ensure_user(user_id, role=ROLE_ADMIN)
      elif existing[1] != ROLE_ADMIN:
        await self.set_role(user_id, ROLE_ADMIN)

  async def create_invite(self, role: str, max_uses: int = 1, ttl_sec: Optional[int] = None) -> str:
    if role not in ROLE_LEVEL:
      raise ValueError("Invalid role")
    otp = secrets.token_urlsafe(16)
    expires_at = None
    if ttl_sec and ttl_sec > 0:
      expires_at = (datetime.utcnow() + timedelta(seconds=ttl_sec)).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute(
        "INSERT INTO mm_invites (otp, role, max_uses, used_count, expires_at) VALUES (?,?,?,?,?)",
        (otp, role, max_uses, 0, expires_at),
      )
      await db.commit()
    return otp

  async def consume_invite(self, otp: str) -> Optional[str]:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute("SELECT role, max_uses, used_count, expires_at FROM mm_invites WHERE otp=?", (otp,)) as cur:
        row = await cur.fetchone()
      if not row:
        return None
      role, max_uses, used_count, expires_at = row
      if expires_at:
        exp_dt = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S")
        if datetime.utcnow() > exp_dt:
          return None
      if used_count >= max_uses:
        return None
      await db.execute("UPDATE mm_invites SET used_count = used_count + 1 WHERE otp=?", (otp,))
      await db.commit()
      return role

  async def audit_maint(
      self,
      user_id: str,
      action: str,
      maintenanceid: str | None,
      itemid: str | None,
      hostid: str | None,
      before_json: str | None,
      after_json: str | None,
      username: str | None = None,
      host_name: str | None = None,
      item_name: str | None = None,
      start_ts: int | None = None,
      end_ts: int | None = None,
  ) -> None:
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute(
        """
        INSERT INTO mm_maint_audit
          (user_id, username, action, maintenanceid, itemid, item_name, hostid, host_name,
           start_ts, end_ts, before_json, after_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
          str(user_id),
          username or "",
          action,
          maintenanceid or "",
          itemid or "",
          item_name or "",
          hostid or "",
          host_name or "",
          start_ts if start_ts is not None else None,
          end_ts if end_ts is not None else None,
          before_json or "",
          after_json or "",
        ),
      )
      await db.commit()

  async def list_maint_audit(self, limit: int = 10, filter_text: str | None = None) -> list[tuple]:
    async with aiosqlite.connect(self.db_path) as db:
      if filter_text:
        like = f"%{filter_text}%"
        async with db.execute(
            """
            SELECT ts, action, COALESCE(username,''), COALESCE(item_name,''), COALESCE(host_name,''),
                   start_ts, end_ts
            FROM mm_maint_audit
            WHERE host_name LIKE ? OR item_name LIKE ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (like, like, int(limit)),
        ) as cur:
          return await cur.fetchall()
      async with db.execute(
          """
          SELECT ts, action, COALESCE(username,''), COALESCE(item_name,''), COALESCE(host_name,''),
                 start_ts, end_ts
          FROM mm_maint_audit
          ORDER BY id DESC
          LIMIT ?
          """,
          (int(limit),),
      ) as cur:
        return await cur.fetchall()

  async def get_report_record(self, dashboard_id: int, period_type: str, start_ts: int) -> Optional[tuple]:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute(
          "SELECT path, mm_file_id, end_ts FROM mm_reports WHERE dashboard_id=? AND period_type=? AND start_ts=?",
          (dashboard_id, period_type, start_ts),
      ) as cur:
        row = await cur.fetchone()
        return row if row else None

  async def upsert_report_path(self, dashboard_id: int, period_type: str, start_ts: int, end_ts: int, path: str) -> None:
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute(
        """
        INSERT INTO mm_reports (dashboard_id, period_type, start_ts, end_ts, path)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(dashboard_id, period_type, start_ts) DO UPDATE SET
          end_ts=excluded.end_ts,
          path=excluded.path
        """,
        (dashboard_id, period_type, start_ts, end_ts, path),
      )
      await db.commit()

  async def set_report_file_id(self, dashboard_id: int, period_type: str, start_ts: int, file_id: str) -> None:
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute(
        """
        UPDATE mm_reports
        SET mm_file_id=?
        WHERE dashboard_id=? AND period_type=? AND start_ts=?
        """,
        (file_id, dashboard_id, period_type, start_ts),
      )
      await db.commit()
