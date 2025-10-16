import secrets
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import aiosqlite

from monbot.handlers.consts import ROLE_ADMIN, ROLE_MAINTAINER, ROLE_VIEWER

CREATE_USERS_SQL_V2 = """
                      CREATE TABLE IF NOT EXISTS users
                      (
                          telegram_id
                          INTEGER
                          PRIMARY
                          KEY,
                          role
                          TEXT
                          NOT
                          NULL
                          CHECK (
                          role
                          IN
                      (
                          'admin',
                          'maintainer',
                          'viewer'
                      )),
                          username TEXT,
                          first_name TEXT,
                          last_name TEXT,
                          tz TEXT DEFAULT 'Europe/Moscow',
                          last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
                          info_refreshed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                          updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                          ); \
                      """

CREATE_INVITES_SQL = """
                     CREATE TABLE IF NOT EXISTS invites
                     (
                         otp
                         TEXT
                         PRIMARY
                         KEY,
                         role
                         TEXT
                         NOT
                         NULL
                         CHECK (
                         role
                         IN
                     (
                         'admin',
                         'maintainer',
                         'viewer'
                     )),
                         max_uses INTEGER NOT NULL DEFAULT 1,
                         used_count INTEGER NOT NULL DEFAULT 0,
                         created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                         expires_at DATETIME
                         ); \
                     """

CREATE_MAINT_AUDIT_SQL = """
                         CREATE TABLE IF NOT EXISTS maint_audit
                         (
                             id
                             INTEGER
                             PRIMARY
                             KEY
                             AUTOINCREMENT,
                             ts
                             DATETIME
                             DEFAULT
                             CURRENT_TIMESTAMP,
                             user_id
                             INTEGER
                             NOT
                             NULL,
                             action
                             TEXT
                             NOT
                             NULL
                             CHECK (
                             action
                             IN
                         (
                             'create',
                             'update',
                             'delete',
                             'end'
                         )),
                             maintenanceid TEXT,
                             itemid TEXT,
                             hostid TEXT,
                             before_json TEXT,
                             after_json TEXT
                             ); \
                         """

ROLE_LEVEL = {ROLE_VIEWER: 1, ROLE_MAINTAINER: 2, ROLE_ADMIN: 3}


class UserDB:
  def __init__(self, db_path: Path):
    self.db_path = db_path

  async def init(self):
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute("PRAGMA foreign_keys=off;")
      cur = await db.execute("SELECT name, sql FROM sqlite_master WHERE type='table' AND name='users'")
      row = await cur.fetchone()
      if not row:
        await db.execute(CREATE_USERS_SQL_V2)
      else:
        ddl = row[1] or ""
        if "CHECK" in ddl and "'maintainer'" not in ddl and "'viewer'" not in ddl:
          await db.execute("ALTER TABLE users RENAME TO users_old;")
          await db.execute(CREATE_USERS_SQL_V2)
          try:
            await db.execute("""
              INSERT INTO users (telegram_id, role, username, first_name, last_name, tz, last_seen,
                                 info_refreshed_at, created_at, updated_at)
              SELECT telegram_id,
                     CASE
                       WHEN role = 'admin' THEN 'admin'
                       WHEN COALESCE(role_maint, 0) = 1 THEN 'maintainer'
                       ELSE 'viewer'
                     END AS role_new,
                     username,
                     first_name,
                     last_name,
                     COALESCE(tz, 'Europe/Moscow'),
                     CURRENT_TIMESTAMP,
                     CURRENT_TIMESTAMP,
                     COALESCE(created_at, CURRENT_TIMESTAMP),
                     CURRENT_TIMESTAMP
              FROM users_old;
            """)
          except Exception:
            await db.execute("""
              INSERT INTO users (telegram_id, role, username, first_name, last_name)
              SELECT telegram_id,
                     CASE WHEN role = 'admin' THEN 'admin' ELSE 'viewer' END,
                     username, first_name, last_name
              FROM users_old;
            """)
          await db.execute("DROP TABLE users_old;")
      # FIX: actually create invites and maintenance audit tables
      await db.execute(CREATE_INVITES_SQL)
      await db.execute(CREATE_MAINT_AUDIT_SQL)
      await db.execute("PRAGMA foreign_keys=on;")
      await db.commit()

  async def add_user(self, telegram_id: int, role: str = ROLE_VIEWER,
                     username: Optional[str] = None,
                     first_name: Optional[str] = None,
                     last_name: Optional[str] = None):
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute("""
                INSERT OR REPLACE INTO users (telegram_id, role, username, first_name, last_name, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (telegram_id, role, username, first_name, last_name))
      await db.commit()

  async def audit_maint(self, user_id: int, action: str, maintenanceid: str | None, itemid: str | None,
                        hostid: str | None, before_json: str | None, after_json: str | None):
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute("""
                       INSERT INTO maint_audit (user_id, action, maintenanceid, itemid, hostid, before_json, after_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       """, (user_id, action, maintenanceid or "", itemid or "", hostid or "", before_json or "",
                             after_json or ""))
      await db.commit()

  async def create_invite(self, role: str, max_uses: int = 1, ttl_sec: Optional[int] = None) -> str:
    if role not in ROLE_LEVEL:
      raise ValueError("Invalid role")
    otp = secrets.token_urlsafe(16)
    expires_at = None
    if ttl_sec and ttl_sec > 0:
      # FIX: store naive UTC string consistently
      expires_at = (datetime.utcnow() + timedelta(seconds=ttl_sec)).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute(
        "INSERT INTO invites (otp, role, max_uses, used_count, expires_at) VALUES (?,?,?,?,?)",
        (otp, role, max_uses, 0, expires_at),
      )
      await db.commit()
    return otp

  async def consume_invite(self, otp: str) -> Optional[str]:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute("SELECT role, max_uses, used_count, expires_at FROM invites WHERE otp=?", (otp,)) as cur:
        row = await cur.fetchone()
      if not row:
        return None
      role, max_uses, used_count, expires_at = row
      if expires_at:
        # FIX: compare naive UTC to naive UTC
        exp_dt = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S")
        if datetime.utcnow() > exp_dt:
          return None
      if used_count >= max_uses:
        return None
      await db.execute("UPDATE invites SET used_count = used_count + 1 WHERE otp=?", (otp,))
      await db.commit()
      return role

  async def upsert_user_info_throttled(self, tg_user, min_interval_sec: int = 3600) -> None:
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute("UPDATE users SET last_seen=CURRENT_TIMESTAMP WHERE telegram_id=?", (tg_user.id,))
      async with db.execute("SELECT info_refreshed_at FROM users WHERE telegram_id=?", (tg_user.id,)) as cur:
        row = await cur.fetchone()
      need_refresh = True
      if row and row[0]:
        try:
          prev = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")  # naive UTC as stored by sqlite
          need_refresh = (datetime.utcnow() - prev).total_seconds() >= min_interval_sec
        except Exception:
          need_refresh = True
      if need_refresh:
        await db.execute("""
          UPDATE users
          SET username=?,
              first_name=?,
              last_name=?,
              info_refreshed_at=CURRENT_TIMESTAMP,
              updated_at=CURRENT_TIMESTAMP
          WHERE telegram_id = ?
        """, (tg_user.username, tg_user.first_name, tg_user.last_name, tg_user.id))
      await db.commit()

  async def add_or_update_user(self, telegram_id: int, role: str, username=None, first_name=None, last_name=None):
    if role not in ROLE_LEVEL:
      raise ValueError("Invalid role")
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute("""
                       INSERT INTO users (telegram_id, role, username, first_name, last_name, updated_at, last_seen,
                                          info_refreshed_at)
                       VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                               CURRENT_TIMESTAMP) ON CONFLICT(telegram_id) DO
                       UPDATE SET
                           role=excluded.role,
                           username=excluded.username,
                           first_name=excluded.first_name,
                           last_name=excluded.last_name,
                           updated_at= CURRENT_TIMESTAMP,
                           last_seen= CURRENT_TIMESTAMP,
                           info_refreshed_at= CURRENT_TIMESTAMP
                       """, (telegram_id, role, username, first_name, last_name))
      await db.commit()

  async def get_user(self, telegram_id: int) -> Optional[Tuple[int, str, str, str, str]]:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute(
          "SELECT telegram_id, role, username, first_name, last_name FROM users WHERE telegram_id = ?",
          (telegram_id,)
      ) as cursor:
        return await cursor.fetchone()

  async def delete_user(self, telegram_id: int) -> bool:
    async with aiosqlite.connect(self.db_path) as db:
      cur = await db.execute("DELETE FROM users WHERE telegram_id = ?", (telegram_id,))
      await db.commit()
      return cur.rowcount > 0

  async def set_role(self, telegram_id: int, role: str) -> bool:
    if role not in ROLE_LEVEL:
      return False
    async with aiosqlite.connect(self.db_path) as db:
      cur = await db.execute("""
                             UPDATE users
                             SET role       = ?,
                                 updated_at = CURRENT_TIMESTAMP
                             WHERE telegram_id = ?
                             """, (role, telegram_id))
      await db.commit()
      return cur.rowcount > 0

  async def list_users(self) -> List[Tuple[int, str, str, str, str]]:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute("""
                            SELECT telegram_id, role, username, first_name, last_name
                            FROM users
                            ORDER BY CASE role WHEN 'admin' THEN 3 WHEN 'maintainer' THEN 2 ELSE 1 END DESC, telegram_id
                                                                                                       ASC
                            """) as cursor:
        return await cursor.fetchall()

  async def ensure_admins(self, admin_ids: list[int]):
    for uid in admin_ids:
      existing = await self.get_user(uid)
      if existing is None:
        await self.add_or_update_user(uid, role=ROLE_ADMIN)
      else:
        if existing[1] != ROLE_ADMIN:
          await self.set_role(uid, ROLE_ADMIN)

  async def get_role(self, telegram_id: int) -> Optional[str]:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute("SELECT role FROM users WHERE telegram_id=?", (telegram_id,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else None

  async def role_at_least(self, telegram_id: int, required: str) -> bool:
    role = await self.get_role(telegram_id)
    if not role:
      return False
    return ROLE_LEVEL.get(role, 0) >= ROLE_LEVEL.get(required, 99)

  async def is_admin(self, telegram_id: int) -> bool:
    return await self.role_at_least(telegram_id, ROLE_ADMIN)

  async def is_maintainer(self, telegram_id: int) -> bool:
    return await self.role_at_least(telegram_id, ROLE_MAINTAINER)

  async def set_timezone(self, telegram_id: int, tz: str):
    async with aiosqlite.connect(self.db_path) as db:
      await db.execute("UPDATE users SET tz = ?, updated_at = CURRENT_TIMESTAMP WHERE telegram_id = ?",
                       (tz, telegram_id))
      await db.commit()

  async def get_timezone(self, telegram_id: int) -> str:
    async with aiosqlite.connect(self.db_path) as db:
      async with db.execute("SELECT tz FROM users WHERE telegram_id = ?", (telegram_id,)) as cur:
        row = await cur.fetchone()
        return row[0] if row and row[0] else "Europe/Moscow"
