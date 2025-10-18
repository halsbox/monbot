from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Union


@dataclass
class CacheResult:
  # If file_id is set, prefer it to avoid upload. Else use bytes/path.
  file_id: Optional[str]
  data: Optional[bytes]
  path: Optional[Path]  # may be set when reading from disk


class _LRU:
  def __init__(self, max_bytes: int):
    self.max_bytes = max_bytes
    self.current_bytes = 0
    self.od: OrderedDict[str, Tuple[Optional[str], Optional[bytes], float, int]] = OrderedDict()
    # key -> (file_id, bytes, expiry_ts, size_bytes)

  def get(self, key: str) -> Optional[Tuple[Optional[str], Optional[bytes], float, int]]:
    val = self.od.get(key)
    if val is None:
      return None
    # move to end (MRU)
    self.od.move_to_end(key)
    return val

  def put(self, key: str, file_id: Optional[str], data: Optional[bytes], expiry_ts: float):
    size = len(data) if data is not None else 0
    if key in self.od:
      _, old_data, _, old_size = self.od.pop(key)
      self.current_bytes -= old_size
    self.od[key] = (file_id, data, expiry_ts, size)
    self.current_bytes += size
    self._evict()

  def set_file_id(self, key: str, file_id: str):
    val = self.od.get(key)
    if val is None:
      return
    _, data, expiry_ts, size = val
    self.od[key] = (file_id, data, expiry_ts, size)
    self.od.move_to_end(key)

  def _evict(self):
    while self.current_bytes > self.max_bytes and self.od:
      k, (_, data, _, size) = self.od.popitem(last=False)
      self.current_bytes -= size


def _is_fresh(expiry_ts: float) -> bool:
  return time.time() < expiry_ts


class ImageCache2:
  def __init__(self, cache_dir: Path, l1_max_bytes: int = 128 * 1024 * 1024, l2_max_bytes: int = 1_000 * 1024 * 1024):
    self.cache_dir = cache_dir
    self.cache_dir.mkdir(parents=True, exist_ok=True)
    self.l1 = _LRU(l1_max_bytes)
    self.l2_max_bytes = l2_max_bytes
    self._locks: Dict[str, asyncio.Lock] = {}
    self._janitor_lock = asyncio.Lock()

  @staticmethod
  def _key_str(parts: Dict[str, Any]) -> str:
    return json.dumps(parts, sort_keys=True, separators=(",", ":"))

  @staticmethod
  def _digest(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

  def _paths(self, key: str) -> Tuple[Path, Path]:
    d = self._digest(key)
    img = self.cache_dir / f"{d}.png"
    meta = self.cache_dir / f"{d}.json"
    return img, meta

  async def _janitor(self):
    # Remove old files when exceeding l2_max_bytes.
    async with self._janitor_lock:
      entries: list[Tuple[float, int, Path, Path]] = []
      total = 0
      for p in self.cache_dir.glob("*.png"):
        meta = p.with_suffix(".json")
        try:
          st = p.stat()
          m = json.loads(meta.read_text()) if meta.exists() else {}
          last_used = float(m.get("last_used_ts", st.st_mtime))
          size = st.st_size
        except Exception:
          last_used = p.stat().st_mtime
          size = p.stat().st_size
        entries.append((last_used, size, p, meta))
        total += size

      if total <= self.l2_max_bytes:
        return

      # Evict LRU until under 0.9 * cap
      target = int(0.9 * self.l2_max_bytes)
      entries.sort(key=lambda t: t[0])  # oldest first
      for _, size, p, meta in entries:
        try:
          p.unlink(missing_ok=True)
        finally:
          meta.unlink(missing_ok=True)
        total -= size
        if total <= target:
          break

  async def get_or_produce(
      self,
      key_parts: Dict[str, Any],
      ttl: int,
      producer: Callable[[], Union[bytes, Awaitable[bytes]]],
  ) -> CacheResult:
    key = self._key_str(key_parts)
    expiry_ts = time.time() + ttl

    # L1
    l1_hit = self.l1.get(key)
    if l1_hit is not None:
      file_id, data, exp, _ = l1_hit
      if _is_fresh(exp):
        if file_id:
          return CacheResult(file_id=file_id, data=None, path=None)
        if data is not None:
          return CacheResult(file_id=None, data=data, path=None)

    # L2
    img_path, meta_path = self._paths(key)
    if img_path.exists() and meta_path.exists():
      try:
        meta = json.loads(meta_path.read_text())
        exp = float(meta.get("expiry_ts", 0.0))
        file_id = meta.get("file_id")
        if _is_fresh(exp):
          try:
            meta["last_used_ts"] = time.time()
            meta_path.write_text(json.dumps(meta, separators=(",", ":"), ensure_ascii=False))
          except Exception:
            pass
          if file_id:
            # promote to L1 with no bytes
            self.l1.put(key, file_id, None, exp)
            return CacheResult(file_id=file_id, data=None, path=img_path)
          # else read bytes
          data = img_path.read_bytes()
          self.l1.put(key, None, data, exp)
          return CacheResult(file_id=None, data=data, path=img_path)
      except Exception:
        pass  # fall through to fetch

    # Singleflight
    lock = self._locks.setdefault(key, asyncio.Lock())
    try:
      async with lock:
        # Re-check inside the lock to avoid thundering herd
        l1_hit = self.l1.get(key)
        if l1_hit is not None:
          file_id, data, exp, _ = l1_hit
          if _is_fresh(exp):
            if file_id:
              return CacheResult(file_id=file_id, data=None, path=None)
            if data is not None:
              return CacheResult(file_id=None, data=data, path=None)
        if img_path.exists() and meta_path.exists():
          try:
            meta = json.loads(meta_path.read_text())
            exp = float(meta.get("expiry_ts", 0.0))
            file_id = meta.get("file_id")
            if _is_fresh(exp):
              if file_id:
                self.l1.put(key, file_id, None, exp)
                return CacheResult(file_id=file_id, data=None, path=img_path)
              data = img_path.read_bytes()
              self.l1.put(key, None, data, exp)
              return CacheResult(file_id=None, data=data, path=img_path)
          except Exception:
            pass

        # Fetch
        data = await asyncio.to_thread(_call_producer, producer)
        # Write atomically to L2
        tmp = img_path.with_suffix(".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, img_path)

        meta = {
          "expiry_ts": expiry_ts,
          "file_id": None,
          "byte_len": len(data),
          "created_ts": time.time(),
          "last_used_ts": time.time(),
        }
        tmpm = meta_path.with_suffix(".tmp.json")
        tmpm.write_text(json.dumps(meta, separators=(",", ":"), ensure_ascii=False))
        os.replace(tmpm, meta_path)

        # Put into L1
        self.l1.put(key, None, data, expiry_ts)

        # Async janitor
        asyncio.create_task(self._janitor())

        return CacheResult(file_id=None, data=data, path=img_path)
    finally:
      try:
        lk = self._locks.get(key)
        if lk is lock and not lk.locked():
          self._locks.pop(key, None)
      except Exception:
        pass

  async def remember_file_id(self, key_parts: Dict[str, Any], file_id: str, ttl: int):
    key = self._key_str(key_parts)
    exp = time.time() + ttl
    # Update L1
    self.l1.set_file_id(key, file_id)
    # Update L2
    img_path, meta_path = self._paths(key)
    if meta_path.exists():
      try:
        meta = json.loads(meta_path.read_text())
      except Exception:
        meta = {}
    else:
      meta = {}
    meta["file_id"] = file_id
    meta["expiry_ts"] = exp
    meta["last_used_ts"] = time.time()
    tmpm = meta_path.with_suffix(".tmp.json")
    tmpm.write_text(json.dumps(meta, separators=(",", ":"), ensure_ascii=False))
    os.replace(tmpm, meta_path)


def _call_producer(producer: Callable[[], Union[bytes, Awaitable[bytes]]]) -> bytes:
  res = producer()
  if asyncio.iscoroutine(res):
    return asyncio.run(res)
  return res
