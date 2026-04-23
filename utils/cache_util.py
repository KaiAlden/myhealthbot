from __future__ import annotations

import threading
import time
from typing import Any


class SimpleCache:
    def __init__(self):
        self._data: dict[str, tuple[Any, float | None]] = {}
        self._lock = threading.Lock()

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            item = self._data.get(key)
            if not item:
                return default
            value, expires_at = item
            if expires_at is not None and expires_at <= time.time():
                self._data.pop(key, None)
                return default
            return value

    def set(self, key: str, value: Any, ttl: int | float | None = None) -> None:
        expires_at = time.time() + ttl if ttl else None
        with self._lock:
            self._data[key] = (value, expires_at)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)


cache = SimpleCache()


def invalidate_merchant_rag_cache(merchant_id: str) -> None:
    merchant_key = str(merchant_id or "").strip()
    if not merchant_key:
        return
    cache.delete(f"dataset_id:{merchant_key}")
    cache.delete(f"merchant_rag_config:{merchant_key}")
