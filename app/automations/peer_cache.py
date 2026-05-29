"""Cache for relationship-based task peer lookups."""

from __future__ import annotations

import threading
import time

_PEER_CACHE_TTL_SECONDS = 600.0
_peer_cache_lock = threading.Lock()
_peer_cache: dict[tuple[str, str], tuple[str, float]] = {}


def cache_peer(route: str, left_task_id: str, right_task_id: str) -> None:
    if not left_task_id or not right_task_id:
        return
    expires_at = time.time() + _PEER_CACHE_TTL_SECONDS
    with _peer_cache_lock:
        _peer_cache[(route, left_task_id)] = (right_task_id, expires_at)
        _peer_cache[(route, right_task_id)] = (left_task_id, expires_at)


def invalidate_cached_peer(route: str, task_id: str) -> None:
    if not task_id:
        return
    with _peer_cache_lock:
        _peer_cache.pop((route, task_id), None)


def get_cached_peer(route: str, task_id: str, related_task_ids: list[str]) -> str | None:
    if not task_id:
        return None
    related_set = set(related_task_ids)
    with _peer_cache_lock:
        entry = _peer_cache.get((route, task_id))
        if not entry:
            return None
        peer_task_id, expires_at = entry
        if time.time() >= expires_at:
            _peer_cache.pop((route, task_id), None)
            return None
        if peer_task_id not in related_set:
            _peer_cache.pop((route, task_id), None)
            return None
        return peer_task_id

