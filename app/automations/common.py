"""Shared helpers used by automation handlers."""

from __future__ import annotations

import logging
import unicodedata
from dataclasses import dataclass

import requests

from app.core.clickup_client import fetch_task, fetch_task_from_dest

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StatusChangeContext:
    task_id: str
    new_status: str
    normalized_new_status: str
    task_data: dict
    task_name: str
    source_list_id: str
    source_list_name: str
    current_status_raw: str
    current_status_normalized: str


class TaskNotFoundError(Exception):
    """Task nao encontrada no ClickUp para nenhum token disponivel."""

    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task nao encontrada: {task_id}")
        self.task_id = str(task_id)
        self.status_code = 404


def normalize_status(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value).strip().lower())
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def task_status_value(task_data: dict) -> str:
    raw_status = task_data.get("status")
    if isinstance(raw_status, dict):
        return str(raw_status.get("status") or raw_status.get("name") or "")
    if isinstance(raw_status, str):
        return raw_status
    return ""


def task_name(task_data: dict) -> str:
    return str(task_data.get("name") or "Sem nome")


def task_list_id(task_data: dict) -> str:
    return str((task_data.get("list") or {}).get("id") or "").strip()


def task_list_name(task_data: dict) -> str:
    return str((task_data.get("list") or {}).get("name") or "").strip()


def extract_related_task_ids(task_data: dict) -> list[str]:
    ids: set[str] = set()

    def _add(value) -> None:
        if value is None:
            return
        text = str(value).strip()
        if text:
            ids.add(text)

    for key in ("linked_tasks", "relationships", "dependencies"):
        entries = task_data.get(key) or []
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            _add(entry.get("task_id"))
            _add(entry.get("link_id"))
            _add(entry.get("linked_task_id"))
            _add(entry.get("linked_task"))
            _add(entry.get("related_task_id"))
            _add(entry.get("depends_on"))
            _add(entry.get("dependency_id"))
            _add(entry.get("task"))
            _add(entry.get("linked_to"))
            _add(entry.get("id"))

    for cf in task_data.get("custom_fields", []) or []:
        cf_type = str(cf.get("type") or "").strip().lower()
        if cf_type not in {"task", "tasks"}:
            continue
        value = cf.get("value")
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _add(item.get("task_id") or item.get("id"))
                else:
                    _add(item)
        elif isinstance(value, dict):
            _add(value.get("task_id") or value.get("id"))
        else:
            _add(value)

    parent = task_data.get("parent")
    if parent:
        _add(parent)

    current_task_id = str(task_data.get("id") or "").strip()
    if current_task_id and current_task_id in ids:
        ids.remove(current_task_id)

    return sorted(ids)


def fetch_task_any(task_id: str) -> dict:
    def _status_code(error: Exception) -> int | None:
        if isinstance(error, requests.HTTPError) and error.response is not None:
            try:
                return int(error.response.status_code)
            except Exception:
                return None
        return None

    source_error: Exception | None = None
    source_status: int | None = None
    try:
        task = fetch_task(task_id)
        logger.debug("fetch_task_any: task_id=%s carregada com token source.", task_id)
        return task
    except Exception as e:
        source_error = e
        source_status = _status_code(e)
        logger.debug(
            "fetch_task_any: falha com token source para task_id=%s: %s",
            task_id,
            e,
        )

    try:
        task = fetch_task_from_dest(task_id)
        logger.debug("fetch_task_any: task_id=%s carregada com token destino.", task_id)
        return task
    except Exception as dest_error:
        dest_status = _status_code(dest_error)
        if source_status == 404 and dest_status == 404:
            logger.info("fetch_task_any.skip task_nao_encontrada task_id=%s", task_id)
            raise TaskNotFoundError(task_id) from dest_error
        logger.exception(
            "fetch_task_any: falha nos dois tokens para task_id=%s. erro_source=%s",
            task_id,
            source_error,
        )
        raise dest_error


def fetch_task_preferring_dest(task_id: str) -> dict:
    """Fetches a task favoring destination token first."""
    def _status_code(error: Exception) -> int | None:
        if isinstance(error, requests.HTTPError) and error.response is not None:
            try:
                return int(error.response.status_code)
            except Exception:
                return None
        return None

    dest_error: Exception | None = None
    dest_status: int | None = None
    try:
        task = fetch_task_from_dest(task_id)
        logger.debug(
            "fetch_task_preferring_dest: task_id=%s carregada com token destino.",
            task_id,
        )
        return task
    except Exception as e:
        dest_error = e
        dest_status = _status_code(e)
        logger.debug(
            "fetch_task_preferring_dest: falha com token destino para task_id=%s: %s",
            task_id,
            e,
        )

    try:
        task = fetch_task(task_id)
        logger.debug(
            "fetch_task_preferring_dest: task_id=%s carregada com token source.",
            task_id,
        )
        return task
    except Exception as source_error:
        source_status = _status_code(source_error)
        if source_status == 404 and dest_status == 404:
            logger.info(
                "fetch_task_preferring_dest.skip task_nao_encontrada task_id=%s",
                task_id,
            )
            raise TaskNotFoundError(task_id) from source_error
        logger.exception(
            "fetch_task_preferring_dest: falha nos dois tokens para task_id=%s. erro_dest=%s",
            task_id,
            dest_error,
        )
        raise source_error


def build_status_change_context(task_id: str, new_status: str) -> StatusChangeContext:
    task_data = fetch_task_any(task_id)
    current_status_raw = task_status_value(task_data)
    return StatusChangeContext(
        task_id=task_id,
        new_status=new_status,
        normalized_new_status=normalize_status(new_status),
        task_data=task_data,
        task_name=task_name(task_data),
        source_list_id=task_list_id(task_data),
        source_list_name=task_list_name(task_data),
        current_status_raw=current_status_raw,
        current_status_normalized=normalize_status(current_status_raw),
    )
