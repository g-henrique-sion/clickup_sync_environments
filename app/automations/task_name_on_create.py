"""Rename tasks on creation for lists with explicit name-format rules."""

from __future__ import annotations

import logging

from app.automations.common import (
    TaskNotReadyError,
    fetch_task_any,
    task_list_id,
    task_name,
)
from app.automations.task_name_formatter import (
    build_formatted_task_name,
    should_format_task_name_on_create,
)
from app.core.clickup_client import task_permalink, update_task_name_any

logger = logging.getLogger(__name__)


def run_task_created(task_id: str) -> dict | None:
    task_data = fetch_task_any(task_id)
    list_id = task_list_id(task_data)
    if not should_format_task_name_on_create(list_id):
        return None

    current_name = task_name(task_data)
    formatted_name = build_formatted_task_name(
        task_data,
        current_name,
        list_id,
        allow_fallback=False,
    )
    if not formatted_name:
        logger.warning(
            "task_name_on_create.defer campos_insuficientes task_id=%s list_id=%s link=%s",
            task_id,
            list_id,
            task_permalink(task_id),
        )
        raise TaskNotReadyError(task_id, max_attempts=8)

    if formatted_name == current_name:
        logger.info(
            "task_name_on_create.noop task_id=%s list_id=%s name='%s'",
            task_id,
            list_id,
            current_name,
        )
        return {"id": task_id, "name": current_name, "updated": False}

    updated = update_task_name_any(task_id, formatted_name)
    logger.info(
        "task_name_on_create.ok task_id=%s list_id=%s old_name='%s' new_name='%s' link=%s",
        task_id,
        list_id,
        current_name,
        formatted_name,
        task_permalink(task_id),
    )
    return updated
