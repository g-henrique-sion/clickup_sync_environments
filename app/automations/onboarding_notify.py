"""Notifica membro do time em comentarios nas listas de onboarding."""

from __future__ import annotations

import logging

from app.automations.common import (
    StatusChangeContext,
    fetch_task_any,
    normalize_status,
    task_list_id,
    task_list_name,
    task_status_value,
)
from app.config.settings import (
    ONBOARDING_NOTIFY_ENABLED,
    ONBOARDING_NOTIFY_LIST_IDS,
    ONBOARDING_NOTIFY_USER_IDS,
    ONBOARDING_NOTIFY_USER_NAMES,
)
from app.core.clickup_client import create_task_comment_with_tags_any

logger = logging.getLogger(__name__)


def _target_lists() -> set[str]:
    return {
        str(list_id).strip()
        for list_id in ONBOARDING_NOTIFY_LIST_IDS
        if str(list_id).strip()
    }


def _is_enabled() -> bool:
    return (
        ONBOARDING_NOTIFY_ENABLED
        and bool(_target_lists())
        and bool(_target_user_ids())
    )


def _target_user_ids() -> list[str]:
    return [str(user_id).strip() for user_id in ONBOARDING_NOTIFY_USER_IDS if str(user_id).strip()]


def _target_user_names() -> list[str]:
    return [str(name).strip() for name in ONBOARDING_NOTIFY_USER_NAMES if str(name).strip()]


def _build_message(list_name: str, status_name: str) -> str:
    safe_list = str(list_name or "Lista desconhecida").strip().replace("(", "").replace(")", "")
    safe_status = _format_status(status_name)
    return f"Novo cooperado na lista {safe_list}, no status {safe_status}"


def _build_status_change_message(old_status: str, new_status: str) -> str:
    previous = _format_status(old_status)
    current = _format_status(new_status)
    return f"Status de cooperado alterado de: {previous} -> {current}"


def _format_status(value: str | None) -> str:
    text = " ".join(str(value or "Status desconhecido").strip().split())
    if not text:
        return "Status desconhecido"
    return text[:1].upper() + text[1:]


def _post_notification(task_id: str, list_name: str, status_name: str) -> dict | None:
    if not _is_enabled():
        return None

    message = _build_message(list_name, status_name)
    result = create_task_comment_with_tags_any(
        task_id=task_id,
        text_prefix=message,
        user_ids=_target_user_ids(),
        user_names_fallback=_target_user_names(),
    )
    logger.info(
        "onboarding_notify.ok task_id=%s lista='%s' status='%s' user_ids=%s",
        task_id,
        list_name,
        status_name,
        ",".join(_target_user_ids()),
    )
    return result


def run_task_created(task_id: str) -> dict | None:
    """Notifica quando task chega (taskCreated) em lista de onboarding."""
    if not _is_enabled():
        return None

    task_data = fetch_task_any(task_id)
    list_id = task_list_id(task_data)
    if list_id not in _target_lists():
        return None

    return _post_notification(
        task_id=task_id,
        list_name=task_list_name(task_data),
        status_name=task_status_value(task_data),
    )


def run_status_change(
    context: StatusChangeContext,
    *,
    old_status_override: str | None = None,
) -> dict | None:
    """Notifica quando task muda status em lista de onboarding."""
    if not _is_enabled():
        return None

    if str(context.source_list_id).strip() not in _target_lists():
        return None

    old_status = old_status_override or context.current_status_raw
    if normalize_status(old_status) == normalize_status(context.new_status):
        logger.debug(
            "onboarding_notify.status_change.skip sem_mudanca task_id=%s old='%s' new='%s'",
            context.task_id,
            old_status,
            context.new_status,
        )
        return None

    message = _build_status_change_message(
        old_status=old_status,
        new_status=context.new_status,
    )
    result = create_task_comment_with_tags_any(
        task_id=context.task_id,
        text_prefix=message,
        user_ids=_target_user_ids(),
        user_names_fallback=_target_user_names(),
    )
    logger.info(
        "onboarding_notify.status_change.ok task_id=%s lista='%s' old_status='%s' new_status='%s' user_ids=%s",
        context.task_id,
        context.source_list_name,
        old_status,
        context.new_status,
        ",".join(_target_user_ids()),
    )
    return result
