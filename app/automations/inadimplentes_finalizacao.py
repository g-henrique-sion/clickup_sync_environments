"""Finalizacao de negativacao na lista de Inadimplentes."""

from __future__ import annotations

import logging

from app.automations.common import (
    StatusChangeContext,
    fetch_task_any,
    normalize_status,
    task_status_value,
)
from app.config.settings import (
    INADIMPLENTES_ALLOW_TASK_ATTACHMENT_COMPROVANTE,
    INADIMPLENTES_COMPROVANTE_FIELD_ID,
    INADIMPLENTES_COMPROVANTE_FIELD_NAME,
    INADIMPLENTES_DONE_STATUS_NAMES,
    INADIMPLENTES_DONE_STATUS_TYPES,
    INADIMPLENTES_FINALIZACAO_ENABLED,
    INADIMPLENTES_FROM_STATUS,
    INADIMPLENTES_LIST_ID,
    INADIMPLENTES_PAID_STATUS,
    INADIMPLENTES_READY_STATUS,
    INADIMPLENTES_REQUIRED_SUBTASK_NAMES,
)
from app.core.clickup_client import (
    create_subtask_in_any_list,
    create_task_comment_any,
    resolve_list_status_name_any,
    task_permalink,
    update_task_status_any,
)

logger = logging.getLogger(__name__)


def _target_list_id() -> str:
    return str(INADIMPLENTES_LIST_ID or "").strip()


def _required_subtask_names() -> list[str]:
    return [
        str(name or "").strip()
        for name in INADIMPLENTES_REQUIRED_SUBTASK_NAMES
        if str(name or "").strip()
    ]


def _is_enabled() -> bool:
    return (
        INADIMPLENTES_FINALIZACAO_ENABLED
        and bool(_target_list_id())
        and bool(_required_subtask_names())
    )


def _is_subtask(task_data: dict) -> bool:
    return bool(str(task_data.get("parent") or "").strip())


def _status_name_from_task(task_data: dict) -> str:
    return task_status_value(task_data)


def _status_type_and_name(task_data: dict) -> tuple[str, str]:
    raw_status = task_data.get("status")
    if isinstance(raw_status, dict):
        status_type = str(raw_status.get("type") or "").strip().lower()
        status_name = str(raw_status.get("status") or raw_status.get("name") or "").strip()
        return status_type, status_name
    return "", _status_name_from_task(task_data)


def _subtasks(task_data: dict) -> list[dict]:
    return [item for item in (task_data.get("subtasks") or []) if isinstance(item, dict)]


def _subtask_name(subtask: dict) -> str:
    return str(subtask.get("name") or "").strip()


def _subtask_id(subtask: dict) -> str:
    return str(subtask.get("id") or "").strip()


def _subtask_by_normalized_name(task_data: dict) -> dict[str, dict]:
    indexed: dict[str, dict] = {}
    for subtask in _subtasks(task_data):
        normalized_name = normalize_status(_subtask_name(subtask))
        if normalized_name:
            indexed.setdefault(normalized_name, subtask)
    return indexed


def _is_done_subtask(subtask: dict) -> bool:
    status_type, status_name = _status_type_and_name(subtask)
    allowed_types = {
        str(value or "").strip().lower()
        for value in INADIMPLENTES_DONE_STATUS_TYPES
        if str(value or "").strip()
    }
    allowed_names = {
        normalize_status(value)
        for value in INADIMPLENTES_DONE_STATUS_NAMES
        if str(value or "").strip()
    }

    if status_type and status_type in allowed_types:
        return True
    if normalize_status(status_name) in allowed_names:
        return True
    if subtask.get("date_closed") or subtask.get("date_done"):
        return True
    return False


def _field_value_has_attachment(value) -> bool:
    if value in (None, "", [], {}):
        return False
    if isinstance(value, list):
        return any(_field_value_has_attachment(item) for item in value)
    if isinstance(value, dict):
        for key in ("id", "url", "download_url", "url_w_query", "title", "name"):
            if value.get(key):
                return True
        return bool(value)
    return bool(str(value).strip())


def _find_comprovante_field(task_data: dict) -> dict | None:
    expected_id = str(INADIMPLENTES_COMPROVANTE_FIELD_ID or "").strip()
    expected_name = normalize_status(INADIMPLENTES_COMPROVANTE_FIELD_NAME)

    for custom_field in task_data.get("custom_fields", []) or []:
        if not isinstance(custom_field, dict):
            continue
        field_id = str(custom_field.get("id") or "").strip()
        if expected_id and field_id == expected_id:
            return custom_field

        if expected_id:
            continue

        field_name = normalize_status(custom_field.get("name"))
        if expected_name and field_name == expected_name:
            return custom_field
    return None


def _has_task_attachment(task_data: dict) -> bool:
    return any(isinstance(item, dict) for item in (task_data.get("attachments") or []))


def _has_required_comprovante(task_data: dict) -> tuple[bool, str]:
    field = _find_comprovante_field(task_data)
    if field is not None:
        label = str(field.get("name") or field.get("id") or "campo de comprovante").strip()
        return _field_value_has_attachment(field.get("value")), f"campo '{label}'"

    if str(INADIMPLENTES_COMPROVANTE_FIELD_ID or "").strip():
        return False, f"campo id {INADIMPLENTES_COMPROVANTE_FIELD_ID}"

    if INADIMPLENTES_ALLOW_TASK_ATTACHMENT_COMPROVANTE:
        return _has_task_attachment(task_data), "anexos da task"

    return False, "campo de comprovante"


def _missing_subtasks(task_data: dict) -> list[str]:
    existing_by_name = _subtask_by_normalized_name(task_data)
    return [
        name
        for name in _required_subtask_names()
        if normalize_status(name) not in existing_by_name
    ]


def _pending_required_subtasks(task_data: dict) -> list[str]:
    existing_by_name = _subtask_by_normalized_name(task_data)
    pending: list[str] = []
    for name in _required_subtask_names():
        subtask = existing_by_name.get(normalize_status(name))
        if subtask is None:
            continue

        subtask_data = subtask
        if not subtask_data.get("status") and _subtask_id(subtask_data):
            try:
                subtask_data = fetch_task_any(_subtask_id(subtask_data))
            except Exception:
                logger.warning(
                    "inadimplentes_finalizacao.subtask_fetch_falhou parent_task_id=%s subtask_id=%s",
                    task_data.get("id"),
                    _subtask_id(subtask_data),
                )

        if not _is_done_subtask(subtask_data):
            pending.append(name)
    return pending


def _create_missing_subtasks(
    context: StatusChangeContext,
    *,
    old_status: str | None,
) -> dict | None:
    if context.normalized_new_status != normalize_status(INADIMPLENTES_READY_STATUS):
        return None

    if normalize_status(old_status) != normalize_status(INADIMPLENTES_FROM_STATUS):
        logger.debug(
            "inadimplentes_finalizacao.skip origem_status_nao_trigger task_id=%s old_status='%s' new_status='%s'",
            context.task_id,
            old_status or "",
            context.new_status,
        )
        return None

    missing_names = _missing_subtasks(context.task_data)
    if not missing_names:
        logger.info(
            "inadimplentes_finalizacao.subtasks.noop task_id=%s motivo=ja_existentes link=%s",
            context.task_id,
            task_permalink(context.task_id),
        )
        return {"task_id": context.task_id, "created": [], "skipped": "already_exists"}

    created: list[dict] = []
    for name in missing_names:
        created.append(
            create_subtask_in_any_list(
                list_id=context.source_list_id,
                parent_task_id=context.task_id,
                name=name,
            )
        )

    logger.info(
        "inadimplentes_finalizacao.subtasks.ok task_id=%s criadas=%d nomes=%s link=%s",
        context.task_id,
        len(created),
        missing_names,
        task_permalink(context.task_id),
    )
    return {
        "task_id": context.task_id,
        "created": [str(item.get("id") or "") for item in created],
        "created_names": missing_names,
    }


def _build_block_comment(missing: list[str], pending: list[str], comprovante_label: str) -> str:
    lines = [
        "Movimento para PAGO bloqueado automaticamente.",
        "",
        "Pendencias para finalizar a baixa da negativacao:",
    ]
    for name in missing:
        lines.append(f"- Subtarefa ausente: {name}")
    for name in pending:
        lines.append(f"- Subtarefa pendente: {name}")
    if comprovante_label:
        lines.append(f"- Comprovante nao anexado em {comprovante_label}")
    return "\n".join(lines)


def _guard_paid_status(
    context: StatusChangeContext,
    *,
    old_status: str | None,
) -> dict | None:
    if context.normalized_new_status != normalize_status(INADIMPLENTES_PAID_STATUS):
        return None

    if normalize_status(old_status) != normalize_status(INADIMPLENTES_READY_STATUS):
        logger.debug(
            "inadimplentes_finalizacao.pago.skip origem_status_nao_validada task_id=%s old_status='%s' new_status='%s'",
            context.task_id,
            old_status or "",
            context.new_status,
        )
        return None

    latest_task = fetch_task_any(context.task_id)
    missing = _missing_subtasks(latest_task)
    pending = _pending_required_subtasks(latest_task)
    has_comprovante, comprovante_label = _has_required_comprovante(latest_task)

    if not missing and not pending and has_comprovante:
        logger.info(
            "inadimplentes_finalizacao.pago.permitido task_id=%s link=%s",
            context.task_id,
            task_permalink(context.task_id),
        )
        return None

    fallback_status = (
        resolve_list_status_name_any(context.source_list_id, INADIMPLENTES_READY_STATUS)
        or INADIMPLENTES_READY_STATUS
    )
    update_task_status_any(context.task_id, fallback_status)

    missing_comprovante_label = "" if has_comprovante else comprovante_label
    comment = _build_block_comment(missing, pending, missing_comprovante_label)
    try:
        create_task_comment_any(context.task_id, comment)
    except Exception:
        logger.exception(
            "inadimplentes_finalizacao.comentario_bloqueio_falhou task_id=%s",
            context.task_id,
        )

    logger.warning(
        "inadimplentes_finalizacao.pago.bloqueado task_id=%s voltou_para='%s' missing=%s pending=%s comprovante=%s link=%s",
        context.task_id,
        fallback_status,
        missing,
        pending,
        has_comprovante,
        task_permalink(context.task_id),
    )
    return {
        "task_id": context.task_id,
        "blocked": True,
        "reverted_to": fallback_status,
        "missing_subtasks": missing,
        "pending_subtasks": pending,
        "has_comprovante": has_comprovante,
    }


def run(
    context: StatusChangeContext,
    *,
    old_status: str | None = None,
) -> dict | None:
    if not _is_enabled():
        return None

    if str(context.source_list_id).strip() != _target_list_id():
        return None

    if _is_subtask(context.task_data):
        logger.debug(
            "inadimplentes_finalizacao.skip subtask task_id=%s parent=%s status='%s'",
            context.task_id,
            context.task_data.get("parent"),
            context.new_status,
        )
        return None

    created = _create_missing_subtasks(context, old_status=old_status)
    if created is not None:
        return created

    return _guard_paid_status(context, old_status=old_status)
