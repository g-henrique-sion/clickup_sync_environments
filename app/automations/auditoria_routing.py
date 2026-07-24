"""Routing automation for auditoria status based on plano de adesao."""

from __future__ import annotations

import logging
import time

from app.automations.common import StatusChangeContext, normalize_status
from app.automations.task_name_formatter import (
    build_formatted_task_name,
    should_format_task_name,
)
from app.config.settings import (
    AUDITORIA_RATEIO_BLACK_LIST_ID,
    AUDITORIA_RATEIO_ONGOING_LIST_ID,
    AUDITORIA_RATEIO_TRIGGER_STATUS,
    AUDITORIA_ROUTING_BLACK_VALUES,
    AUDITORIA_ROUTING_ONBOARDING_BLACK_LIST_ID,
    AUDITORIA_ROUTING_ONBOARDING_LIST_ID,
    AUDITORIA_ROUTING_ONBOARDING_NEW_TASK_STATUS,
    AUDITORIA_ROUTING_PLAN_FIELD_ID,
    AUDITORIA_ROUTING_SOURCE_LIST_IDS,
    AUDITORIA_ROUTING_TRIGGER_STATUS,
)
from app.core.clickup_client import (
    add_task_link_any,
    clone_attachments_dest_to_source,
    clone_comments_dest_to_source,
    create_task_in_any_list,
    get_list_fields_any,
    move_task_to_list_any,
    resolve_list_status_name_any,
    set_task_custom_item_any,
    task_permalink,
    update_task_status_any,
)

logger = logging.getLogger(__name__)

_ATTACHMENT_TYPES = {"attachment", "file", "files", "file_attachment", "file_upload"}
_BLOCKED_TYPES = {"formula", "rollup", "progress", "automatic_progress", "button"}
_MILESTONE_CUSTOM_ITEM_ID = 1
_COPY_RETRIES = 3
_COPY_RETRY_SLEEP_SECONDS = 1.0
_target_list_field_ids_cache: dict[str, set[str]] = {}


def _normalize_field_type(value: str | None) -> str:
    field_type = str(value or "").strip().lower()
    alias = {
        "dropdown": "drop_down",
        "text": "short_text",
        "textarea": "short_text",
        "long_text": "short_text",
        "website": "url",
        "money": "currency",
        "file": "attachment",
        "files": "attachment",
        "file_attachment": "attachment",
        "file_upload": "attachment",
    }
    return alias.get(field_type, field_type)


def _expected_custom_fields_count(task_data: dict) -> int:
    total = 0
    for cf in task_data.get("custom_fields", []) or []:
        field_id = str(cf.get("id") or "").strip()
        if not field_id:
            continue
        field_type = _normalize_field_type(cf.get("type"))
        if field_type in _ATTACHMENT_TYPES or field_type in _BLOCKED_TYPES:
            continue
        if cf.get("value") is None:
            continue
        total += 1
    return total


def _get_target_list_field_ids(list_id: str) -> set[str]:
    cached = _target_list_field_ids_cache.get(str(list_id))
    if cached is not None:
        return cached

    fields = get_list_fields_any(list_id)
    resolved = {
        str(field.get("id") or "").strip()
        for field in fields
        if str(field.get("id") or "").strip()
    }
    _target_list_field_ids_cache[str(list_id)] = resolved
    return resolved


def _build_direct_custom_fields_payload(
    task_data: dict,
    *,
    allowed_field_ids: set[str] | None = None,
) -> list[dict]:
    fields: list[dict] = []
    for cf in task_data.get("custom_fields", []) or []:
        field_id = str(cf.get("id") or "").strip()
        if not field_id:
            continue
        if allowed_field_ids is not None and field_id not in allowed_field_ids:
            continue
        field_type = _normalize_field_type(cf.get("type"))
        if field_type in _ATTACHMENT_TYPES or field_type in _BLOCKED_TYPES:
            continue
        value = cf.get("value")
        if value is None:
            continue
        fields.append({"id": field_id, "value": value})
    return fields


def _find_custom_field(task_data: dict, field_id: str) -> dict | None:
    target = str(field_id or "").strip()
    if not target:
        return None
    for cf in task_data.get("custom_fields", []) or []:
        if str(cf.get("id") or "").strip() == target:
            return cf
    return None


def _resolve_dropdown_label(cf: dict) -> str:
    value = cf.get("value")
    if value is None:
        return ""

    field_type = _normalize_field_type(cf.get("type"))
    if field_type != "drop_down":
        return str(value).strip()

    cfg = cf.get("type_config") or {}
    options = cfg.get("options") or []
    value_token = str(value)
    for opt in options:
        label = str(opt.get("name") or opt.get("label") or "").strip()
        if not label:
            continue
        for token in (opt.get("id"), opt.get("orderindex"), opt.get("value")):
            if token is None:
                continue
            if str(token) == value_token:
                return label
    return str(value).strip()


def _resolve_plano_label(task_data: dict) -> str:
    cf = _find_custom_field(task_data, AUDITORIA_ROUTING_PLAN_FIELD_ID)
    if not cf:
        return ""
    return _resolve_dropdown_label(cf)


def _is_black_plan(plano_label: str) -> bool:
    normalized_plano = normalize_status(plano_label)
    if not normalized_plano:
        return False
    allowed = {
        normalize_status(value)
        for value in AUDITORIA_ROUTING_BLACK_VALUES
        if str(value).strip()
    }
    return normalized_plano in allowed


def _resolve_target_for_trigger(normalized_status: str, plano_label: str) -> tuple[str, str]:
    status_auditoria = normalize_status(AUDITORIA_ROUTING_TRIGGER_STATUS)
    status_rateio = normalize_status(AUDITORIA_RATEIO_TRIGGER_STATUS)
    is_black = _is_black_plan(plano_label)

    if normalized_status == status_auditoria:
        if is_black:
            return "auditoria", AUDITORIA_ROUTING_ONBOARDING_BLACK_LIST_ID
        return "auditoria", AUDITORIA_ROUTING_ONBOARDING_LIST_ID

    if normalized_status == status_rateio:
        if is_black:
            return "rateio", AUDITORIA_RATEIO_BLACK_LIST_ID
        return "rateio", AUDITORIA_RATEIO_ONGOING_LIST_ID

    return "", ""


def _run_copy_with_retry(label: str, fn):
    last_exc: Exception | None = None
    for attempt in range(1, _COPY_RETRIES + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt >= _COPY_RETRIES:
                break
            logger.warning(
                "%s falhou tentativa=%d/%d. retry em %.1fs",
                label,
                attempt,
                _COPY_RETRIES,
                _COPY_RETRY_SLEEP_SECONDS * attempt,
            )
            time.sleep(_COPY_RETRY_SLEEP_SECONDS * attempt)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{label} falhou sem excecao definida")


def run(context: StatusChangeContext) -> dict | None:
    if context.source_list_id not in set(AUDITORIA_ROUTING_SOURCE_LIST_IDS):
        return None

    plano_label = _resolve_plano_label(context.task_data)
    route_key, target_list_id = _resolve_target_for_trigger(
        context.normalized_new_status,
        plano_label,
    )
    if not route_key:
        return None

    if not target_list_id:
        logger.warning(
            "auditoria_routing.skip lista_destino_nao_configurada task_id=%s rota=%s plano='%s'",
            context.task_id,
            route_key,
            plano_label,
        )
        return None

    original_name = context.task_name
    name = (
        build_formatted_task_name(
            context.task_data,
            original_name,
            target_list_id,
        )
        if should_format_task_name(target_list_id)
        else original_name
    )
    description = context.task_data.get("description", "")
    target_field_ids = _get_target_list_field_ids(target_list_id)
    allowed_target_field_ids: set[str] | None = target_field_ids or None
    custom_fields = _build_direct_custom_fields_payload(
        context.task_data,
        allowed_field_ids=allowed_target_field_ids,
    )
    expected_custom_fields = _expected_custom_fields_count(context.task_data)
    movable_field_ids = [
        str(cf.get("id") or "").strip()
        for cf in (context.task_data.get("custom_fields", []) or [])
        if str(cf.get("id") or "").strip()
        and (
            allowed_target_field_ids is None
            or str(cf.get("id") or "").strip() in allowed_target_field_ids
        )
    ]

    logger.info(
        "auditoria_routing.inicio task_id=%s task='%s' rota=%s status='%s' plano='%s' origem_lista=%s destino_lista=%s",
        context.task_id,
        context.task_name,
        route_key,
        context.new_status,
        plano_label or "nao_informado",
        context.source_list_id,
        target_list_id,
    )
    logger.info(
        "auditoria_routing.custom_fields.preparado task_id=%s enviados=%d esperados=%d status=%s",
        context.task_id,
        len(custom_fields),
        expected_custom_fields,
        "ok" if len(custom_fields) == expected_custom_fields else "parcial",
    )
    if allowed_target_field_ids is not None:
        logger.info(
            "auditoria_routing.schema_destino.aplicado task_id=%s destino_lista=%s campos_permitidos=%d descartados=%d",
            context.task_id,
            target_list_id,
            len(allowed_target_field_ids),
            max(0, expected_custom_fields - len(custom_fields)),
        )

    if route_key == "rateio":
        moved = move_task_to_list_any(
            task_id=context.task_id,
            target_list_id=target_list_id,
            source_status_name=context.current_status_raw or context.new_status,
            custom_fields_to_move=movable_field_ids,
        )
        logger.info(
            "auditoria_routing.rateio.movida task_id=%s origem_lista=%s destino_lista=%s link=%s",
            context.task_id,
            context.source_list_id,
            target_list_id,
            task_permalink(context.task_id),
        )
        return moved

    created = create_task_in_any_list(
        list_id=target_list_id,
        name=name,
        description=description,
        custom_fields=custom_fields or None,
        custom_item_id=(
            _MILESTONE_CUSTOM_ITEM_ID
            if target_list_id
            in {
                AUDITORIA_ROUTING_ONBOARDING_LIST_ID,
                AUDITORIA_ROUTING_ONBOARDING_BLACK_LIST_ID,
            }
            else None
        ),
    )
    created_id = str(created.get("id") or "")

    logger.info(
        "auditoria_routing.task_criada auditoria_task_id=%s destino_task_id=%s auditoria_link=%s destino_link=%s",
        context.task_id,
        created_id,
        task_permalink(context.task_id),
        task_permalink(created_id),
    )

    if target_list_id in {
        AUDITORIA_ROUTING_ONBOARDING_LIST_ID,
        AUDITORIA_ROUTING_ONBOARDING_BLACK_LIST_ID,
    }:
        target_status_name = (
            resolve_list_status_name_any(
                target_list_id, AUDITORIA_ROUTING_ONBOARDING_NEW_TASK_STATUS
            )
            or AUDITORIA_ROUTING_ONBOARDING_NEW_TASK_STATUS
        )
        try:
            set_task_custom_item_any(created_id, _MILESTONE_CUSTOM_ITEM_ID)
            update_task_status_any(created_id, target_status_name)
            add_task_link_any(context.task_id, created_id)
            logger.info(
                "auditoria_routing.relacionamento_milestone origem_task_id=%s destino_task_id=%s custom_item_id=%s status_aplicado='%s'",
                context.task_id,
                created_id,
                _MILESTONE_CUSTOM_ITEM_ID,
                target_status_name,
            )
        except Exception:
            logger.exception(
                "auditoria_routing.erro_relacionamento_milestone origem_task_id=%s destino_task_id=%s",
                context.task_id,
                created_id,
            )

    try:
        attachment_result = _run_copy_with_retry(
            "auditoria_routing.clone_attachments",
            lambda: clone_attachments_dest_to_source(
                context.task_data,
                created_id,
                allowed_source_field_ids=allowed_target_field_ids,
            ),
        )
        logger.info(
            "auditoria_routing.anexos auditoria_task_id=%s destino_task_id=%s enviados=%d tentados=%d falhas=%d status=%s",
            context.task_id,
            created_id,
            attachment_result.get("sent", 0),
            attachment_result.get("attempted", 0),
            attachment_result.get("failed", 0),
            "ok" if attachment_result.get("ok") else "parcial",
        )
    except Exception:
        logger.exception(
            "auditoria_routing.erro_anexos auditoria_task_id=%s destino_task_id=%s",
            context.task_id,
            created_id,
        )

    try:
        comments_result = _run_copy_with_retry(
            "auditoria_routing.clone_comments",
            lambda: clone_comments_dest_to_source(context.task_id, created_id),
        )
        logger.info(
            "auditoria_routing.comentarios auditoria_task_id=%s destino_task_id=%s comentarios=%d respostas=%d status=%s",
            context.task_id,
            created_id,
            comments_result.get("comments", 0),
            comments_result.get("replies", 0),
            "ok" if comments_result.get("ok") else "parcial",
        )
    except Exception:
        logger.exception(
            "auditoria_routing.erro_comentarios auditoria_task_id=%s destino_task_id=%s",
            context.task_id,
            created_id,
        )

    logger.info(
        "auditoria_routing.concluido auditoria_task_id=%s destino_task_id=%s rota=%s plano='%s' destino_lista=%s",
        context.task_id,
        created_id,
        route_key,
        plano_label or "nao_informado",
        target_list_id,
    )
    return created
