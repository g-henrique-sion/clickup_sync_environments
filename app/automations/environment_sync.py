"""Environment sync automation (source -> destino and return flow)."""

from __future__ import annotations

import logging
import threading
import time

from app.automations.common import StatusChangeContext, normalize_status
from app.automations.task_name_formatter import (
    build_formatted_task_name,
    should_format_task_name,
)
from app.config.settings import (
    CLONE_FIELD_MAP,
    DEST_LIST_ID,
    DEST_RETURN_TRIGGER_STATUS,
    SOURCE_LIST_ID,
    SOURCE_LIST_MAP,
    SOURCE_RETURN_LIST_ID,
    SOURCE_RETURN_TRIGGER_STATUS,
)
from app.core.clickup_client import (
    build_custom_fields_payload,
    build_reverse_custom_fields_payload,
    clone_attachments,
    clone_attachments_dest_to_source,
    clone_comments,
    clone_comments_dest_to_source,
    create_task_in_dest,
    create_task_in_source_list,
    delete_task_in_dest,
    get_list_fields_any,
    task_permalink,
)

logger = logging.getLogger(__name__)

_ATTACHMENT_TYPES = {"attachment", "file", "files", "file_attachment", "file_upload"}
_BLOCKED_TYPES = {"formula", "rollup", "progress", "automatic_progress", "button"}
_inflight_return_hydration: set[str] = set()
_inflight_return_hydration_lock = threading.Lock()
_target_list_field_ids_cache: dict[str, set[str]] = {}
_COPY_RETRIES = 3
_COPY_RETRY_SLEEP_SECONDS = 1.0


def _mark_return_hydration_start(task_id: str) -> None:
    with _inflight_return_hydration_lock:
        _inflight_return_hydration.add(str(task_id))


def _mark_return_hydration_done(task_id: str) -> None:
    with _inflight_return_hydration_lock:
        _inflight_return_hydration.discard(str(task_id))


def _is_return_hydration_inflight(task_id: str) -> bool:
    with _inflight_return_hydration_lock:
        return str(task_id) in _inflight_return_hydration


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


def _clone_to_dest_from_source(
    context: StatusChangeContext,
    *,
    is_source_return_to_dest: bool,
) -> dict:
    task_data = context.task_data
    source_list_id = context.source_list_id
    task_id = context.task_id

    original_name = task_data.get("name", "Sem nome")
    name = (
        build_formatted_task_name(task_data, original_name, DEST_LIST_ID)
        if should_format_task_name(DEST_LIST_ID)
        else original_name
    )
    description = task_data.get("description", "")
    restrict_to_dest_schema = (
        not is_source_return_to_dest and source_list_id == SOURCE_LIST_ID
    )
    allowed_dest_field_ids: set[str] | None = None
    if restrict_to_dest_schema:
        resolved_ids = _get_target_list_field_ids(DEST_LIST_ID)
        if resolved_ids:
            allowed_dest_field_ids = resolved_ids
        else:
            logger.warning(
                "env_sync.schema_destino.indisponivel origem_task_id=%s origem_lista=%s destino_lista=%s. "
                "Seguindo sem filtro para nao bloquear o fluxo.",
                task_id,
                source_list_id,
                DEST_LIST_ID,
            )

    custom_fields = build_custom_fields_payload(
        task_data,
        allowed_field_ids=allowed_dest_field_ids,
    )
    expected_custom_fields = _expected_custom_fields_count(task_data)

    logger.info(
        "env_sync.inicio origem_task_id=%s origem_lista=%s trigger_status='%s'",
        task_id,
        source_list_id,
        context.new_status,
    )
    logger.info(
        "env_sync.custom_fields.preparado origem_task_id=%s enviados=%d esperados=%d status=%s",
        task_id,
        len(custom_fields),
        expected_custom_fields,
        "ok" if len(custom_fields) == expected_custom_fields else "parcial",
    )
    if allowed_dest_field_ids is not None:
        logger.info(
            "env_sync.schema_destino.aplicado origem_task_id=%s origem_lista=%s destino_lista=%s campos_permitidos=%d descartados=%d",
            task_id,
            source_list_id,
            DEST_LIST_ID,
            len(allowed_dest_field_ids),
            max(0, expected_custom_fields - len(custom_fields)),
        )
    logger.info(
        "Clonando task '%s' (%s) da lista %s com %d custom fields (map=%d)...",
        name,
        task_id,
        source_list_id,
        len(custom_fields),
        len(CLONE_FIELD_MAP),
    )

    created = create_task_in_dest(
        name=name,
        description=description,
        custom_fields=custom_fields or None,
    )
    created_id = str(created.get("id") or "")
    logger.info(
        "env_sync.task_criada origem_task_id=%s destino_task_id=%s origem_link=%s destino_link=%s",
        task_id,
        created_id,
        task_permalink(task_id),
        task_permalink(created_id),
    )

    try:
        attachment_result = _run_copy_with_retry(
            "env_sync.clone_attachments",
            lambda: clone_attachments(
                task_data,
                created_id,
                allowed_dest_field_ids=allowed_dest_field_ids,
            ),
        )
        logger.info(
            "env_sync.anexos origem_task_id=%s destino_task_id=%s enviados=%d tentados=%d falhas=%d status=%s",
            task_id,
            created_id,
            attachment_result.get("sent", 0),
            attachment_result.get("attempted", 0),
            attachment_result.get("failed", 0),
            "ok" if attachment_result.get("ok") else "parcial",
        )
    except Exception as e:
        logger.exception("Falha ao clonar anexos da task %s: %s", task_id, e)

    try:
        comment_result = _run_copy_with_retry(
            "env_sync.clone_comments",
            lambda: clone_comments(task_id, created_id),
        )
        logger.info(
            "env_sync.comentarios origem_task_id=%s destino_task_id=%s comentarios=%d respostas=%d status=%s",
            task_id,
            created_id,
            comment_result.get("comments", 0),
            comment_result.get("replies", 0),
            "ok" if comment_result.get("ok") else "parcial",
        )
    except Exception as e:
        logger.exception("Falha ao clonar comentarios da task %s: %s", task_id, e)

    if is_source_return_to_dest:
        logger.info(
            "Retorno concluido: task source %s (lista %s) -> destino id=%s link=%s (origem preservada)",
            task_id,
            source_list_id,
            created_id,
            task_permalink(created_id),
        )
    else:
        logger.info(
            "Clone concluido: '%s' (lista %s) -> destino id=%s link=%s",
            name,
            source_list_id,
            created_id,
            task_permalink(created_id),
        )

    return created


def run(context: StatusChangeContext) -> dict | None:
    task_data = context.task_data
    source_list_id = context.source_list_id
    source_list_name = context.source_list_name
    task_id = context.task_id

    if (
        source_list_id == DEST_LIST_ID
        and context.normalized_new_status == normalize_status(DEST_RETURN_TRIGGER_STATUS)
    ):
        original_return_name = task_data.get("name", "Sem nome")
        return_name = (
            build_formatted_task_name(
                task_data,
                original_return_name,
                SOURCE_RETURN_LIST_ID,
            )
            if should_format_task_name(SOURCE_RETURN_LIST_ID)
            else original_return_name
        )
        return_description = task_data.get("description", "")
        return_custom_fields = build_reverse_custom_fields_payload(task_data)
        created_return = create_task_in_source_list(
            list_id=SOURCE_RETURN_LIST_ID,
            name=return_name,
            description=return_description,
            custom_fields=return_custom_fields or None,
        )
        created_return_id = str(created_return.get("id") or "")
        logger.info(
            "env_sync.retorno.task_criada origem_task_id=%s retorno_task_id=%s origem_link=%s retorno_link=%s",
            task_id,
            created_return_id,
            task_permalink(task_id),
            task_permalink(created_return_id),
        )

        _mark_return_hydration_start(created_return_id)
        try:
            attachment_result = _run_copy_with_retry(
                "env_sync.retorno.clone_attachments",
                lambda: clone_attachments_dest_to_source(task_data, created_return_id),
            )
            logger.info(
                "env_sync.retorno.anexos origem_task_id=%s retorno_task_id=%s enviados=%d tentados=%d falhas=%d status=%s",
                task_id,
                created_return_id,
                attachment_result.get("sent", 0),
                attachment_result.get("attempted", 0),
                attachment_result.get("failed", 0),
                "ok" if attachment_result.get("ok") else "parcial",
            )

            comment_result = _run_copy_with_retry(
                "env_sync.retorno.clone_comments",
                lambda: clone_comments_dest_to_source(task_id, created_return_id),
            )
            logger.info(
                "env_sync.retorno.comentarios origem_task_id=%s retorno_task_id=%s comentarios=%d respostas=%d status=%s",
                task_id,
                created_return_id,
                comment_result.get("comments", 0),
                comment_result.get("replies", 0),
                "ok" if comment_result.get("ok") else "parcial",
            )

            delete_task_in_dest(task_id)
        finally:
            _mark_return_hydration_done(created_return_id)

        logger.info(
            "Retorno concluido: task destino %s -> lista source %s (id=%s link=%s)",
            task_id,
            SOURCE_RETURN_LIST_ID,
            created_return_id,
            task_permalink(created_return_id),
        )
        return created_return

    is_source_return_to_dest = (
        source_list_id == SOURCE_RETURN_LIST_ID
        and context.normalized_new_status == normalize_status(SOURCE_RETURN_TRIGGER_STATUS)
    )

    if not is_source_return_to_dest:
        trigger_status = SOURCE_LIST_MAP.get(source_list_id)
        if trigger_status is None:
            logger.debug(
                "process_status_change.skip lista_fora_source_map task_id=%s list_id=%s list_name='%s'",
                task_id,
                source_list_id,
                source_list_name,
            )
            return None

        if context.normalized_new_status != normalize_status(trigger_status):
            logger.debug(
                "process_status_change.skip status_nao_trigger task_id=%s list_id=%s status_evento='%s' status_normalizado='%s' trigger='%s'",
                task_id,
                source_list_id,
                context.new_status,
                context.normalized_new_status,
                normalize_status(trigger_status),
            )
            return None
    elif _is_return_hydration_inflight(task_id):
        # Se "corrigido" chegar enquanto a task ainda esta sendo hidratada
        # (anexos/comentarios), falha de forma retryable para nao perder dados.
        logger.warning(
            "env_sync.retorno.defer task_id=%s motivo=hydration_inflight status='%s'",
            task_id,
            context.new_status,
        )
        raise RuntimeError("source_return_hydration_inflight")

    return _clone_to_dest_from_source(
        context,
        is_source_return_to_dest=is_source_return_to_dest,
    )
