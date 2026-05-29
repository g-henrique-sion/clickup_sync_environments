"""Automation for taskCreated: Adesao Reprovada -> Demissoes milestone."""

from __future__ import annotations

import logging
import time
import unicodedata

from app.automations.common import fetch_task_any, task_list_id, task_name
from app.config.settings import (
    ADESAO_REPROVADA_LIST_ID,
    DEMISSOES_CREATE_STATUS,
    DEMISSOES_LIST_ID,
)
from app.core.clickup_client import (
    add_task_link_any,
    clone_attachments_dest_to_source,
    clone_comments_dest_to_source,
    create_task_in_any_list,
    get_list_fields_any,
    resolve_list_status_name_any,
    set_task_custom_field_any,
    task_permalink,
)

logger = logging.getLogger(__name__)

_MILESTONE_CUSTOM_ITEM_ID = 1
_ATTACHMENT_TYPES = {"attachment", "file", "files", "file_attachment", "file_upload"}
_BLOCKED_TYPES = {"formula", "rollup", "progress", "automatic_progress", "button"}
_LINK_MAX_RETRIES = 3
_LINK_RETRY_BASE_SECONDS = 1.0
_COPY_RETRIES = 3
_COPY_RETRY_SLEEP_SECONDS = 1.0


def _normalize_field_name(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


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


def _are_field_types_compatible(source_type: str, destination_type: str) -> bool:
    if not source_type or not destination_type:
        return False
    if source_type == destination_type:
        return True
    return {source_type, destination_type} <= {"short_text", "text", "textarea"}


def _index_destination_fields(destination_list_id: str) -> tuple[dict[str, dict], dict[str, list[dict]]]:
    fields = get_list_fields_any(destination_list_id)
    by_id: dict[str, dict] = {}
    by_name: dict[str, list[dict]] = {}
    for field in fields:
        field_id = str(field.get("id") or "").strip()
        if not field_id:
            continue
        by_id[field_id] = field
        field_name = _normalize_field_name(field.get("name"))
        if field_name:
            by_name.setdefault(field_name, []).append(field)
    return by_id, by_name


def _resolve_destination_field_id(
    source_field: dict,
    destination_fields_by_id: dict[str, dict],
    destination_fields_by_name: dict[str, list[dict]],
) -> str | None:
    source_id = str(source_field.get("id") or "").strip()
    source_type = _normalize_field_type(source_field.get("type"))
    source_name = _normalize_field_name(source_field.get("name"))

    if source_id:
        destination_field = destination_fields_by_id.get(source_id)
        if destination_field:
            destination_type = _normalize_field_type(destination_field.get("type"))
            if _are_field_types_compatible(source_type, destination_type):
                return source_id

    if source_name:
        candidates = destination_fields_by_name.get(source_name) or []
        for destination_field in candidates:
            destination_id = str(destination_field.get("id") or "").strip()
            destination_type = _normalize_field_type(destination_field.get("type"))
            if destination_id and _are_field_types_compatible(source_type, destination_type):
                return destination_id

    return None


def _collect_copyable_custom_fields(
    task_data: dict,
    destination_fields_by_id: dict[str, dict],
    destination_fields_by_name: dict[str, list[dict]],
    *,
    allow_direct_fallback: bool = False,
) -> list[dict]:
    items: list[dict] = []
    for custom_field in task_data.get("custom_fields", []) or []:
        field_type = _normalize_field_type(custom_field.get("type"))
        if field_type in _ATTACHMENT_TYPES or field_type in _BLOCKED_TYPES:
            continue

        value = custom_field.get("value")
        if value is None:
            continue

        destination_field_id = _resolve_destination_field_id(
            custom_field,
            destination_fields_by_id,
            destination_fields_by_name,
        )
        if not destination_field_id and allow_direct_fallback:
            # Fallback: sem catalogo da lista destino, tenta reaproveitar mesmo ID.
            destination_field_id = str(custom_field.get("id") or "").strip()
        if not destination_field_id:
            continue

        items.append(
            {
                "source_id": str(custom_field.get("id") or "").strip(),
                "source_name": str(custom_field.get("name") or "").strip(),
                "dest_id": destination_field_id,
                "value": value,
            }
        )
    return items


def _build_create_payload_custom_fields(copyable_fields: list[dict]) -> list[dict]:
    # Dedup por id destino para evitar enviar o mesmo campo duas vezes.
    by_destination_id: dict[str, dict] = {}
    for item in copyable_fields:
        destination_id = str(item.get("dest_id") or "").strip()
        if not destination_id or destination_id in by_destination_id:
            continue
        by_destination_id[destination_id] = {"id": destination_id, "value": item.get("value")}
    return list(by_destination_id.values())


def _apply_post_create_custom_fields(task_id: str, copyable_fields: list[dict]) -> dict[str, int]:
    # Segunda passada para maximizar recuperacao de campos (best-effort).
    attempted = 0
    applied = 0
    failed = 0
    processed_destination_ids: set[str] = set()
    for item in copyable_fields:
        destination_id = str(item.get("dest_id") or "").strip()
        if not destination_id or destination_id in processed_destination_ids:
            continue
        processed_destination_ids.add(destination_id)
        attempted += 1
        ok = set_task_custom_field_any(task_id, destination_id, item.get("value"))
        if ok:
            applied += 1
        else:
            failed += 1
    return {"attempted": attempted, "applied": applied, "failed": failed}


def _ensure_relationship(source_task_id: str, destination_task_id: str) -> bool:
    for attempt in range(1, _LINK_MAX_RETRIES + 1):
        try:
            add_task_link_any(source_task_id, destination_task_id)
            return True
        except Exception:
            if attempt >= _LINK_MAX_RETRIES:
                logger.exception(
                    "adesao_reprovada_demissoes.erro_relacionamento origem_task_id=%s destino_task_id=%s",
                    source_task_id,
                    destination_task_id,
                )
                return False
            time.sleep(_LINK_RETRY_BASE_SECONDS * attempt)
    return False


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


def run(task_id: str) -> dict | None:
    """Handles taskCreated event for Adesao Reprovada list."""
    if not ADESAO_REPROVADA_LIST_ID:
        return None

    if not DEMISSOES_LIST_ID:
        logger.warning(
            "adesao_reprovada_demissoes.skip demissoes_list_nao_configurada task_id=%s",
            task_id,
        )
        return None

    task_data = fetch_task_any(task_id)
    source_list_id = task_list_id(task_data)
    if source_list_id != ADESAO_REPROVADA_LIST_ID:
        return None

    # Evita duplicar automacao para subtarefa criada dentro da lista.
    if task_data.get("parent"):
        logger.debug(
            "adesao_reprovada_demissoes.skip subtarefa task_id=%s parent=%s",
            task_id,
            task_data.get("parent"),
        )
        return None

    try:
        destination_status = (
            resolve_list_status_name_any(DEMISSOES_LIST_ID, DEMISSOES_CREATE_STATUS)
            or DEMISSOES_CREATE_STATUS
        )
    except Exception:
        logger.warning(
            "adesao_reprovada_demissoes.status_resolve_falhou list_id=%s status='%s'. Usando status configurado sem validacao.",
            DEMISSOES_LIST_ID,
            DEMISSOES_CREATE_STATUS,
        )
        destination_status = DEMISSOES_CREATE_STATUS
    destination_fields_by_id, destination_fields_by_name = _index_destination_fields(
        DEMISSOES_LIST_ID
    )
    has_destination_catalog = bool(destination_fields_by_id or destination_fields_by_name)
    copyable_custom_fields = _collect_copyable_custom_fields(
        task_data,
        destination_fields_by_id,
        destination_fields_by_name,
        allow_direct_fallback=not has_destination_catalog,
    )
    custom_fields_create_payload = (
        _build_create_payload_custom_fields(copyable_custom_fields)
        if has_destination_catalog
        else []
    )

    created = create_task_in_any_list(
        list_id=DEMISSOES_LIST_ID,
        name=task_name(task_data),
        description=task_data.get("description", ""),
        custom_fields=custom_fields_create_payload or None,
        custom_item_id=_MILESTONE_CUSTOM_ITEM_ID,
        status=destination_status,
    )

    created_id = str(created.get("id") or "")

    linked = _ensure_relationship(task_id, created_id)

    custom_fields_post_result = _apply_post_create_custom_fields(created_id, copyable_custom_fields)
    logger.info(
        "adesao_reprovada_demissoes.custom_fields origem_task_id=%s destino_task_id=%s create_payload=%d pos_create_aplicados=%d tentados=%d falhas=%d status=%s",
        task_id,
        created_id,
        len(custom_fields_create_payload),
        custom_fields_post_result["applied"],
        custom_fields_post_result["attempted"],
        custom_fields_post_result["failed"],
        "ok" if custom_fields_post_result["failed"] == 0 else "parcial",
    )

    try:
        attachment_result = _run_copy_with_retry(
            "adesao_reprovada_demissoes.clone_attachments",
            lambda: clone_attachments_dest_to_source(task_data, created_id),
        )
        logger.info(
            "adesao_reprovada_demissoes.anexos origem_task_id=%s destino_task_id=%s enviados=%d tentados=%d falhas=%d status=%s",
            task_id,
            created_id,
            attachment_result.get("sent", 0),
            attachment_result.get("attempted", 0),
            attachment_result.get("failed", 0),
            "ok" if attachment_result.get("ok") else "parcial",
        )
    except Exception:
        logger.exception(
            "adesao_reprovada_demissoes.erro_anexos origem_task_id=%s destino_task_id=%s",
            task_id,
            created_id,
        )

    try:
        comment_result = _run_copy_with_retry(
            "adesao_reprovada_demissoes.clone_comments",
            lambda: clone_comments_dest_to_source(task_id, created_id),
        )
        logger.info(
            "adesao_reprovada_demissoes.comentarios origem_task_id=%s destino_task_id=%s comentarios=%d respostas=%d status=%s",
            task_id,
            created_id,
            comment_result.get("comments", 0),
            comment_result.get("replies", 0),
            "ok" if comment_result.get("ok") else "parcial",
        )
    except Exception:
        logger.exception(
            "adesao_reprovada_demissoes.erro_comentarios origem_task_id=%s destino_task_id=%s",
            task_id,
            created_id,
        )

    logger.info(
        "adesao_reprovada_demissoes.ok origem_task_id=%s origem_link=%s destino_task_id=%s destino_link=%s status='%s' relacionamento=%s custom_fields_total=%d custom_fields_aplicados=%d",
        task_id,
        task_permalink(task_id),
        created_id,
        task_permalink(created_id),
        destination_status,
        "ok" if linked else "falhou",
        len(custom_fields_create_payload),
        custom_fields_post_result["applied"],
    )
    return created
