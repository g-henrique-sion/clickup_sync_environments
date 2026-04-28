"""Serviço de clonagem — orquestra busca, validação e criação do clone."""

import logging
import threading
import time
import unicodedata

from app.config.settings import (
    BLACK_SYNC_ALLOWED_STATUSES,
    CLONE_FIELD_MAP,
    DEST_LIST_ID,
    DEST_RETURN_TRIGGER_STATUS,
    DEST_SYNC_ALLOWED_STATUSES,
    ONBOARDING_BLACK_SYNC_LIST_ID,
    ONBOARDING_SYNC_LIST_ID,
    ONGOING_SYNC_LIST_ID,
    PLANEJAMENTO_BLACK_SYNC_LIST_ID,
    SOURCE_RETURN_LIST_ID,
    SOURCE_RETURN_TRIGGER_STATUS,
    SOURCE_LIST_MAP,
)
from app.core.clickup_client import (
    build_reverse_custom_fields_payload,
    build_custom_fields_payload,
    clone_comments,
    clone_comments_dest_to_source,
    clone_attachments,
    clone_attachments_dest_to_source,
    create_task_in_source_list,
    create_task_in_dest,
    delete_task_in_dest,
    fetch_task,
    fetch_task_from_dest,
    update_task_status_any,
)

logger = logging.getLogger(__name__)

_PEER_CACHE_TTL_SECONDS = 600.0
_peer_cache_lock = threading.Lock()
_peer_cache: dict[tuple[str, str], tuple[str, float]] = {}


def _normalize_status(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value).strip().lower())
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _get_dest_sync_allowed_statuses() -> set[str]:
    return {
        _normalize_status(status)
        for status in DEST_SYNC_ALLOWED_STATUSES
        if str(status).strip()
    }


def _get_black_sync_allowed_statuses() -> set[str]:
    return {
        _normalize_status(status)
        for status in BLACK_SYNC_ALLOWED_STATUSES
        if str(status).strip()
    }


def _task_status_value(task_data: dict) -> str:
    raw_status = task_data.get("status")
    if isinstance(raw_status, dict):
        return str(raw_status.get("status") or raw_status.get("name") or "")
    if isinstance(raw_status, str):
        return raw_status
    return ""


def _task_name(task_data: dict) -> str:
    return str(task_data.get("name") or "Sem nome")


def _task_list_id(task_data: dict) -> str:
    return str((task_data.get("list") or {}).get("id") or "").strip()


def _task_list_name(task_data: dict) -> str:
    return str((task_data.get("list") or {}).get("name") or "").strip()


def _extract_related_task_ids(task_data: dict) -> list[str]:
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


def _resolve_peer_sync_list_id(current_list_id: str) -> str | None:
    if current_list_id == ONGOING_SYNC_LIST_ID:
        return ONBOARDING_SYNC_LIST_ID
    if current_list_id == ONBOARDING_SYNC_LIST_ID:
        return ONGOING_SYNC_LIST_ID
    return None


def _cache_peer(route: str, left_task_id: str, right_task_id: str) -> None:
    if not left_task_id or not right_task_id:
        return
    expires_at = time.time() + _PEER_CACHE_TTL_SECONDS
    with _peer_cache_lock:
        _peer_cache[(route, left_task_id)] = (right_task_id, expires_at)
        _peer_cache[(route, right_task_id)] = (left_task_id, expires_at)


def _invalidate_cached_peer(route: str, task_id: str) -> None:
    if not task_id:
        return
    with _peer_cache_lock:
        _peer_cache.pop((route, task_id), None)


def _get_cached_peer(route: str, task_id: str, related_task_ids: list[str]) -> str | None:
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


def _fetch_task_any(task_id: str) -> dict:
    source_error: Exception | None = None
    try:
        task = fetch_task(task_id)
        logger.debug("fetch_task_any: task_id=%s carregada com token source.", task_id)
        return task
    except Exception as e:
        source_error = e
        logger.debug(
            "fetch_task_any: falha com token source para task_id=%s: %s",
            task_id,
            e,
        )

    try:
        task = fetch_task_from_dest(task_id)
        logger.debug("fetch_task_any: task_id=%s carregada com token destino.", task_id)
        return task
    except Exception:
        logger.exception(
            "fetch_task_any: falha nos dois tokens para task_id=%s. erro_source=%s",
            task_id,
            source_error,
        )
        raise


def _fetch_task_preferring_dest(task_id: str) -> dict:
    """Busca task priorizando token de destino (rotas de sync interno/black)."""
    dest_error: Exception | None = None
    try:
        task = fetch_task_from_dest(task_id)
        logger.debug(
            "fetch_task_preferring_dest: task_id=%s carregada com token destino.",
            task_id,
        )
        return task
    except Exception as e:
        dest_error = e
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
    except Exception:
        logger.exception(
            "fetch_task_preferring_dest: falha nos dois tokens para task_id=%s. erro_dest=%s",
            task_id,
            dest_error,
        )
        raise


def _sync_dest_internal_status(task_data: dict, new_status: str) -> dict | None:
    task_id = str(task_data.get("id") or "").strip()
    current_list_id = _task_list_id(task_data)
    peer_list_id = _resolve_peer_sync_list_id(current_list_id)
    if not peer_list_id:
        return None

    normalized_new_status = _normalize_status(new_status)
    allowed_statuses = _get_dest_sync_allowed_statuses()
    if normalized_new_status not in allowed_statuses:
        logger.debug(
            "sync_interno.skip status_nao_permitido task_id=%s status='%s'",
            task_id,
            new_status,
        )
        return None

    related_task_ids = _extract_related_task_ids(task_data)
    if not related_task_ids:
        logger.debug(
            "sync_interno.skip sem_relacionamento task_id=%s list_id=%s",
            task_id,
            current_list_id,
        )
        return None

    route = "dest_internal"
    cached_peer_task_id = _get_cached_peer(route, task_id, related_task_ids)
    if cached_peer_task_id:
        try:
            updated = update_task_status_any(cached_peer_task_id, new_status)
            ongoing_task_id = (
                task_id if current_list_id == ONGOING_SYNC_LIST_ID else cached_peer_task_id
            )
            onboarding_task_id = (
                task_id if current_list_id == ONBOARDING_SYNC_LIST_ID else cached_peer_task_id
            )
            logger.info(
                "sync_interno: ongoing_task_id=%s onboarding_task_id=%s foi de '%s' -> '%s'",
                ongoing_task_id,
                onboarding_task_id,
                "cache",
                new_status,
            )
            return updated
        except Exception:
            logger.debug(
                "sync_interno.cache_invalido task_id=%s peer_task_id=%s; recalculando relacionamento.",
                task_id,
                cached_peer_task_id,
            )
            _invalidate_cached_peer(route, task_id)

    peer_task: dict | None = None
    for related_task_id in related_task_ids:
        try:
            candidate = _fetch_task_preferring_dest(related_task_id)
        except Exception:
            logger.debug(
                "sync_interno.relacionada_erro task_id=%s relacionada=%s erro=fetch_failed",
                task_id,
                related_task_id,
            )
            continue

        candidate_list_id = _task_list_id(candidate)
        if candidate_list_id == peer_list_id:
            peer_task = candidate
            break

    if not peer_task:
        logger.debug(
            "sync_interno.skip sem_task_par task_id=%s peer_list_id=%s relacionados=%s",
            task_id,
            peer_list_id,
            related_task_ids,
        )
        return None

    peer_task_id = str(peer_task.get("id") or "").strip()
    if not peer_task_id:
        logger.debug(
            "sync_interno.skip task_par_sem_id task_id=%s peer_list_id=%s",
            task_id,
            peer_list_id,
        )
        return None

    _cache_peer(route, task_id, peer_task_id)

    peer_task_status_raw = _task_status_value(peer_task)
    peer_status = _normalize_status(_task_status_value(peer_task))
    if peer_status == normalized_new_status:
        logger.debug(
            "sync_interno.skip ja_sincronizada task_id=%s peer_task_id=%s status='%s'",
            task_id,
            peer_task_id,
            peer_task_status_raw,
        )
        return None

    try:
        updated = update_task_status_any(peer_task_id, new_status)
    except Exception:
        logger.exception(
            "sync_interno.erro_update task_id=%s peer_task_id=%s status_destino='%s'",
            task_id,
            peer_task_id,
            new_status,
        )
        raise

    ongoing_task_id = task_id if current_list_id == ONGOING_SYNC_LIST_ID else peer_task_id
    onboarding_task_id = (
        task_id if current_list_id == ONBOARDING_SYNC_LIST_ID else peer_task_id
    )
    logger.info(
        "sync_interno: ongoing_task_id=%s onboarding_task_id=%s foi de '%s' -> '%s'",
        ongoing_task_id,
        onboarding_task_id,
        peer_task_status_raw,
        new_status,
    )
    return updated


def _sync_black_unilateral_status(task_data: dict, new_status: str) -> dict | None:
    task_id = str(task_data.get("id") or "").strip()
    source_list_id = _task_list_id(task_data)
    if source_list_id != PLANEJAMENTO_BLACK_SYNC_LIST_ID:
        return None
    if not ONBOARDING_BLACK_SYNC_LIST_ID:
        logger.debug(
            "sync_black.skip onboarding_black_nao_configurada task_id=%s",
            task_id,
        )
        return None

    normalized_new_status = _normalize_status(new_status)
    allowed_statuses = _get_black_sync_allowed_statuses()
    if normalized_new_status not in allowed_statuses:
        logger.debug(
            "sync_black.skip status_nao_permitido task_id=%s status='%s'",
            task_id,
            new_status,
        )
        return None

    related_task_ids = _extract_related_task_ids(task_data)
    if not related_task_ids:
        logger.debug("sync_black.skip sem_relacionamento task_id=%s", task_id)
        return None

    route = "black_unilateral"
    cached_peer_task_id = _get_cached_peer(route, task_id, related_task_ids)
    if cached_peer_task_id:
        try:
            updated = update_task_status_any(cached_peer_task_id, new_status)
            logger.info(
                "sync_black: planejamento_task_id=%s onboarding_task_id=%s foi de '%s' -> '%s'",
                task_id,
                cached_peer_task_id,
                "cache",
                new_status,
            )
            return updated
        except Exception:
            logger.debug(
                "sync_black.cache_invalido task_id=%s peer_task_id=%s; recalculando relacionamento.",
                task_id,
                cached_peer_task_id,
            )
            _invalidate_cached_peer(route, task_id)

    peer_task: dict | None = None
    for related_task_id in related_task_ids:
        try:
            candidate = _fetch_task_preferring_dest(related_task_id)
        except Exception:
            logger.debug(
                "sync_black.relacionada_erro task_id=%s relacionada=%s erro=fetch_failed",
                task_id,
                related_task_id,
            )
            continue
        if _task_list_id(candidate) == ONBOARDING_BLACK_SYNC_LIST_ID:
            peer_task = candidate
            break

    if not peer_task:
        logger.debug(
            "sync_black.skip sem_task_par task_id=%s onboarding_black_list_id=%s relacionados=%s",
            task_id,
            ONBOARDING_BLACK_SYNC_LIST_ID,
            related_task_ids,
        )
        return None

    peer_task_id = str(peer_task.get("id") or "").strip()
    if not peer_task_id:
        logger.debug("sync_black.skip task_par_sem_id task_id=%s", task_id)
        return None

    _cache_peer(route, task_id, peer_task_id)

    peer_task_status_raw = _task_status_value(peer_task)
    peer_status = _normalize_status(peer_task_status_raw)
    if peer_status == normalized_new_status:
        logger.debug(
            "sync_black.skip ja_sincronizada task_id=%s peer_task_id=%s status='%s'",
            task_id,
            peer_task_id,
            peer_task_status_raw,
        )
        return None

    try:
        updated = update_task_status_any(peer_task_id, new_status)
    except Exception:
        logger.exception(
            "sync_black.erro_update task_id=%s peer_task_id=%s status_destino='%s'",
            task_id,
            peer_task_id,
            new_status,
        )
        raise

    logger.info(
        "sync_black: planejamento_task_id=%s onboarding_task_id=%s foi de '%s' -> '%s'",
        task_id,
        peer_task_id,
        peer_task_status_raw,
        new_status,
    )
    return updated


def process_status_change(task_id: str, new_status: str) -> dict | None:
    """Processa uma mudança de status e clona se necessário.

    Returns:
        dict com dados da task criada, ou None se ignorada.
    """
    # 1. Busca dados completos da task
    logger.debug("process_status_change.inicio task_id=%s status='%s'", task_id, new_status)
    task_data = _fetch_task_any(task_id)

    list_info = task_data.get("list", {})
    source_list_id = list_info.get("id", "")
    source_list_name = list_info.get("name", "")
    trigger_status = SOURCE_LIST_MAP.get(source_list_id)
    normalized_new_status = _normalize_status(new_status)
    logger.debug(
        "process_status_change.context task_id=%s task='%s' list_id=%s list_name='%s' status_atual='%s' status_evento='%s' status_evento_normalizado='%s'",
        task_id,
        _task_name(task_data),
        source_list_id,
        source_list_name,
        _task_status_value(task_data),
        new_status,
        normalized_new_status,
    )
    current_status_normalized = _normalize_status(_task_status_value(task_data))
    if current_status_normalized and current_status_normalized != normalized_new_status:
        logger.debug(
            "process_status_change.skip stale_event task_id=%s status_evento='%s' status_atual='%s'",
            task_id,
            normalized_new_status,
            current_status_normalized,
        )
        return None

    # 1a. Sync unilateral Planejamento Black -> Onboarding Black
    if source_list_id == PLANEJAMENTO_BLACK_SYNC_LIST_ID:
        logger.debug(
            "process_status_change.rota sync_black task_id=%s list_id=%s",
            task_id,
            source_list_id,
        )
        return _sync_black_unilateral_status(task_data, new_status)

    # 1b. Sync interno entre listas destino baseado em relacionamento
    if source_list_id in {ONGOING_SYNC_LIST_ID, ONBOARDING_SYNC_LIST_ID}:
        logger.debug(
            "process_status_change.rota sync_interno task_id=%s list_id=%s",
            task_id,
            source_list_id,
        )
        return _sync_dest_internal_status(task_data, new_status)

    # 2. Fluxo de retorno: destino -> source quando status = pendencias
    if (
        source_list_id == DEST_LIST_ID
        and normalized_new_status == _normalize_status(DEST_RETURN_TRIGGER_STATUS)
    ):
        return_name = task_data.get("name", "Sem nome")
        return_description = task_data.get("description", "")
        return_custom_fields = build_reverse_custom_fields_payload(task_data)
        created_return = create_task_in_source_list(
            list_id=SOURCE_RETURN_LIST_ID,
            name=return_name,
            description=return_description,
            custom_fields=return_custom_fields or None,
        )
        clone_attachments_dest_to_source(task_data, created_return.get("id"))
        clone_comments_dest_to_source(task_id, created_return.get("id"))
        delete_task_in_dest(task_id)

        logger.info(
            "Retorno concluido: task destino %s -> lista source %s (id=%s)",
            task_id,
            SOURCE_RETURN_LIST_ID,
            created_return.get("id"),
        )
        return created_return

    is_source_return_to_dest = (
        source_list_id == SOURCE_RETURN_LIST_ID
        and normalized_new_status == _normalize_status(SOURCE_RETURN_TRIGGER_STATUS)
    )

    # 3. Fluxo source principal -> destino
    if not is_source_return_to_dest:
        if trigger_status is None:
            logger.debug(
                "process_status_change.skip lista_fora_source_map task_id=%s list_id=%s list_name='%s'",
                task_id,
                source_list_id,
                source_list_name,
            )
            return None

        # 4. Verifica se o novo status e o trigger desta lista
        if normalized_new_status != _normalize_status(trigger_status):
            logger.debug(
                "process_status_change.skip status_nao_trigger task_id=%s list_id=%s status_evento='%s' status_normalizado='%s' trigger='%s'",
                task_id,
                source_list_id,
                new_status,
                normalized_new_status,
                _normalize_status(trigger_status),
            )
            return None

    # 5. Monta payload e cria clone
    name = task_data.get("name", "Sem nome")
    description = task_data.get("description", "")
    custom_fields = build_custom_fields_payload(task_data)

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

    # 5b. Clona anexos (somente custom fields configurados)
    try:
        clone_attachments(task_data, created.get("id"))
    except Exception as e:
        logger.exception("Falha ao clonar anexos da task %s: %s", task_id, e)

    # 5c. Clona comentarios (inclui replies)
    try:
        clone_comments(task_id, created.get("id"))
    except Exception as e:
        logger.exception("Falha ao clonar comentarios da task %s: %s", task_id, e)

    if is_source_return_to_dest:
        logger.info(
            "Retorno concluido: task source %s (lista %s) -> destino id=%s (origem preservada)",
            task_id,
            source_list_id,
            created.get("id"),
        )
    else:
        logger.info(
            "Clone concluido: '%s' (lista %s) -> destino id=%s",
            name,
            source_list_id,
            created.get("id"),
        )

    return created
