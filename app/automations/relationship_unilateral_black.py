"""Status sync between Planejamento Black and Onboarding Black.

Supports direct status sync for shared statuses and mapped sync for
non-identical statuses between the two lists.
"""

from __future__ import annotations

import logging

from app.automations.common import (
    StatusChangeContext,
    extract_related_task_ids,
    fetch_task_preferring_dest,
    normalize_status,
    task_list_id,
    task_status_value,
)
from app.automations.peer_cache import cache_peer, get_cached_peer, invalidate_cached_peer
from app.config.settings import (
    BLACK_SYNC_ALLOWED_STATUSES,
    BLACK_SYNC_STATUS_MAP,
    ONBOARDING_BLACK_SYNC_LIST_ID,
    PLANEJAMENTO_BLACK_SYNC_LIST_ID,
)
from app.core.clickup_client import (
    resolve_list_status_name_any,
    task_permalink,
    update_task_status_any,
)

logger = logging.getLogger(__name__)


def _allowed_statuses() -> set[str]:
    allowed = {
        normalize_status(status)
        for status in BLACK_SYNC_ALLOWED_STATUSES
        if str(status).strip()
    }
    allowed.update(_planejamento_to_onboarding_map().keys())
    allowed.update(_onboarding_to_planejamento_map().keys())
    return allowed


def _planejamento_to_onboarding_map() -> dict[str, str]:
    mapped: dict[str, str] = {}
    for planejamento_status, onboarding_status in BLACK_SYNC_STATUS_MAP.items():
        left = str(planejamento_status or "").strip()
        right = str(onboarding_status or "").strip()
        if not left or not right:
            continue
        mapped[normalize_status(left)] = right
    return mapped


def _onboarding_to_planejamento_map() -> dict[str, str]:
    mapped: dict[str, str] = {}
    for planejamento_status, onboarding_status in BLACK_SYNC_STATUS_MAP.items():
        left = str(planejamento_status or "").strip()
        right = str(onboarding_status or "").strip()
        if not left or not right:
            continue
        mapped[normalize_status(right)] = left
    return mapped


def _resolve_peer_sync_list_id(current_list_id: str) -> str | None:
    if current_list_id == PLANEJAMENTO_BLACK_SYNC_LIST_ID:
        return ONBOARDING_BLACK_SYNC_LIST_ID
    if current_list_id == ONBOARDING_BLACK_SYNC_LIST_ID:
        return PLANEJAMENTO_BLACK_SYNC_LIST_ID
    return None


def _target_status_for_peer(current_list_id: str, source_status: str) -> str | None:
    normalized_source_status = normalize_status(source_status)

    if current_list_id == PLANEJAMENTO_BLACK_SYNC_LIST_ID:
        mapped = _planejamento_to_onboarding_map().get(normalized_source_status)
        if mapped:
            return mapped
    elif current_list_id == ONBOARDING_BLACK_SYNC_LIST_ID:
        mapped = _onboarding_to_planejamento_map().get(normalized_source_status)
        if mapped:
            return mapped

    if normalized_source_status in {
        normalize_status(status)
        for status in BLACK_SYNC_ALLOWED_STATUSES
        if str(status).strip()
    }:
        return source_status
    return None


def _source_status_changed_during_processing(
    task_id: str,
    expected_status_normalized: str,
) -> bool:
    """Revalida status da task origem antes do update para evitar overwrite antigo."""
    try:
        latest_task = fetch_task_preferring_dest(task_id)
    except Exception:
        logger.debug(
            "sync_black.revalidacao_status_falhou task_id=%s; seguindo fluxo.",
            task_id,
        )
        return False

    latest_status_raw = task_status_value(latest_task)
    latest_status_normalized = normalize_status(latest_status_raw)
    if latest_status_normalized == expected_status_normalized:
        return False

    logger.info(
        "sync_black.skip stale_runtime task_id=%s status_evento='%s' status_atual='%s'",
        task_id,
        expected_status_normalized,
        latest_status_normalized,
    )
    return True


def run(context: StatusChangeContext) -> dict | None:
    task_id = context.task_id
    current_list_id = context.source_list_id

    peer_list_id = _resolve_peer_sync_list_id(current_list_id)
    if not peer_list_id:
        return None

    if not ONBOARDING_BLACK_SYNC_LIST_ID or not PLANEJAMENTO_BLACK_SYNC_LIST_ID:
        logger.debug(
            "sync_black.skip listas_black_nao_configuradas task_id=%s",
            task_id,
        )
        return None

    target_status = _target_status_for_peer(current_list_id, context.new_status)
    if not target_status or context.normalized_new_status not in _allowed_statuses():
        logger.debug(
            "sync_black.skip status_nao_permitido task_id=%s status='%s'",
            task_id,
            context.new_status,
        )
        return None
    target_status_resolved = (
        resolve_list_status_name_any(peer_list_id, target_status) or target_status
    )
    target_status_normalized = normalize_status(target_status_resolved)

    related_task_ids = extract_related_task_ids(context.task_data)
    if not related_task_ids:
        logger.debug(
            "sync_black.skip sem_relacionamento task_id=%s list_id=%s",
            task_id,
            current_list_id,
        )
        return None

    route = "black_internal"
    cached_peer_task_id = get_cached_peer(route, task_id, related_task_ids)
    if cached_peer_task_id:
        if _source_status_changed_during_processing(task_id, context.normalized_new_status):
            return None
        try:
            updated = update_task_status_any(cached_peer_task_id, target_status_resolved)
            logger.info(
                "sync_black: origem_task_id=%s destino_task_id=%s foi de '%s' -> '%s' origem_link=%s destino_link=%s",
                task_id,
                cached_peer_task_id,
                "cache",
                target_status_resolved,
                task_permalink(task_id),
                task_permalink(cached_peer_task_id),
            )
            return updated
        except Exception:
            logger.debug(
                "sync_black.cache_invalido task_id=%s peer_task_id=%s; recalculando relacionamento.",
                task_id,
                cached_peer_task_id,
            )
            invalidate_cached_peer(route, task_id)

    peer_task: dict | None = None
    for related_task_id in related_task_ids:
        try:
            candidate = fetch_task_preferring_dest(related_task_id)
        except Exception:
            logger.debug(
                "sync_black.relacionada_erro task_id=%s relacionada=%s erro=fetch_failed",
                task_id,
                related_task_id,
            )
            continue

        if task_list_id(candidate) == peer_list_id:
            peer_task = candidate
            break

    if not peer_task:
        logger.debug(
            "sync_black.skip sem_task_par task_id=%s peer_list_id=%s relacionados=%s",
            task_id,
            peer_list_id,
            related_task_ids,
        )
        return None

    peer_task_id = str(peer_task.get("id") or "").strip()
    if not peer_task_id:
        logger.debug("sync_black.skip task_par_sem_id task_id=%s", task_id)
        return None

    cache_peer(route, task_id, peer_task_id)

    peer_task_status_raw = task_status_value(peer_task)
    peer_status = normalize_status(peer_task_status_raw)
    if peer_status == target_status_normalized:
        logger.debug(
            "sync_black.skip ja_sincronizada task_id=%s peer_task_id=%s status_origem='%s' status_destino='%s'",
            task_id,
            peer_task_id,
            context.new_status,
            target_status_resolved,
        )
        return None

    if _source_status_changed_during_processing(task_id, context.normalized_new_status):
        return None

    try:
        updated = update_task_status_any(peer_task_id, target_status_resolved)
    except Exception:
        logger.exception(
            "sync_black.erro_update task_id=%s peer_task_id=%s status_destino='%s'",
            task_id,
            peer_task_id,
            target_status_resolved,
        )
        raise

    logger.info(
        "sync_black: origem_task_id=%s destino_task_id=%s foi de '%s' -> '%s' origem_link=%s destino_link=%s",
        task_id,
        peer_task_id,
        peer_task_status_raw,
        target_status_resolved,
        task_permalink(task_id),
        task_permalink(peer_task_id),
    )
    return updated
