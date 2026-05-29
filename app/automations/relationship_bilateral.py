"""Bilateral status sync between Ongoing and Onboarding lists."""

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
    DEST_SYNC_ALLOWED_STATUSES,
    ONBOARDING_SYNC_LIST_ID,
    ONGOING_SYNC_LIST_ID,
)
from app.core.clickup_client import task_permalink, update_task_status_any

logger = logging.getLogger(__name__)


def _allowed_statuses() -> set[str]:
    return {
        normalize_status(status)
        for status in DEST_SYNC_ALLOWED_STATUSES
        if str(status).strip()
    }


def _resolve_peer_sync_list_id(current_list_id: str) -> str | None:
    if current_list_id == ONGOING_SYNC_LIST_ID:
        return ONBOARDING_SYNC_LIST_ID
    if current_list_id == ONBOARDING_SYNC_LIST_ID:
        return ONGOING_SYNC_LIST_ID
    return None


def _source_status_changed_during_processing(
    task_id: str,
    expected_status_normalized: str,
) -> bool:
    """Revalida status da origem antes do update para evitar status antigo."""
    try:
        latest_task = fetch_task_preferring_dest(task_id)
    except Exception:
        logger.debug(
            "sync_interno.revalidacao_status_falhou task_id=%s; seguindo fluxo.",
            task_id,
        )
        return False

    latest_status_raw = task_status_value(latest_task)
    latest_status_normalized = normalize_status(latest_status_raw)
    if latest_status_normalized == expected_status_normalized:
        return False

    logger.info(
        "sync_interno.skip stale_runtime task_id=%s status_evento='%s' status_atual='%s'",
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

    if context.normalized_new_status not in _allowed_statuses():
        logger.debug(
            "sync_interno.skip status_nao_permitido task_id=%s status='%s'",
            task_id,
            context.new_status,
        )
        return None

    related_task_ids = extract_related_task_ids(context.task_data)
    if not related_task_ids:
        logger.debug(
            "sync_interno.skip sem_relacionamento task_id=%s list_id=%s",
            task_id,
            current_list_id,
        )
        return None

    route = "dest_internal"
    cached_peer_task_id = get_cached_peer(route, task_id, related_task_ids)
    if cached_peer_task_id:
        if _source_status_changed_during_processing(task_id, context.normalized_new_status):
            return None
        try:
            updated = update_task_status_any(cached_peer_task_id, context.new_status)
            ongoing_task_id = (
                task_id if current_list_id == ONGOING_SYNC_LIST_ID else cached_peer_task_id
            )
            onboarding_task_id = (
                task_id
                if current_list_id == ONBOARDING_SYNC_LIST_ID
                else cached_peer_task_id
            )
            logger.info(
                "sync_interno: ongoing_task_id=%s onboarding_task_id=%s foi de '%s' -> '%s' ongoing_link=%s onboarding_link=%s",
                ongoing_task_id,
                onboarding_task_id,
                "cache",
                context.new_status,
                task_permalink(ongoing_task_id),
                task_permalink(onboarding_task_id),
            )
            return updated
        except Exception:
            logger.debug(
                "sync_interno.cache_invalido task_id=%s peer_task_id=%s; recalculando relacionamento.",
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
                "sync_interno.relacionada_erro task_id=%s relacionada=%s erro=fetch_failed",
                task_id,
                related_task_id,
            )
            continue

        if task_list_id(candidate) == peer_list_id:
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

    cache_peer(route, task_id, peer_task_id)

    peer_task_status_raw = task_status_value(peer_task)
    peer_status = normalize_status(peer_task_status_raw)
    if peer_status == context.normalized_new_status:
        logger.debug(
            "sync_interno.skip ja_sincronizada task_id=%s peer_task_id=%s status='%s'",
            task_id,
            peer_task_id,
            peer_task_status_raw,
        )
        return None

    if _source_status_changed_during_processing(task_id, context.normalized_new_status):
        return None

    try:
        updated = update_task_status_any(peer_task_id, context.new_status)
    except Exception:
        logger.exception(
            "sync_interno.erro_update task_id=%s peer_task_id=%s status_destino='%s'",
            task_id,
            peer_task_id,
            context.new_status,
        )
        raise

    ongoing_task_id = task_id if current_list_id == ONGOING_SYNC_LIST_ID else peer_task_id
    onboarding_task_id = (
        task_id if current_list_id == ONBOARDING_SYNC_LIST_ID else peer_task_id
    )
    logger.info(
        "sync_interno: ongoing_task_id=%s onboarding_task_id=%s foi de '%s' -> '%s' ongoing_link=%s onboarding_link=%s",
        ongoing_task_id,
        onboarding_task_id,
        peer_task_status_raw,
        context.new_status,
        task_permalink(ongoing_task_id),
        task_permalink(onboarding_task_id),
    )
    return updated
