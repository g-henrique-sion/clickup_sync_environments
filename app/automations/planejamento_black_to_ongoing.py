"""Move Planejamento Black tasks to Ongoing when the final billing status is reached."""

from __future__ import annotations

import logging

from app.automations.common import (
    StatusChangeContext,
    normalize_status,
    task_status_value,
)
from app.config.settings import (
    PLANEJAMENTO_BLACK_SYNC_LIST_ID,
    PLANEJAMENTO_BLACK_TO_ONGOING_ENABLED,
    PLANEJAMENTO_BLACK_TO_ONGOING_TARGET_LIST_ID,
    PLANEJAMENTO_BLACK_TO_ONGOING_TARGET_STATUS,
    PLANEJAMENTO_BLACK_TO_ONGOING_TRIGGER_STATUS,
)
from app.core.clickup_client import (
    move_task_to_list_any,
    resolve_list_status_name_any,
    task_permalink,
    update_task_status_any,
)

logger = logging.getLogger(__name__)


def run(context: StatusChangeContext) -> dict | None:
    """Moves the current Planejamento Black task to Ongoing as Ativo."""
    if not PLANEJAMENTO_BLACK_TO_ONGOING_ENABLED:
        return None

    if context.source_list_id != PLANEJAMENTO_BLACK_SYNC_LIST_ID:
        return None

    trigger_status = normalize_status(PLANEJAMENTO_BLACK_TO_ONGOING_TRIGGER_STATUS)
    if context.normalized_new_status != trigger_status:
        logger.debug(
            "planejamento_black_to_ongoing.skip status_nao_trigger task_id=%s status='%s' trigger='%s'",
            context.task_id,
            context.new_status,
            PLANEJAMENTO_BLACK_TO_ONGOING_TRIGGER_STATUS,
        )
        return None

    target_list_id = PLANEJAMENTO_BLACK_TO_ONGOING_TARGET_LIST_ID
    if not target_list_id:
        logger.warning(
            "planejamento_black_to_ongoing.skip lista_destino_nao_configurada task_id=%s",
            context.task_id,
        )
        return None

    target_status = (
        resolve_list_status_name_any(
            target_list_id,
            PLANEJAMENTO_BLACK_TO_ONGOING_TARGET_STATUS,
        )
        or ""
    )
    if not target_status:
        logger.error(
            "planejamento_black_to_ongoing.skip status_destino_nao_encontrado task_id=%s destino_lista=%s status='%s'",
            context.task_id,
            target_list_id,
            PLANEJAMENTO_BLACK_TO_ONGOING_TARGET_STATUS,
        )
        return None

    logger.info(
        "planejamento_black_to_ongoing.inicio task_id=%s origem_lista=%s destino_lista=%s trigger_status='%s' destino_status='%s'",
        context.task_id,
        context.source_list_id,
        target_list_id,
        context.new_status,
        target_status,
    )
    moved = move_task_to_list_any(
        task_id=context.task_id,
        target_list_id=target_list_id,
        source_status_name=context.current_status_raw or context.new_status,
        destination_status_name=target_status,
    )

    moved_status = normalize_status(task_status_value(moved))
    target_status_normalized = normalize_status(target_status)
    if moved_status != target_status_normalized:
        logger.warning(
            "planejamento_black_to_ongoing.status_pos_move task_id=%s status_atual='%s' status_esperado='%s'; aplicando update_status",
            context.task_id,
            task_status_value(moved),
            target_status,
        )
        moved = update_task_status_any(context.task_id, target_status)

    logger.info(
        "planejamento_black_to_ongoing.ok task_id=%s origem_lista=%s destino_lista=%s destino_status='%s' link=%s",
        context.task_id,
        context.source_list_id,
        target_list_id,
        target_status,
        task_permalink(context.task_id),
    )
    return moved
