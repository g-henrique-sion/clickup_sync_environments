"""Dispatcher for status-change automations."""

from __future__ import annotations

import logging

from app.automations import (
    adesao_reprovada_demissoes,
    ativo_inicio_operacao,
    auditoria_routing,
    environment_sync,
    inadimplentes_finalizacao,
    onboarding_notify,
    relationship_bilateral,
    relationship_unilateral_black,
    task_name_on_create,
)
from app.automations.common import (
    StatusConvergencePendingError,
    TaskNotFoundError,
    TaskNotReadyError,
    build_status_change_context,
    normalize_status,
)
from app.config.settings import (
    AUDITORIA_ROUTING_SOURCE_LIST_IDS,
    ONBOARDING_BLACK_SYNC_LIST_ID,
    ONBOARDING_SYNC_LIST_ID,
    ONGOING_SYNC_LIST_ID,
    PLANEJAMENTO_BLACK_SYNC_LIST_ID,
)

logger = logging.getLogger(__name__)


def process_clickup_event(
    task_id: str,
    event_type: str,
    new_status: str | None = None,
    old_status: str | None = None,
    status_changed_at_ms: int | None = None,
) -> dict | None:
    """Dispatches webhook event by type."""
    event = str(event_type or "").strip()
    if event == "taskCreated":
        logger.debug("process_clickup_event.task_created task_id=%s", task_id)
        try:
            onboarding_notify.run_task_created(task_id)
            name_update = task_name_on_create.run_task_created(task_id)
            created = adesao_reprovada_demissoes.run(task_id)
        except TaskNotFoundError as exc:
            logger.warning(
                "process_clickup_event.defer task_nao_consistente task_id=%s event=%s",
                task_id,
                event,
            )
            raise TaskNotReadyError(task_id, max_attempts=8) from exc
        return created or name_update

    if event != "taskStatusUpdated":
        logger.debug(
            "process_clickup_event.skip event_nao_suportado task_id=%s event=%s",
            task_id,
            event,
        )
        return None

    if not new_status:
        logger.warning(
            "process_clickup_event.skip status_vazio task_id=%s event=%s",
            task_id,
            event,
        )
        return None

    return process_status_change(
        task_id,
        new_status,
        old_status=old_status,
        status_changed_at_ms=status_changed_at_ms,
    )


def process_status_change(
    task_id: str,
    new_status: str,
    *,
    old_status: str | None = None,
    status_changed_at_ms: int | None = None,
) -> dict | None:
    """Processes a status change using the configured automation routes."""
    logger.debug("process_status_change.inicio task_id=%s status='%s'", task_id, new_status)
    try:
        context = build_status_change_context(task_id, new_status)
    except TaskNotFoundError as exc:
        logger.warning(
            "process_status_change.defer task_nao_consistente task_id=%s status_evento='%s'",
            task_id,
            new_status,
        )
        raise TaskNotReadyError(task_id, max_attempts=6) from exc

    logger.debug(
        "process_status_change.context task_id=%s task='%s' list_id=%s list_name='%s' status_atual='%s' status_evento='%s' status_evento_normalizado='%s'",
        context.task_id,
        context.task_name,
        context.source_list_id,
        context.source_list_name,
        context.current_status_raw,
        context.new_status,
        context.normalized_new_status,
    )

    if (
        context.current_status_normalized
        and context.current_status_normalized != context.normalized_new_status
    ):
        old_status_normalized = normalize_status(old_status)
        if (
            old_status_normalized
            and context.current_status_normalized == old_status_normalized
        ):
            logger.warning(
                "process_status_change.defer status_ainda_nao_convergiu task_id=%s old_status='%s' status_evento='%s' status_atual='%s'",
                context.task_id,
                old_status or "",
                context.normalized_new_status,
                context.current_status_normalized,
            )
            raise StatusConvergencePendingError(
                context.task_id,
                current_status=context.current_status_raw,
                old_status=old_status or "",
                new_status=context.new_status,
                max_attempts=5,
            )

        logger.debug(
            "process_status_change.skip stale_event task_id=%s status_evento='%s' status_atual='%s'",
            context.task_id,
            context.normalized_new_status,
            context.current_status_normalized,
        )
        return None

    onboarding_notify.run_status_change(context, old_status_override=old_status)
    ativo_inicio_operacao.run(
        context,
        old_status=old_status,
        status_changed_at_ms=status_changed_at_ms,
    )
    inadimplentes_result = inadimplentes_finalizacao.run(
        context,
        old_status=old_status,
    )
    if inadimplentes_result is not None:
        return inadimplentes_result

    if context.source_list_id in {
        PLANEJAMENTO_BLACK_SYNC_LIST_ID,
        ONBOARDING_BLACK_SYNC_LIST_ID,
    }:
        logger.debug(
            "process_status_change.rota sync_black task_id=%s list_id=%s",
            context.task_id,
            context.source_list_id,
        )
        return relationship_unilateral_black.run(context)

    if context.source_list_id in {ONGOING_SYNC_LIST_ID, ONBOARDING_SYNC_LIST_ID}:
        logger.debug(
            "process_status_change.rota sync_interno task_id=%s list_id=%s",
            context.task_id,
            context.source_list_id,
        )
        return relationship_bilateral.run(context)

    if context.source_list_id in set(AUDITORIA_ROUTING_SOURCE_LIST_IDS):
        logger.debug(
            "process_status_change.rota auditoria_routing task_id=%s list_id=%s",
            context.task_id,
            context.source_list_id,
        )
        routed = auditoria_routing.run(context)
        if routed is not None:
            return routed

    return environment_sync.run(context)
