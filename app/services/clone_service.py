"""Serviço de clonagem — orquestra busca, validação e criação do clone."""

import logging

from app.config.settings import CLONE_FIELD_MAP, SOURCE_LIST_ID, TRIGGER_STATUS
from app.core.clickup_client import (
    build_custom_fields_payload,
    clone_comments,
    clone_attachments,
    create_task_in_dest,
    fetch_task,
)
from app.services.dedup import mark_cloned, was_cloned

logger = logging.getLogger(__name__)


def process_status_change(task_id: str, new_status: str) -> dict | None:
    """Processa uma mudança de status e clona se necessário.

    Returns:
        dict com dados da task criada, ou None se ignorada.
    """
    # 1. Verifica se o status é o trigger
    if new_status.strip().lower() != TRIGGER_STATUS:
        logger.debug(
            "Status '%s' não é o trigger '%s'. Ignorando task %s.",
            new_status, TRIGGER_STATUS, task_id,
        )
        return None

    # 2. Verifica dedup
    if was_cloned(task_id):
        logger.info("Task %s já foi clonada anteriormente. Ignorando.", task_id)
        return None

    # 3. Busca dados completos da task
    logger.info("Status trigger detectado! Buscando task %s...", task_id)
    source_task = fetch_task(task_id)

    # 4. Valida que a task pertence à lista monitorada
    list_info = source_task.get("list", {})
    if list_info.get("id") != SOURCE_LIST_ID:
        logger.debug(
            "Task %s pertence à lista %s, não à monitorada %s. Ignorando.",
            task_id, list_info.get("id"), SOURCE_LIST_ID,
        )
        return None

    # 5. Monta payload e cria clone
    name = source_task.get("name", "Sem nome")
    description = source_task.get("description", "")
    custom_fields = build_custom_fields_payload(source_task)

    logger.info(
        "Clonando task '%s' (%s) com %d custom fields (map=%d)...",
        name, task_id, len(custom_fields), len(CLONE_FIELD_MAP),
    )

    created = create_task_in_dest(
        name=name,
        description=description,
        custom_fields=custom_fields or None,
    )

    # 5b. Clona anexos (task + custom fields de arquivo)
    try:
        clone_attachments(source_task, created.get("id"))
    except Exception as e:
        logger.exception("Falha ao clonar anexos da task %s: %s", task_id, e)

    # 5c. Clona comentários (inclui replies)
    try:
        clone_comments(task_id, created.get("id"))
    except Exception as e:
        logger.exception("Falha ao clonar comentários da task %s: %s", task_id, e)

    # 6. Marca como clonada
    mark_cloned(task_id)

    logger.info(
        "Clone concluído: '%s' -> destino id=%s",
        name, created.get("id"),
    )

    return created
