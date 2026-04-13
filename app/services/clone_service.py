"""Serviço de clonagem — orquestra busca, validação e criação do clone."""

import logging

from app.config.settings import CLONE_FIELD_MAP, SOURCE_LIST_MAP
from app.core.clickup_client import (
    build_custom_fields_payload,
    clone_comments,
    clone_attachments,
    create_task_in_dest,
    fetch_task,
)
from app.services.dedup import mark_cloned, release_reservation, try_reserve

logger = logging.getLogger(__name__)


def process_status_change(task_id: str, new_status: str) -> dict | None:
    """Processa uma mudança de status e clona se necessário.

    Returns:
        dict com dados da task criada, ou None se ignorada.
    """
    # 1. Reserva task (evita corrida durante rajadas)
    if not try_reserve(task_id):
        logger.debug(
            "Task %s ja clonada ou em processamento. Ignorando evento duplicado.",
            task_id,
        )
        return None

    cloned = False
    try:
        # 2. Busca dados completos da task
        logger.info(
            "Webhook recebido para task %s (status='%s'). Buscando dados...",
            task_id,
            new_status,
        )
        source_task = fetch_task(task_id)

        # 3. Verifica se a task pertence a uma das listas monitoradas
        list_info = source_task.get("list", {})
        source_list_id = list_info.get("id", "")
        trigger_status = SOURCE_LIST_MAP.get(source_list_id)

        if trigger_status is None:
            logger.debug(
                "Task %s pertence a lista %s, fora do SOURCE_LIST_MAP. Ignorando.",
                task_id,
                source_list_id,
            )
            return None

        # 4. Verifica se o novo status e o trigger desta lista
        if new_status.strip().lower() != trigger_status:
            logger.debug(
                "Status '%s' nao e trigger '%s' da lista %s. Ignorando task %s.",
                new_status,
                trigger_status,
                source_list_id,
                task_id,
            )
            return None

        # 5. Monta payload e cria clone
        name = source_task.get("name", "Sem nome")
        description = source_task.get("description", "")
        custom_fields = build_custom_fields_payload(source_task)

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

        # 5b. Clona anexos (task + custom fields de arquivo)
        try:
            clone_attachments(source_task, created.get("id"))
        except Exception as e:
            logger.exception("Falha ao clonar anexos da task %s: %s", task_id, e)

        # 5c. Clona comentarios (inclui replies)
        try:
            clone_comments(task_id, created.get("id"))
        except Exception as e:
            logger.exception("Falha ao clonar comentarios da task %s: %s", task_id, e)

        # 6. Marca como clonada
        mark_cloned(task_id)
        cloned = True

        logger.info(
            "Clone concluido: '%s' (lista %s) -> destino id=%s",
            name,
            source_list_id,
            created.get("id"),
        )

        return created
    finally:
        if not cloned:
            release_reservation(task_id)
