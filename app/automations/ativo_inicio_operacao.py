"""Preenche inicio de operacao ao entrar no status Ativo."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

from app.automations.common import StatusChangeContext, normalize_status
from app.config.settings import (
    ATIVO_INICIO_OPERACAO_ENABLED,
    ATIVO_INICIO_OPERACAO_FIELD_ID,
    ATIVO_INICIO_OPERACAO_LIST_IDS,
    ATIVO_INICIO_OPERACAO_TRIGGER_STATUS,
)
from app.core.clickup_client import set_task_custom_field_any

logger = logging.getLogger(__name__)


def _target_lists() -> set[str]:
    return {
        str(list_id).strip()
        for list_id in ATIVO_INICIO_OPERACAO_LIST_IDS
        if str(list_id).strip()
    }


def _is_enabled() -> bool:
    return (
        ATIVO_INICIO_OPERACAO_ENABLED
        and bool(_target_lists())
        and bool(str(ATIVO_INICIO_OPERACAO_FIELD_ID).strip())
    )


def _first_day_of_month_ms(status_changed_at_ms: int | None) -> int:
    if ZoneInfo:
        try:
            tz = ZoneInfo("America/Sao_Paulo")
        except Exception:
            tz = timezone(timedelta(hours=-3))
    else:
        tz = timezone(timedelta(hours=-3))
    if status_changed_at_ms and status_changed_at_ms > 0:
        base_dt = datetime.fromtimestamp(status_changed_at_ms / 1000, tz=tz)
    else:
        base_dt = datetime.now(tz=tz)
    first_day = base_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return int(first_day.timestamp() * 1000)


def _custom_field_value(task_data: dict, field_id: str):
    wanted = str(field_id or "").strip()
    if not wanted:
        return None
    for custom_field in task_data.get("custom_fields", []) or []:
        if str(custom_field.get("id") or "").strip() != wanted:
            continue
        return custom_field.get("value")
    return None


def run(
    context: StatusChangeContext,
    *,
    old_status: str | None = None,
    status_changed_at_ms: int | None = None,
) -> dict | None:
    if not _is_enabled():
        return None

    if str(context.source_list_id).strip() not in _target_lists():
        return None

    trigger = normalize_status(ATIVO_INICIO_OPERACAO_TRIGGER_STATUS)
    if context.normalized_new_status != trigger:
        return None

    if old_status is not None and normalize_status(old_status) == trigger:
        logger.debug(
            "ativo_inicio_operacao.skip sem_transicao task_id=%s status='%s'",
            context.task_id,
            context.new_status,
        )
        return None

    existing_value = _custom_field_value(context.task_data, ATIVO_INICIO_OPERACAO_FIELD_ID)
    if existing_value not in (None, "", [], {}):
        logger.info(
            "ativo_inicio_operacao.skip ja_preenchido task_id=%s valor=%s",
            context.task_id,
            existing_value,
        )
        return None

    value_ms = _first_day_of_month_ms(status_changed_at_ms)
    success = set_task_custom_field_any(
        task_id=context.task_id,
        field_id=ATIVO_INICIO_OPERACAO_FIELD_ID,
        value=value_ms,
    )
    if not success:
        logger.error(
            "ativo_inicio_operacao.erro task_id=%s field_id=%s valor_ms=%s",
            context.task_id,
            ATIVO_INICIO_OPERACAO_FIELD_ID,
            value_ms,
        )
        raise RuntimeError("Falha ao definir campo inicio de operacao.")

    logger.info(
        "ativo_inicio_operacao.ok task_id=%s list_id=%s status='%s' valor_ms=%s",
        context.task_id,
        context.source_list_id,
        context.new_status,
        value_ms,
    )
    return {"task_id": context.task_id, "field_id": ATIVO_INICIO_OPERACAO_FIELD_ID, "value": value_ms}
