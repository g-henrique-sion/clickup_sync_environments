"""Task name formatter based on custom fields."""

from __future__ import annotations

import logging

from app.config.settings import (
    TASK_NAME_FORMAT_LIST_IDS,
    TASK_NAME_RAZAO_FIELD_ID,
    TASK_NAME_TEMPLATE,
    TASK_NAME_UC_FIELD_ID,
)

logger = logging.getLogger(__name__)


def _custom_field_value(task_data: dict, field_id: str) -> str:
    target = str(field_id or "").strip()
    if not target:
        return ""

    for field in task_data.get("custom_fields", []) or []:
        if str(field.get("id") or "").strip() != target:
            continue
        value = field.get("value")
        if value is None:
            return ""
        if isinstance(value, (str, int, float, bool)):
            return str(value).strip()
        if isinstance(value, dict):
            for key in ("name", "label", "value", "text"):
                if key in value and value.get(key) is not None:
                    return str(value.get(key)).strip()
            return str(value).strip()
        if isinstance(value, list):
            parts = [str(item).strip() for item in value if str(item).strip()]
            return ", ".join(parts).strip()
        return str(value).strip()
    return ""


def should_format_task_name(list_id: str) -> bool:
    current = str(list_id or "").strip()
    if not current:
        return False
    return current in {str(item).strip() for item in TASK_NAME_FORMAT_LIST_IDS if str(item).strip()}


def build_formatted_task_name(task_data: dict, fallback_name: str) -> str:
    razao = _custom_field_value(task_data, TASK_NAME_RAZAO_FIELD_ID)
    uc = _custom_field_value(task_data, TASK_NAME_UC_FIELD_ID)

    if not razao or not uc:
        return str(fallback_name or "").strip() or "Sem nome"

    try:
        rendered = TASK_NAME_TEMPLATE.format(razao=razao, uc=uc)
    except Exception:
        logger.warning(
            "TASK_NAME_TEMPLATE invalido. Usando fallback padrao razao/uc."
        )
        rendered = f"{razao} - UC {uc}"

    rendered = str(rendered or "").strip()
    return rendered or (str(fallback_name or "").strip() or "Sem nome")

