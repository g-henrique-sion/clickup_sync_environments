"""Task name formatter based on custom fields."""

from __future__ import annotations

import logging

from app.config.settings import (
    TASK_NAME_FORMAT_LIST_IDS,
    TASK_NAME_FORMAT_RULES,
    TASK_NAME_RAZAO_FIELD_ID,
    TASK_NAME_TEMPLATE,
    TASK_NAME_UC_FIELD_ID,
)

logger = logging.getLogger(__name__)


def _normalize_field_type(value: str | None) -> str:
    field_type = str(value or "").strip().lower()
    if field_type == "dropdown":
        return "drop_down"
    return field_type


def _option_label(field: dict, value) -> str:
    field_type = _normalize_field_type(field.get("type"))
    if field_type not in {"drop_down", "labels"}:
        return ""

    options = (field.get("type_config") or {}).get("options") or []
    values = value if isinstance(value, list) else [value]
    labels: list[str] = []
    for raw_value in values:
        raw_text = str(raw_value).strip()
        if not raw_text:
            continue
        for option in options:
            label = str(option.get("name") or option.get("label") or "").strip()
            if not label:
                continue
            for token in (option.get("id"), option.get("orderindex"), option.get("value")):
                if token is not None and str(token).strip() == raw_text:
                    labels.append(label)
                    break
            else:
                continue
            break

    return ", ".join(labels).strip()


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
        option_label = _option_label(field, value)
        if option_label:
            return option_label
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
    configured_lists = {
        str(item).strip() for item in TASK_NAME_FORMAT_LIST_IDS if str(item).strip()
    }
    return current in configured_lists or current in TASK_NAME_FORMAT_RULES


def should_format_task_name_on_create(list_id: str) -> bool:
    current = str(list_id or "").strip()
    return bool(current and current in TASK_NAME_FORMAT_RULES)


def _render_from_rule(
    task_data: dict,
    fallback_name: str,
    list_id: str,
    rule: dict[str, str],
    *,
    allow_fallback: bool,
) -> str:
    field_a = _custom_field_value(task_data, rule.get("field_a_id", ""))
    field_b = _custom_field_value(task_data, rule.get("field_b_id", ""))

    if not field_a or not field_b:
        return (str(fallback_name or "").strip() or "Sem nome") if allow_fallback else ""

    template = str(rule.get("template") or "{field_a} - {field_b}").strip()
    try:
        rendered = template.format(
            field_a=field_a,
            field_b=field_b,
            left=field_a,
            right=field_b,
        )
    except Exception:
        logger.warning(
            "TASK_NAME_FORMAT_RULES[%s] possui template invalido. Usando fallback padrao field_a/field_b.",
            list_id,
        )
        rendered = f"{field_a} - {field_b}"

    rendered = str(rendered or "").strip()
    return rendered or (str(fallback_name or "").strip() or "Sem nome")


def build_formatted_task_name(
    task_data: dict,
    fallback_name: str,
    list_id: str | None = None,
    *,
    allow_fallback: bool = True,
) -> str:
    current_list_id = str(list_id or "").strip()
    if current_list_id:
        rule = TASK_NAME_FORMAT_RULES.get(current_list_id)
        if rule:
            return _render_from_rule(
                task_data,
                fallback_name,
                current_list_id,
                rule,
                allow_fallback=allow_fallback,
            )

    razao = _custom_field_value(task_data, TASK_NAME_RAZAO_FIELD_ID)
    uc = _custom_field_value(task_data, TASK_NAME_UC_FIELD_ID)

    if not razao or not uc:
        return (str(fallback_name or "").strip() or "Sem nome") if allow_fallback else ""

    try:
        rendered = TASK_NAME_TEMPLATE.format(razao=razao, uc=uc)
    except Exception:
        logger.warning(
            "TASK_NAME_TEMPLATE invalido. Usando fallback padrao razao/uc."
        )
        rendered = f"{razao} - UC {uc}"

    rendered = str(rendered or "").strip()
    return rendered or (str(fallback_name or "").strip() or "Sem nome")
