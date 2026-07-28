"""Configuracoes centrais carregadas de variaveis de ambiente."""

import json
import logging
import os
from typing import Any, TypeVar

from dotenv import load_dotenv

load_dotenv()

_Number = TypeVar("_Number", int, float)

# Tokens
SOURCE_CLICKUP_TOKEN: str = os.getenv("SOURCE_CLICKUP_TOKEN", "")
DEST_CLICKUP_TOKEN: str = os.getenv("DEST_CLICKUP_TOKEN", "")

# IDs
DEST_LIST_ID: str = os.getenv("DEST_LIST_ID", "")
DEST_WORKSPACE_ID: str = os.getenv("DEST_WORKSPACE_ID", "")


def _parse_json_mapping(raw_value: str, var_name: str) -> dict[str, str]:
    """Parseia um mapeamento JSON garantindo objeto chave/valor string."""
    try:
        parsed = json.loads(raw_value)
        if not isinstance(parsed, dict):
            raise ValueError(f"{var_name} deve ser um objeto JSON.")
        return {str(k).strip(): str(v).strip() for k, v in parsed.items()}
    except (json.JSONDecodeError, ValueError):
        logging.getLogger(__name__).warning(
            "%s invalido (JSON). Valor bruto ignorado.", var_name
        )
        return {}


def _parse_json_object(raw_value: str, var_name: str) -> dict[str, Any]:
    """Parseia um objeto JSON mantendo a estrutura interna."""
    try:
        parsed = json.loads(raw_value)
        if not isinstance(parsed, dict):
            raise ValueError(f"{var_name} deve ser um objeto JSON.")
        return parsed
    except (json.JSONDecodeError, ValueError):
        logging.getLogger(__name__).warning(
            "%s invalido (JSON). Valor bruto ignorado.", var_name
        )
        return {}


def _parse_csv_values(raw_value: str) -> list[str]:
    """Parseia lista CSV simples removendo vazios e espacos extras."""
    if not raw_value:
        return []
    return [part.strip() for part in raw_value.split(",") if part.strip()]


def _parse_bool(raw_value: str | None, default: bool = False) -> bool:
    if raw_value is None or str(raw_value).strip() == "":
        return default
    return str(raw_value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _parse_number(
    raw_value: str | None,
    default: _Number,
    caster: type[_Number],
    var_name: str,
) -> _Number:
    if raw_value is None or str(raw_value).strip() == "":
        return default
    try:
        return caster(raw_value)
    except (TypeError, ValueError):
        logging.getLogger(__name__).warning(
            "%s invalido. Usando padrao=%s.", var_name, default
        )
        return default


def _parse_int(raw_value: str | None, default: int, var_name: str) -> int:
    return int(_parse_number(raw_value, default, int, var_name))


def _parse_float(raw_value: str | None, default: float, var_name: str) -> float:
    return float(_parse_number(raw_value, default, float, var_name))


# Origem monitorada (automacao source -> destino)
SOURCE_LIST_ID: str = os.getenv("SOURCE_LIST_ID", "").strip()
SOURCE_TRIGGER_STATUS: str = os.getenv(
    "SOURCE_TRIGGER_STATUS", "cooperado aprovado"
).strip().lower()
SOURCE_LIST_MAP = (
    {SOURCE_LIST_ID: SOURCE_TRIGGER_STATUS}
    if SOURCE_LIST_ID and SOURCE_TRIGGER_STATUS
    else {}
)

# Retrabalho entre destino e source retorno
SOURCE_RETURN_LIST_ID: str = os.getenv("SOURCE_RETURN_LIST_ID", "").strip()
DEST_RETURN_TRIGGER_STATUS: str = os.getenv(
    "DEST_RETURN_TRIGGER_STATUS", "pend. comercial"
).strip().lower()
SOURCE_RETURN_TRIGGER_STATUS: str = os.getenv(
    "SOURCE_RETURN_TRIGGER_STATUS", "corrigido"
).strip().lower()

# Sync interno bilateral (ongoing <-> onboarding)
ONGOING_SYNC_LIST_ID: str = os.getenv(
    "ONGOING_SYNC_LIST_ID",
    os.getenv("DEST_SYNC_LIST_A_ID", ""),
).strip()
ONBOARDING_SYNC_LIST_ID: str = os.getenv(
    "ONBOARDING_SYNC_LIST_ID",
    os.getenv("DEST_SYNC_LIST_B_ID", ""),
).strip()

# Sync interno entre Planejamento Black <-> Onboarding Black
PLANEJAMENTO_BLACK_SYNC_LIST_ID: str = os.getenv(
    "PLANEJAMENTO_BLACK_SYNC_LIST_ID",
    "",
).strip()
ONBOARDING_BLACK_SYNC_LIST_ID: str = os.getenv(
    "ONBOARDING_BLACK_SYNC_LIST_ID",
    "",
).strip()
BLACK_SYNC_ALLOWED_STATUSES: list[str] = _parse_csv_values(
    os.getenv(
        "BLACK_SYNC_ALLOWED_STATUSES",
        "1a fatura sem inj,1a fatura com desconto",
    )
)
_BLACK_SYNC_STATUS_MAP_DEFAULT = {
    "Troca Solicitada": "Agendamento TT",
    "Titularidade Alterada": "Troca de TT",
    "Cadastrado na Usina": "Cadastro aprovado",
}
BLACK_SYNC_STATUS_MAP: dict[str, str] = _parse_json_mapping(
    os.getenv(
        "BLACK_SYNC_STATUS_MAP",
        json.dumps(_BLACK_SYNC_STATUS_MAP_DEFAULT, ensure_ascii=False),
    ),
    "BLACK_SYNC_STATUS_MAP",
)
if not BLACK_SYNC_STATUS_MAP:
    BLACK_SYNC_STATUS_MAP = dict(_BLACK_SYNC_STATUS_MAP_DEFAULT)

# Notificacao em comentarios para listas de onboarding
_onboarding_notify_default_lists = ",".join(
    [
        value
        for value in [ONBOARDING_SYNC_LIST_ID, ONBOARDING_BLACK_SYNC_LIST_ID]
        if str(value).strip()
    ]
)
ONBOARDING_NOTIFY_ENABLED: bool = _parse_bool(
    os.getenv("ONBOARDING_NOTIFY_ENABLED"),
    default=True,
)
ONBOARDING_NOTIFY_LIST_IDS: list[str] = _parse_csv_values(
    os.getenv("ONBOARDING_NOTIFY_LIST_IDS", _onboarding_notify_default_lists)
)
ONBOARDING_NOTIFY_USER_ID: str = os.getenv(
    "ONBOARDING_NOTIFY_USER_ID",
    "112035201",
).strip()
ONBOARDING_NOTIFY_USER_NAME: str = os.getenv(
    "ONBOARDING_NOTIFY_USER_NAME",
    "Christian Lopes de Moura",
).strip()
ONBOARDING_NOTIFY_USER_IDS: list[str] = _parse_csv_values(
    os.getenv("ONBOARDING_NOTIFY_USER_IDS", ONBOARDING_NOTIFY_USER_ID)
)
ONBOARDING_NOTIFY_USER_NAMES: list[str] = _parse_csv_values(
    os.getenv("ONBOARDING_NOTIFY_USER_NAMES", ONBOARDING_NOTIFY_USER_NAME)
)

# Preencher campo "inicio de operacao" ao entrar em Ativo
_ativo_inicio_operacao_default_lists = ",".join(
    [
        value
        for value in [ONGOING_SYNC_LIST_ID, ONBOARDING_SYNC_LIST_ID]
        if str(value).strip()
    ]
)
ATIVO_INICIO_OPERACAO_ENABLED: bool = _parse_bool(
    os.getenv("ATIVO_INICIO_OPERACAO_ENABLED"),
    default=True,
)
ATIVO_INICIO_OPERACAO_LIST_IDS: list[str] = _parse_csv_values(
    os.getenv("ATIVO_INICIO_OPERACAO_LIST_IDS", _ativo_inicio_operacao_default_lists)
)
ATIVO_INICIO_OPERACAO_TRIGGER_STATUS: str = os.getenv(
    "ATIVO_INICIO_OPERACAO_TRIGGER_STATUS",
    "ativo",
).strip().lower()
ATIVO_INICIO_OPERACAO_FIELD_ID: str = os.getenv(
    "ATIVO_INICIO_OPERACAO_FIELD_ID",
    "ebd051a1-d5b6-4cb1-861b-574a1f968663",
).strip()

# Roteamento de auditoria por plano de adesao (auditoria -> onboarding/black)
AUDITORIA_ROUTING_SOURCE_LIST_IDS: list[str] = _parse_csv_values(
    os.getenv("AUDITORIA_ROUTING_SOURCE_LIST_IDS", DEST_LIST_ID)
)
AUDITORIA_ROUTING_TRIGGER_STATUS: str = os.getenv(
    "AUDITORIA_ROUTING_TRIGGER_STATUS",
    "auditoria",
).strip().lower()
AUDITORIA_ROUTING_PLAN_FIELD_ID: str = os.getenv(
    "AUDITORIA_ROUTING_PLAN_FIELD_ID",
    "0e009719-1e94-482a-825a-c359e268727e",
).strip()
AUDITORIA_ROUTING_BLACK_VALUES: list[str] = _parse_csv_values(
    os.getenv(
        "AUDITORIA_ROUTING_BLACK_VALUES",
        "Black Linear 25%,BLACK,Performance 15% (COPEL/CELESC),Max 25% (COPEL/CELESC)",
    )
)
AUDITORIA_ROUTING_ONBOARDING_LIST_ID: str = os.getenv(
    "AUDITORIA_ROUTING_ONBOARDING_LIST_ID",
    ONBOARDING_SYNC_LIST_ID,
).strip()
AUDITORIA_ROUTING_ONBOARDING_BLACK_LIST_ID: str = os.getenv(
    "AUDITORIA_ROUTING_ONBOARDING_BLACK_LIST_ID",
    ONBOARDING_BLACK_SYNC_LIST_ID,
).strip()
AUDITORIA_ROUTING_ONBOARDING_NEW_TASK_STATUS: str = os.getenv(
    "AUDITORIA_ROUTING_ONBOARDING_NEW_TASK_STATUS",
    "telefone etapa 1",
).strip()

# Roteamento de auditoria para rateio (auditoria -> ongoing/planejamento black)
AUDITORIA_RATEIO_TRIGGER_STATUS: str = os.getenv(
    "AUDITORIA_RATEIO_TRIGGER_STATUS",
    "enviado para rateio",
).strip().lower()
AUDITORIA_RATEIO_ONGOING_LIST_ID: str = os.getenv(
    "AUDITORIA_RATEIO_ONGOING_LIST_ID",
    ONGOING_SYNC_LIST_ID,
).strip()
AUDITORIA_RATEIO_BLACK_LIST_ID: str = os.getenv(
    "AUDITORIA_RATEIO_BLACK_LIST_ID",
    PLANEJAMENTO_BLACK_SYNC_LIST_ID,
).strip()

# Automacao de criacao (Adesao Reprovada -> Demissoes)
ADESAO_REPROVADA_LIST_ID: str = os.getenv(
    "ADESAO_REPROVADA_LIST_ID",
    "",
).strip()
DEMISSOES_LIST_ID: str = os.getenv(
    "DEMISSOES_LIST_ID",
    "",
).strip()
DEMISSOES_CREATE_STATUS: str = os.getenv(
    "DEMISSOES_CREATE_STATUS",
    "to do",
).strip()

# Inadimplentes: baixa de negativacao e bloqueio reativo de PAGO
INADIMPLENTES_FINALIZACAO_ENABLED: bool = _parse_bool(
    os.getenv("INADIMPLENTES_FINALIZACAO_ENABLED"),
    default=True,
)
INADIMPLENTES_LIST_ID: str = os.getenv(
    "INADIMPLENTES_LIST_ID",
    "901326084050",
).strip()
INADIMPLENTES_FROM_STATUS: str = os.getenv(
    "INADIMPLENTES_FROM_STATUS",
    "NEGATIVADO",
).strip()
INADIMPLENTES_READY_STATUS: str = os.getenv(
    "INADIMPLENTES_READY_STATUS",
    "A BAIXAR NEGATIVA\u00c7\u00c3O",
).strip()
INADIMPLENTES_PAID_STATUS: str = os.getenv(
    "INADIMPLENTES_PAID_STATUS",
    "PAGO",
).strip()
INADIMPLENTES_REQUIRED_SUBTASK_NAMES: list[str] = _parse_csv_values(
    os.getenv(
        "INADIMPLENTES_REQUIRED_SUBTASK_NAMES",
        (
            "Solicitar a baixa da negativa\u00e7\u00e3o,"
            "Enviar comprovante de baixa ao cooperado"
        ),
    )
)
INADIMPLENTES_COMPROVANTE_FIELD_ID: str = os.getenv(
    "INADIMPLENTES_COMPROVANTE_FIELD_ID",
    "3e4964bf-557b-4a47-8f86-b1bc43780910",
).strip()
INADIMPLENTES_COMPROVANTE_FIELD_NAME: str = os.getenv(
    "INADIMPLENTES_COMPROVANTE_FIELD_NAME",
    "Comprovante de Baixa",
).strip()
INADIMPLENTES_ALLOW_TASK_ATTACHMENT_COMPROVANTE: bool = _parse_bool(
    os.getenv("INADIMPLENTES_ALLOW_TASK_ATTACHMENT_COMPROVANTE"),
    default=True,
)
INADIMPLENTES_DONE_STATUS_TYPES: list[str] = _parse_csv_values(
    os.getenv("INADIMPLENTES_DONE_STATUS_TYPES", "done,closed")
)
INADIMPLENTES_DONE_STATUS_NAMES: list[str] = _parse_csv_values(
    os.getenv(
        "INADIMPLENTES_DONE_STATUS_NAMES",
        "concluido,concluida,feito,feita,done,complete,completed,closed",
    )
)

DEST_SYNC_ALLOWED_STATUSES: list[str] = _parse_csv_values(
    os.getenv(
        "DEST_SYNC_ALLOWED_STATUSES",
        "Aguardando Cadastro,Cadastro em Andamento,Ativo",
    )
)

# Field mapping (cf_id origem -> cf_id destino)
CLONE_FIELD_MAP = _parse_json_mapping(
    os.getenv("CLONE_FIELD_MAP", "{}"), "CLONE_FIELD_MAP"
)
ENV_SYNC_USE_DIRECT_FIELDS: bool = _parse_bool(
    os.getenv("ENV_SYNC_USE_DIRECT_FIELDS"),
    default=True,
)

# Task name formatter (reutilizavel para multiplas listas)
TASK_NAME_FORMAT_LIST_IDS: list[str] = _parse_csv_values(
    os.getenv("TASK_NAME_FORMAT_LIST_IDS", DEST_LIST_ID)
)
TASK_NAME_RAZAO_FIELD_ID: str = os.getenv(
    "TASK_NAME_RAZAO_FIELD_ID",
    "dfb0de9b-121a-4bf6-977f-dfb5eec523cb",
).strip()
TASK_NAME_UC_FIELD_ID: str = os.getenv(
    "TASK_NAME_UC_FIELD_ID",
    "abb7e1e9-3c99-4044-b20c-5eb19575a6d5",
).strip()
TASK_NAME_TEMPLATE: str = os.getenv(
    "TASK_NAME_TEMPLATE",
    "{razao} - UC {uc}",
).strip()
_TASK_NAME_FORMAT_RULES_DEFAULT = {
    "901326902129": {
        "field_a_id": "6b668919-9c13-4127-bc2e-fa14eee95e8a",
        "field_b_id": "2ef3a097-4122-4c3d-9626-000087de9ced",
        "template": "{field_a} - {field_b}",
    }
}
_task_name_format_rules_env = os.getenv("TASK_NAME_FORMAT_RULES")
_task_name_format_rules_raw = _parse_json_object(
    _task_name_format_rules_env
    if _task_name_format_rules_env is not None
    else json.dumps(_TASK_NAME_FORMAT_RULES_DEFAULT, ensure_ascii=False),
    "TASK_NAME_FORMAT_RULES",
)
TASK_NAME_FORMAT_RULES: dict[str, dict[str, str]] = {}
for raw_list_id, raw_rule in _task_name_format_rules_raw.items():
    list_id = str(raw_list_id or "").strip()
    if not list_id or not isinstance(raw_rule, dict):
        continue
    normalized_rule = {
        str(key).strip(): str(value).strip()
        for key, value in raw_rule.items()
        if str(key).strip() and value is not None and str(value).strip()
    }
    if normalized_rule:
        TASK_NAME_FORMAT_RULES[list_id] = normalized_rule

# Servidor
PORT: int = int(os.getenv("PORT", "8000"))
HOST: str = os.getenv("HOST", "0.0.0.0")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# Webhook
WEBHOOK_ENDPOINT: str = os.getenv("WEBHOOK_ENDPOINT", "").strip()
WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")
WEBHOOK_SECRETS: list[str] = _parse_csv_values(os.getenv("WEBHOOK_SECRETS", ""))
if WEBHOOK_SECRET and WEBHOOK_SECRET not in WEBHOOK_SECRETS:
    WEBHOOK_SECRETS.insert(0, WEBHOOK_SECRET)
WEBHOOK_WORKERS: int = max(1, int(os.getenv("WEBHOOK_WORKERS", "4")))
WEBHOOK_QUEUE_MAXSIZE: int = max(100, int(os.getenv("WEBHOOK_QUEUE_MAXSIZE", "2000")))
WEBHOOK_TEAM_IDS: list[str] = _parse_csv_values(os.getenv("WEBHOOK_TEAM_IDS", ""))
WEBHOOK_EXPECTED_EVENTS: list[str] = _parse_csv_values(
    os.getenv("WEBHOOK_EXPECTED_EVENTS", "taskStatusUpdated,taskCreated")
)
WEBHOOK_GUARD_ENABLED: bool = _parse_bool(
    os.getenv("WEBHOOK_GUARD_ENABLED"),
    default=True,
)
WEBHOOK_GUARD_INTERVAL_SECONDS: int = max(
    15,
    _parse_int(
        os.getenv("WEBHOOK_GUARD_INTERVAL_SECONDS"),
        default=60,
        var_name="WEBHOOK_GUARD_INTERVAL_SECONDS",
    ),
)
WEBHOOK_GUARD_FAIL_COUNT_THRESHOLD: int = max(
    1,
    _parse_int(
        os.getenv("WEBHOOK_GUARD_FAIL_COUNT_THRESHOLD"),
        default=5,
        var_name="WEBHOOK_GUARD_FAIL_COUNT_THRESHOLD",
    ),
)
WEBHOOK_GUARD_RECREATE_UNHEALTHY: bool = _parse_bool(
    os.getenv("WEBHOOK_GUARD_RECREATE_UNHEALTHY"),
    default=True,
)
WEBHOOK_GUARD_CREATE_IF_MISSING: bool = _parse_bool(
    os.getenv("WEBHOOK_GUARD_CREATE_IF_MISSING"),
    default=True,
)
WEBHOOK_GUARD_DELETE_DUPLICATES: bool = _parse_bool(
    os.getenv("WEBHOOK_GUARD_DELETE_DUPLICATES"),
    default=True,
)
WEBHOOK_GUARD_ROTATE_IF_SECRET_UNKNOWN: bool = _parse_bool(
    os.getenv("WEBHOOK_GUARD_ROTATE_IF_SECRET_UNKNOWN"),
    default=False,
)
WEBHOOK_GUARD_PERSIST_SECRETS: bool = _parse_bool(
    os.getenv("WEBHOOK_GUARD_PERSIST_SECRETS"),
    default=False,
)

# HTTP ClickUp
CLICKUP_HTTP_MAX_RETRIES: int = max(
    1,
    _parse_int(
        os.getenv("CLICKUP_HTTP_MAX_RETRIES"),
        default=6,
        var_name="CLICKUP_HTTP_MAX_RETRIES",
    ),
)
CLICKUP_HTTP_BACKOFF_SECONDS: float = max(
    0.1,
    _parse_float(
        os.getenv("CLICKUP_HTTP_BACKOFF_SECONDS"),
        default=1.5,
        var_name="CLICKUP_HTTP_BACKOFF_SECONDS",
    ),
)
CLICKUP_HTTP_MAX_BACKOFF_SECONDS: float = max(
    CLICKUP_HTTP_BACKOFF_SECONDS,
    _parse_float(
        os.getenv("CLICKUP_HTTP_MAX_BACKOFF_SECONDS"),
        default=30.0,
        var_name="CLICKUP_HTTP_MAX_BACKOFF_SECONDS",
    ),
)
CLICKUP_HTTP_CONNECT_TIMEOUT_SECONDS: float = max(
    1.0,
    _parse_float(
        os.getenv("CLICKUP_HTTP_CONNECT_TIMEOUT_SECONDS"),
        default=10.0,
        var_name="CLICKUP_HTTP_CONNECT_TIMEOUT_SECONDS",
    ),
)
CLICKUP_HTTP_READ_TIMEOUT_SECONDS: float = max(
    1.0,
    _parse_float(
        os.getenv("CLICKUP_HTTP_READ_TIMEOUT_SECONDS"),
        default=60.0,
        var_name="CLICKUP_HTTP_READ_TIMEOUT_SECONDS",
    ),
)
CLICKUP_HTTP_POOL_CONNECTIONS: int = max(
    4,
    _parse_int(
        os.getenv("CLICKUP_HTTP_POOL_CONNECTIONS"),
        default=20,
        var_name="CLICKUP_HTTP_POOL_CONNECTIONS",
    ),
)
CLICKUP_HTTP_POOL_MAXSIZE: int = max(
    CLICKUP_HTTP_POOL_CONNECTIONS,
    _parse_int(
        os.getenv("CLICKUP_HTTP_POOL_MAXSIZE"),
        default=50,
        var_name="CLICKUP_HTTP_POOL_MAXSIZE",
    ),
)

# Legado (nao utilizado no fluxo atual)
DEDUP_NAME_SOURCE_FIELD_ID: str = os.getenv(
    "DEDUP_NAME_SOURCE_FIELD_ID", "91f5b947-7c0a-4025-9b2b-06a6dac51651"
)
DEDUP_UC_SOURCE_FIELD_ID: str = os.getenv(
    "DEDUP_UC_SOURCE_FIELD_ID", "0ed5e250-d5f0-42d7-bee0-67c8acf14a79"
)

# Persistencia local
DATA_DIR: str = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"
)

# Validacao obrigatoria de runtime
_REQUIRED = {
    "SOURCE_CLICKUP_TOKEN": SOURCE_CLICKUP_TOKEN,
    "DEST_CLICKUP_TOKEN": DEST_CLICKUP_TOKEN,
    "SOURCE_LIST_MAP": bool(SOURCE_LIST_MAP),
    "SOURCE_RETURN_LIST_ID": SOURCE_RETURN_LIST_ID,
    "DEST_LIST_ID": DEST_LIST_ID,
}


def validate_config() -> list[str]:
    """Retorna lista de variaveis obrigatorias ausentes."""
    missing: list[str] = []
    for key, value in _REQUIRED.items():
        if not value:
            missing.append(key)
    return missing
