"""Configuracoes centrais carregadas de variaveis de ambiente."""

import json
import logging
import os
from typing import TypeVar

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


def _parse_csv_values(raw_value: str) -> list[str]:
    """Parseia lista CSV em formato simples, removendo vazios e espacos."""
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


# Origem unica monitorada
# Lista: https://app.clickup.com/90171084182/v/li/901713154569
# Trigger: cooperado aprovado
SOURCE_LIST_ID: str = os.getenv("SOURCE_LIST_ID", "901713154569").strip()
SOURCE_TRIGGER_STATUS: str = os.getenv(
    "SOURCE_TRIGGER_STATUS", "cooperado aprovado"
).strip().lower()
SOURCE_LIST_MAP = (
    {SOURCE_LIST_ID: SOURCE_TRIGGER_STATUS}
    if SOURCE_LIST_ID and SOURCE_TRIGGER_STATUS
    else {}
)

# Retorno entre destino e source de retrabalho
SOURCE_RETURN_LIST_ID: str = os.getenv("SOURCE_RETURN_LIST_ID", "901712728189").strip()
DEST_RETURN_TRIGGER_STATUS: str = os.getenv(
    "DEST_RETURN_TRIGGER_STATUS", "pendencias"
).strip().lower()
SOURCE_RETURN_TRIGGER_STATUS: str = os.getenv(
    "SOURCE_RETURN_TRIGGER_STATUS", "corrigido"
).strip().lower()

# Sincronismo de status entre listas no workspace destino
ONGOING_SYNC_LIST_ID: str = os.getenv(
    "ONGOING_SYNC_LIST_ID",
    os.getenv("DEST_SYNC_LIST_A_ID", "901326789506"),
).strip()
ONBOARDING_SYNC_LIST_ID: str = os.getenv(
    "ONBOARDING_SYNC_LIST_ID",
    os.getenv("DEST_SYNC_LIST_B_ID", "901326986645"),
).strip()

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
        "1ª fatura sem inj,1ª fatura com desconto",
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
WEBHOOK_TEAM_IDS: list[str] = _parse_csv_values(
    os.getenv("WEBHOOK_TEAM_IDS", "90171084182,9013290037")
)
WEBHOOK_EXPECTED_EVENTS: list[str] = _parse_csv_values(
    os.getenv("WEBHOOK_EXPECTED_EVENTS", "taskStatusUpdated")
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

# Deduplicacao por campos de negocio (origem)
DEDUP_NAME_SOURCE_FIELD_ID: str = os.getenv(
    "DEDUP_NAME_SOURCE_FIELD_ID", "91f5b947-7c0a-4025-9b2b-06a6dac51651"
)
DEDUP_UC_SOURCE_FIELD_ID: str = os.getenv(
    "DEDUP_UC_SOURCE_FIELD_ID", "0ed5e250-d5f0-42d7-bee0-67c8acf14a79"
)

# Persistencia de dedup
DATA_DIR: str = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"
)

# Validacao
_REQUIRED = {
    "SOURCE_CLICKUP_TOKEN": SOURCE_CLICKUP_TOKEN,
    "DEST_CLICKUP_TOKEN": DEST_CLICKUP_TOKEN,
    "SOURCE_LIST_MAP": bool(SOURCE_LIST_MAP),
    "DEST_LIST_ID": DEST_LIST_ID,
}


def validate_config() -> list[str]:
    """Retorna lista de variaveis obrigatorias que estao faltando."""
    missing = []
    for k, v in _REQUIRED.items():
        if not v:
            missing.append(k)
    return missing
