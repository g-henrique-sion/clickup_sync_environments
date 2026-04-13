"""Configuracoes centrais carregadas de variaveis de ambiente."""

import json
import logging
import os

from dotenv import load_dotenv

load_dotenv()

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


# Mapeamento lista_origem -> status trigger
# JSON: {"list_id": "status_trigger", ...}
SOURCE_LIST_MAP = {
    list_id: trigger.lower()
    for list_id, trigger in _parse_json_mapping(
        os.getenv("SOURCE_LIST_MAP", "{}"), "SOURCE_LIST_MAP"
    ).items()
}

# Field mapping (cf_id origem -> cf_id destino)
CLONE_FIELD_MAP = _parse_json_mapping(
    os.getenv("CLONE_FIELD_MAP", "{}"), "CLONE_FIELD_MAP"
)

# Servidor
PORT: int = int(os.getenv("PORT", "8000"))
HOST: str = os.getenv("HOST", "0.0.0.0")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# Webhook
WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")
WEBHOOK_WORKERS: int = max(1, int(os.getenv("WEBHOOK_WORKERS", "4")))
WEBHOOK_QUEUE_MAXSIZE: int = max(100, int(os.getenv("WEBHOOK_QUEUE_MAXSIZE", "2000")))

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
