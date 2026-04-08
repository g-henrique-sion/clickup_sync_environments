"""Configurações centrais carregadas de variáveis de ambiente."""

import json
import logging
import os

from dotenv import load_dotenv

load_dotenv()

# ── Tokens ────────────────────────────────────────────────────
SOURCE_CLICKUP_TOKEN: str = os.getenv("SOURCE_CLICKUP_TOKEN", "")
DEST_CLICKUP_TOKEN: str = os.getenv("DEST_CLICKUP_TOKEN", "")

# ── IDs ───────────────────────────────────────────────────────
DEST_LIST_ID: str = os.getenv("DEST_LIST_ID", "")
DEST_WORKSPACE_ID: str = os.getenv("DEST_WORKSPACE_ID", "")

# ── Mapeamento lista_origem -> status trigger ─────────────────
# JSON: {"list_id": "status_trigger", ...}
# Cada lista monitorada tem seu próprio status que dispara a clonagem.
_raw_list_map = os.getenv("SOURCE_LIST_MAP", "{}")
try:
    SOURCE_LIST_MAP: dict[str, str] = {
        k.strip(): v.strip().lower()
        for k, v in json.loads(_raw_list_map).items()
    }
except json.JSONDecodeError:
    logging.getLogger(__name__).warning(
        "SOURCE_LIST_MAP inválido (JSON). Valor bruto ignorado."
    )
    SOURCE_LIST_MAP = {}

# ── Field mapping (cf_id origem -> cf_id destino) ─────────────
_raw_map = os.getenv("CLONE_FIELD_MAP", "{}")
try:
    CLONE_FIELD_MAP: dict[str, str] = json.loads(_raw_map)
except json.JSONDecodeError:
    logging.getLogger(__name__).warning(
        "CLONE_FIELD_MAP invÃ¡lido (JSON). Valor bruto ignorado."
    )
    CLONE_FIELD_MAP = {}

# ── Servidor ──────────────────────────────────────────────────
PORT: int = int(os.getenv("PORT", "8000"))
HOST: str = os.getenv("HOST", "0.0.0.0")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# ── Webhook ───────────────────────────────────────────────────
WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")

# ── Persistência de dedup ─────────────────────────────────────
DATA_DIR: str = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")

# ── Validação ─────────────────────────────────────────────────
_REQUIRED = {
    "SOURCE_CLICKUP_TOKEN": SOURCE_CLICKUP_TOKEN,
    "DEST_CLICKUP_TOKEN": DEST_CLICKUP_TOKEN,
    "SOURCE_LIST_MAP": bool(SOURCE_LIST_MAP),
    "DEST_LIST_ID": DEST_LIST_ID,
}


def validate_config() -> list[str]:
    """Retorna lista de variáveis obrigatórias que estão faltando."""
    missing = []
    for k, v in _REQUIRED.items():
        if not v:
            missing.append(k)
    return missing