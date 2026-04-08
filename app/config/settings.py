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
SOURCE_LIST_ID: str = os.getenv("SOURCE_LIST_ID", "")
DEST_LIST_ID: str = os.getenv("DEST_LIST_ID", "")
DEST_WORKSPACE_ID: str = os.getenv("DEST_WORKSPACE_ID", "")

# ── Trigger ───────────────────────────────────────────────────
TRIGGER_STATUS: str = os.getenv("TRIGGER_STATUS", "").strip().lower()

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
    "SOURCE_LIST_ID": SOURCE_LIST_ID,
    "DEST_LIST_ID": DEST_LIST_ID,
    "TRIGGER_STATUS": TRIGGER_STATUS,
}


def validate_config() -> list[str]:
    """Retorna lista de variáveis obrigatórias que estão faltando."""
    return [k for k, v in _REQUIRED.items() if not v]
