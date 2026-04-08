"""Deduplicação — garante que cada task seja clonada apenas uma vez.

Persiste em arquivo JSON simples para sobreviver a restarts no Railway.
"""

import json
import logging
import os
import threading

from app.config.settings import DATA_DIR

logger = logging.getLogger(__name__)

_DEDUP_FILE = os.path.join(DATA_DIR, "cloned_tasks.json")
_lock = threading.Lock()
_cloned: set[str] = set()
_loaded = False


def _ensure_loaded() -> None:
    """Carrega o arquivo de dedup do disco (lazy, uma vez)."""
    global _cloned, _loaded
    if _loaded:
        return

    with _lock:
        if _loaded:
            return

        os.makedirs(DATA_DIR, exist_ok=True)

        if os.path.exists(_DEDUP_FILE):
            try:
                with open(_DEDUP_FILE, "r") as f:
                    data = json.load(f)
                _cloned = set(data)
                logger.info("Dedup carregado: %d tasks já clonadas.", len(_cloned))
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Erro ao ler dedup file, iniciando vazio: %s", e)
                _cloned = set()
        else:
            logger.info("Nenhum arquivo de dedup encontrado. Iniciando vazio.")

        _loaded = True


def _persist() -> None:
    """Salva o set de dedup no disco."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(_DEDUP_FILE, "w") as f:
            json.dump(sorted(_cloned), f)
    except OSError as e:
        logger.error("Falha ao persistir dedup: %s", e)


def was_cloned(task_id: str) -> bool:
    """Verifica se uma task já foi clonada."""
    _ensure_loaded()
    return task_id in _cloned


def mark_cloned(task_id: str) -> None:
    """Marca uma task como clonada e persiste."""
    _ensure_loaded()
    with _lock:
        _cloned.add(task_id)
        _persist()
    logger.debug("Task %s marcada como clonada. Total: %d", task_id, len(_cloned))


def get_count() -> int:
    """Retorna quantas tasks já foram clonadas."""
    _ensure_loaded()
    return len(_cloned)
