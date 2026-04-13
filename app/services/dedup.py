"""Deduplicacao para garantir clone unico por task."""

import json
import logging
import os
import threading

from app.config.settings import DATA_DIR

logger = logging.getLogger(__name__)

_DEDUP_FILE = os.path.join(DATA_DIR, "cloned_tasks.json")
_DEDUP_TMP_FILE = f"{_DEDUP_FILE}.tmp"
_lock = threading.Lock()
_cloned: set[str] = set()
_in_progress: set[str] = set()
_loaded = False


def _ensure_loaded() -> None:
    """Carrega arquivo de dedup do disco apenas uma vez."""
    global _cloned, _loaded
    if _loaded:
        return

    with _lock:
        if _loaded:
            return

        os.makedirs(DATA_DIR, exist_ok=True)
        if os.path.exists(_DEDUP_FILE):
            try:
                with open(_DEDUP_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    _cloned = {str(item) for item in data}
                else:
                    _cloned = set()
                logger.info("Dedup carregado: %d tasks ja clonadas.", len(_cloned))
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Erro ao ler dedup file, iniciando vazio: %s", e)
                _cloned = set()
        else:
            logger.info("Nenhum arquivo de dedup encontrado. Iniciando vazio.")
        _loaded = True


def _persist() -> None:
    """Persiste _cloned no disco com escrita atomica."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(_DEDUP_TMP_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(_cloned), f)
        os.replace(_DEDUP_TMP_FILE, _DEDUP_FILE)
    except OSError as e:
        logger.error("Falha ao persistir dedup: %s", e)
        try:
            if os.path.exists(_DEDUP_TMP_FILE):
                os.unlink(_DEDUP_TMP_FILE)
        except OSError:
            pass


def try_reserve(task_id: str) -> bool:
    """Reserva task para processamento.

    Retorna False se ja clonada ou em processamento.
    """
    _ensure_loaded()
    with _lock:
        if task_id in _cloned or task_id in _in_progress:
            return False
        _in_progress.add(task_id)
        return True


def release_reservation(task_id: str) -> None:
    """Libera reserva sem marcar como clonada (falha/ignorado)."""
    _ensure_loaded()
    with _lock:
        _in_progress.discard(task_id)


def mark_cloned(task_id: str) -> None:
    """Marca task como clonada e libera reserva."""
    _ensure_loaded()
    with _lock:
        _in_progress.discard(task_id)
        _cloned.add(task_id)
        _persist()
    logger.debug("Task %s marcada como clonada. Total: %d", task_id, len(_cloned))


def was_cloned(task_id: str) -> bool:
    """Verifica se task ja foi clonada ou esta em processamento."""
    _ensure_loaded()
    with _lock:
        return task_id in _cloned or task_id in _in_progress


def get_count() -> int:
    """Retorna quantidade de tasks definitivamente clonadas."""
    _ensure_loaded()
    with _lock:
        return len(_cloned)
