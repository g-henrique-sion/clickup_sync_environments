"""Fila de processamento para desacoplar webhook de clonagem."""

from __future__ import annotations

import asyncio
import logging

from app.config.settings import WEBHOOK_QUEUE_MAXSIZE, WEBHOOK_WORKERS
from app.services.clone_service import process_status_change

logger = logging.getLogger(__name__)

_queue: asyncio.Queue[tuple[str, str] | None] = asyncio.Queue(
    maxsize=WEBHOOK_QUEUE_MAXSIZE
)
_workers: list[asyncio.Task] = []
_pending_keys: set[tuple[str, str]] = set()
_pending_lock = asyncio.Lock()
_started = False


def _normalize_status(status: str) -> str:
    return status.strip().lower()


async def start_workers() -> None:
    """Inicia workers em background apenas uma vez."""
    global _started
    if _started:
        return

    _started = True
    for i in range(WEBHOOK_WORKERS):
        task = asyncio.create_task(_worker_loop(i + 1), name=f"webhook-worker-{i + 1}")
        _workers.append(task)
    logger.info("Workers de webhook iniciados: %d", len(_workers))


async def stop_workers() -> None:
    """Encerra workers e limpa estado em memoria."""
    global _started
    if not _started:
        return

    _started = False
    for _ in _workers:
        await _queue.put(None)

    for task in _workers:
        try:
            await task
        except Exception:
            logger.exception("Erro ao finalizar worker de webhook.")

    _workers.clear()
    async with _pending_lock:
        _pending_keys.clear()
    logger.info("Workers de webhook finalizados.")


async def enqueue_webhook(task_id: str, new_status: str) -> bool:
    """Enfileira evento de webhook para processamento assincrono.

    Retorna:
        True se entrou na fila.
        False se ja havia evento equivalente pendente.
    """
    key = (task_id, _normalize_status(new_status))
    async with _pending_lock:
        if key in _pending_keys:
            return False
        _pending_keys.add(key)

    try:
        _queue.put_nowait((task_id, new_status))
        return True
    except asyncio.QueueFull:
        async with _pending_lock:
            _pending_keys.discard(key)
        logger.error("Fila de webhook cheia (%d). Evento descartado.", WEBHOOK_QUEUE_MAXSIZE)
        raise


def get_queue_stats() -> dict[str, int]:
    """Retorna metricas basicas da fila para observabilidade."""
    return {
        "queue_size": _queue.qsize(),
        "queue_maxsize": WEBHOOK_QUEUE_MAXSIZE,
        "workers": len(_workers),
        "pending_unique": len(_pending_keys),
    }


async def _worker_loop(worker_id: int) -> None:
    while True:
        item = await _queue.get()
        if item is None:
            _queue.task_done()
            logger.info("Worker %d encerrado.", worker_id)
            break

        task_id, new_status = item
        key = (task_id, _normalize_status(new_status))

        try:
            await asyncio.to_thread(process_status_change, task_id, new_status)
        except Exception:
            logger.exception(
                "Worker %d falhou ao processar task %s (%s).",
                worker_id,
                task_id,
                new_status,
            )
        finally:
            async with _pending_lock:
                _pending_keys.discard(key)
            _queue.task_done()
