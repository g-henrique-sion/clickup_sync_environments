"""Fila de processamento para desacoplar webhook de clonagem.

Implementa fila duravel com persistencia em disco e retry automatico.
Se um evento for aceito no webhook, ele fica salvo ate processar com sucesso.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import unicodedata
import uuid
from dataclasses import asdict, dataclass

from app.config.settings import DATA_DIR, WEBHOOK_QUEUE_MAXSIZE, WEBHOOK_WORKERS
from app.services.clone_service import process_status_change

logger = logging.getLogger(__name__)

_EVENTS_FILE = os.path.join(DATA_DIR, "webhook_events.json")
_EVENTS_TMP_FILE = f"{_EVENTS_FILE}.tmp"


@dataclass
class WebhookEvent:
    id: str
    task_id: str
    new_status: str
    normalized_status: str
    attempts: int = 0
    next_retry_at: float = 0.0
    created_at: float = 0.0
    last_error: str | None = None


_queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=WEBHOOK_QUEUE_MAXSIZE)
_workers: list[asyncio.Task] = []
_retry_tasks: set[asyncio.Task] = set()

_store_lock = asyncio.Lock()
_store_loaded = False
_started = False

_events_by_id: dict[str, WebhookEvent] = {}
_key_to_event_id: dict[tuple[str, str], str] = {}
_queued_event_ids: set[str] = set()


def _normalize_status(status: str) -> str:
    normalized = unicodedata.normalize("NFKD", status.strip().lower())
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _load_events_from_disk_locked() -> None:
    global _store_loaded
    if _store_loaded:
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(_EVENTS_FILE):
        _store_loaded = True
        return

    try:
        with open(_EVENTS_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.error("Falha ao carregar fila duravel de webhook: %s", e)
        _store_loaded = True
        return

    if not isinstance(raw, list):
        logger.warning("Arquivo de fila duravel invalido. Ignorando conteudo.")
        _store_loaded = True
        return

    loaded = 0
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            event = WebhookEvent(
                id=str(item.get("id") or uuid.uuid4().hex),
                task_id=str(item["task_id"]),
                new_status=str(item["new_status"]),
                normalized_status=str(
                    item.get("normalized_status") or _normalize_status(str(item["new_status"]))
                ),
                attempts=int(item.get("attempts") or 0),
                next_retry_at=float(item.get("next_retry_at") or 0.0),
                created_at=float(item.get("created_at") or time.time()),
                last_error=(
                    str(item["last_error"]) if item.get("last_error") is not None else None
                ),
            )
        except Exception:
            continue

        key = (event.task_id, event.normalized_status)
        if key in _key_to_event_id:
            # Mantem o mais recente em caso de duplicata no arquivo.
            continue
        _events_by_id[event.id] = event
        _key_to_event_id[key] = event.id
        loaded += 1

    _store_loaded = True
    if loaded:
        logger.debug("Fila duravel carregada: %d evento(s) pendente(s).", loaded)


def _persist_events_locked() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    payload = [asdict(event) for event in _events_by_id.values()]
    with open(_EVENTS_TMP_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(_EVENTS_TMP_FILE, _EVENTS_FILE)


def _try_enqueue_event_locked(event_id: str) -> bool:
    if event_id in _queued_event_ids:
        return True
    try:
        _queue.put_nowait(event_id)
        _queued_event_ids.add(event_id)
        return True
    except asyncio.QueueFull:
        return False


def _remove_event_locked(event_id: str) -> None:
    event = _events_by_id.pop(event_id, None)
    _queued_event_ids.discard(event_id)
    if not event:
        return
    key = (event.task_id, event.normalized_status)
    _key_to_event_id.pop(key, None)


def _remove_events_by_task_locked(task_id: str) -> list[str]:
    """Remove eventos pendentes de uma task para manter apenas o mais recente."""
    removed_ids: list[str] = []
    for event_id, event in list(_events_by_id.items()):
        if event.task_id != task_id:
            continue
        removed_ids.append(event_id)
        _remove_event_locked(event_id)
    return removed_ids


def _compute_retry_delay_seconds(attempts: int) -> float:
    # Backoff exponencial com teto de 5 minutos.
    return float(min(300, 2 ** min(attempts, 8)))


def _spawn_requeue_task(event_id: str, delay_seconds: float) -> None:
    async def _runner() -> None:
        try:
            await asyncio.sleep(max(0.0, delay_seconds))
            while True:
                async with _store_lock:
                    if not _started:
                        return
                    if event_id not in _events_by_id:
                        return
                    if _try_enqueue_event_locked(event_id):
                        return
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Erro ao agendar requeue do evento %s.", event_id)

    task = asyncio.create_task(_runner(), name=f"webhook-requeue-{event_id[:8]}")
    _retry_tasks.add(task)
    task.add_done_callback(lambda t: _retry_tasks.discard(t))


async def start_workers() -> None:
    """Inicia workers em background e restaura eventos pendentes."""
    global _started
    if _started:
        return

    async with _store_lock:
        _load_events_from_disk_locked()

    _started = True
    for i in range(WEBHOOK_WORKERS):
        task = asyncio.create_task(_worker_loop(i + 1), name=f"webhook-worker-{i + 1}")
        _workers.append(task)

    overflow_ids: list[str] = []
    async with _store_lock:
        for event_id in list(_events_by_id.keys()):
            if not _try_enqueue_event_locked(event_id):
                overflow_ids.append(event_id)

    for event_id in overflow_ids:
        _spawn_requeue_task(event_id, delay_seconds=0.5)

    logger.debug(
        "Workers de webhook iniciados: %d (pendentes=%d)",
        len(_workers),
        len(_events_by_id),
    )


async def stop_workers() -> None:
    """Encerra workers sem descartar eventos pendentes."""
    global _started
    if not _started:
        return

    _started = False

    for task in list(_retry_tasks):
        task.cancel()
    if _retry_tasks:
        await asyncio.gather(*_retry_tasks, return_exceptions=True)
    _retry_tasks.clear()

    for _ in _workers:
        await _queue.put(None)

    for task in _workers:
        try:
            await task
        except Exception:
            logger.exception("Erro ao finalizar worker de webhook.")
    _workers.clear()

    logger.debug("Workers de webhook finalizados.")


async def enqueue_webhook(task_id: str, new_status: str) -> bool:
    """Enfileira evento de webhook para processamento assincrono duravel.

    Retorna:
        True se evento novo foi aceito/persistido.
        False se ja havia evento equivalente pendente.
    """
    normalized_status = _normalize_status(new_status)
    key = (task_id, normalized_status)

    needs_requeue = False
    event_id = ""
    async with _store_lock:
        _load_events_from_disk_locked()
        if key in _key_to_event_id:
            logger.debug(
                "enqueue_webhook.duplicate task_id=%s status='%s' normalized='%s' existing_event_id=%s",
                task_id,
                new_status,
                normalized_status,
                _key_to_event_id.get(key),
            )
            return False

        removed_ids = _remove_events_by_task_locked(task_id)
        if removed_ids:
            logger.debug(
                "enqueue_webhook.coalesce task_id=%s removed_events=%s",
                task_id,
                removed_ids,
            )

        event = WebhookEvent(
            id=uuid.uuid4().hex,
            task_id=task_id,
            new_status=new_status,
            normalized_status=normalized_status,
            attempts=0,
            next_retry_at=0.0,
            created_at=time.time(),
        )
        event_id = event.id
        _events_by_id[event.id] = event
        _key_to_event_id[key] = event.id
        _persist_events_locked()

        if not _try_enqueue_event_locked(event.id):
            needs_requeue = True

    logger.debug(
        "enqueue_webhook.ok task_id=%s status='%s' normalized='%s' event_id=%s pending=%d",
        task_id,
        new_status,
        normalized_status,
        event_id,
        len(_events_by_id),
    )

    if needs_requeue:
        logger.warning(
            "Fila em memoria cheia (%d). Evento %s persistido e agendado para requeue.",
            WEBHOOK_QUEUE_MAXSIZE,
            event_id,
        )
        _spawn_requeue_task(event_id, delay_seconds=0.5)

    return True


def get_queue_stats() -> dict[str, int]:
    """Retorna metricas basicas da fila para observabilidade."""
    return {
        "queue_size": _queue.qsize(),
        "queue_maxsize": WEBHOOK_QUEUE_MAXSIZE,
        "workers": len(_workers),
        "pending_unique": len(_events_by_id),
    }


async def _worker_loop(worker_id: int) -> None:
    while True:
        item = await _queue.get()
        if item is None:
            _queue.task_done()
            logger.info("Worker %d encerrado.", worker_id)
            break

        event_id = item
        async with _store_lock:
            _queued_event_ids.discard(event_id)
            event = _events_by_id.get(event_id)

        if not event:
            logger.debug(
                "worker.skip event_inexistente_ou_coalescido worker=%d event_id=%s",
                worker_id,
                event_id,
            )
            _queue.task_done()
            continue

        now = time.time()
        if event.next_retry_at and now < event.next_retry_at:
            logger.debug(
                "worker.requeue_espera worker=%d event_id=%s task_id=%s status='%s' wait_s=%.1f",
                worker_id,
                event_id,
                event.task_id,
                event.new_status,
                event.next_retry_at - now,
            )
            _spawn_requeue_task(event_id, delay_seconds=event.next_retry_at - now)
            _queue.task_done()
            continue

        started = time.time()
        logger.debug(
            "worker.processando worker=%d event_id=%s task_id=%s status='%s' attempts=%d",
            worker_id,
            event_id,
            event.task_id,
            event.new_status,
            event.attempts,
        )
        try:
            result = await asyncio.to_thread(
                process_status_change,
                event.task_id,
                event.new_status,
            )
        except Exception as e:
            attempts = event.attempts + 1
            delay = _compute_retry_delay_seconds(attempts)
            async with _store_lock:
                current = _events_by_id.get(event_id)
                if current:
                    current.attempts = attempts
                    current.last_error = str(e)
                    current.next_retry_at = time.time() + delay
                    _persist_events_locked()

            logger.exception(
                "worker.erro worker=%d event_id=%s task_id=%s status='%s' tentativa=%d retry_s=%.1f",
                worker_id,
                event_id,
                event.task_id,
                event.new_status,
                attempts,
                delay,
            )
            _spawn_requeue_task(event_id, delay_seconds=delay)
        else:
            async with _store_lock:
                _remove_event_locked(event_id)
                _persist_events_locked()
            elapsed = time.time() - started
            if result is not None:
                logger.info(
                    "worker.ok worker=%d event_id=%s task_id=%s status='%s' elapsed_s=%.3f pending=%d",
                    worker_id,
                    event_id,
                    event.task_id,
                    event.new_status,
                    elapsed,
                    len(_events_by_id),
                )
            else:
                logger.debug(
                    "worker.ok_noop worker=%d event_id=%s task_id=%s status='%s' elapsed_s=%.3f pending=%d",
                    worker_id,
                    event_id,
                    event.task_id,
                    event.new_status,
                    elapsed,
                    len(_events_by_id),
                )
        finally:
            _queue.task_done()
