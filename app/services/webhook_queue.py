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

import requests

from app.config.settings import DATA_DIR, WEBHOOK_QUEUE_MAXSIZE, WEBHOOK_WORKERS
from app.services.clone_service import process_clickup_event

logger = logging.getLogger(__name__)

_EVENTS_FILE = os.path.join(DATA_DIR, "webhook_events.json")
_EVENTS_TMP_FILE = f"{_EVENTS_FILE}.tmp"


@dataclass
class WebhookEvent:
    id: str
    task_id: str
    event_type: str
    new_status: str
    normalized_status: str
    old_status: str | None = None
    status_changed_at_ms: int | None = None
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
_key_to_event_id: dict[tuple[str, str, str], str] = {}
_queued_event_ids: set[str] = set()
_task_processing_locks: dict[str, asyncio.Lock] = {}


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
                event_type=str(item.get("event_type") or "taskStatusUpdated"),
                new_status=str(item["new_status"]),
                old_status=(
                    str(item["old_status"])
                    if item.get("old_status") is not None
                    else None
                ),
                status_changed_at_ms=(
                    int(item["status_changed_at_ms"])
                    if item.get("status_changed_at_ms") is not None
                    else None
                ),
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

        key = (event.task_id, event.event_type, event.normalized_status)
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
    key = (event.task_id, event.event_type, event.normalized_status)
    _key_to_event_id.pop(key, None)


def _remove_events_by_task_and_type_locked(task_id: str, event_type: str) -> list[str]:
    """Remove eventos pendentes de uma task/tipo para manter apenas o mais recente."""
    removed_ids: list[str] = []
    for event_id, event in list(_events_by_id.items()):
        if event.task_id != task_id:
            continue
        if event.event_type != event_type:
            continue
        removed_ids.append(event_id)
        _remove_event_locked(event_id)
    return removed_ids


def _get_task_processing_lock(task_id: str) -> asyncio.Lock:
    lock = _task_processing_locks.get(task_id)
    if lock is None:
        lock = asyncio.Lock()
        _task_processing_locks[task_id] = lock
    return lock


def _compute_retry_delay_seconds(attempts: int) -> float:
    # Backoff exponencial com teto de 5 minutos.
    return float(min(300, 2 ** min(attempts, 8)))


def _extract_http_status_code(error: Exception) -> int | None:
    status_attr = getattr(error, "status_code", None)
    if status_attr is not None:
        try:
            return int(status_attr)
        except Exception:
            pass

    if isinstance(error, requests.HTTPError) and error.response is not None:
        try:
            return int(error.response.status_code)
        except Exception:
            return None
    return None


def _is_non_retryable_error(
    error: Exception,
    *,
    event_type: str,
    attempts: int,
) -> bool:
    status = _extract_http_status_code(error)
    if status in {400, 401, 403, 404, 422}:
        return True

    if status is None:
        return False

    # Mantido para legibilidade; com os status acima, nao chega aqui.
    if event_type == "taskCreated":
        return attempts >= 8
    return attempts >= 3


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


async def enqueue_webhook(
    task_id: str,
    new_status: str,
    *,
    event_type: str = "taskStatusUpdated",
    old_status: str | None = None,
    status_changed_at_ms: int | None = None,
) -> bool:
    """Enfileira evento de webhook para processamento assincrono duravel.

    Retorna:
        True se evento novo foi aceito/persistido.
        False se ja havia evento equivalente pendente.
    """
    normalized_status = _normalize_status(new_status)
    normalized_event = str(event_type or "").strip() or "taskStatusUpdated"
    key = (task_id, normalized_event, normalized_status)

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

        removed_ids: list[str] = []
        if normalized_event == "taskStatusUpdated":
            removed_ids = _remove_events_by_task_and_type_locked(task_id, normalized_event)
        if removed_ids:
            logger.debug(
                "enqueue_webhook.coalesce task_id=%s removed_events=%s",
                task_id,
                removed_ids,
            )

        event = WebhookEvent(
            id=uuid.uuid4().hex,
            task_id=task_id,
            event_type=normalized_event,
            new_status=new_status,
            old_status=(str(old_status).strip() if old_status is not None else None),
            status_changed_at_ms=(int(status_changed_at_ms) if status_changed_at_ms is not None else None),
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

    logger.info(
        "enqueue_webhook.ok task_id=%s event=%s old_status='%s' status='%s' change_ms=%s event_id=%s pending=%d",
        task_id,
        normalized_event,
        old_status or "",
        new_status,
        status_changed_at_ms if status_changed_at_ms is not None else "",
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
        try:
            if item is None:
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
                continue

            # Garante processamento serial por task para evitar corrida entre statuses.
            task_lock = _get_task_processing_lock(event.task_id)
            async with task_lock:
                # Revalida evento ao entrar no lock, pois pode ter sido coalescido/descartado.
                async with _store_lock:
                    current_event = _events_by_id.get(event_id)
                if not current_event:
                    logger.debug(
                        "worker.skip event_coalescido_pos_lock worker=%d event_id=%s",
                        worker_id,
                        event_id,
                    )
                    continue
                event = current_event

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
                    continue

                started = time.time()
                logger.debug(
                    "worker.processando worker=%d event_id=%s task_id=%s event=%s old_status='%s' status='%s' change_ms=%s attempts=%d",
                    worker_id,
                    event_id,
                    event.task_id,
                    event.event_type,
                    event.old_status or "",
                    event.new_status,
                    event.status_changed_at_ms if event.status_changed_at_ms is not None else "",
                    event.attempts,
                )
                try:
                    result = await asyncio.to_thread(
                        process_clickup_event,
                        event.task_id,
                        event.event_type,
                        event.new_status,
                        event.old_status,
                        event.status_changed_at_ms,
                    )
                except Exception as e:
                    if _is_non_retryable_error(
                        e,
                        event_type=event.event_type,
                        attempts=event.attempts + 1,
                    ):
                        status = _extract_http_status_code(e)
                        async with _store_lock:
                            _remove_event_locked(event_id)
                            _persist_events_locked()
                        logger.warning(
                            "worker.drop_non_retryable worker=%d event_id=%s task_id=%s event=%s status_evento='%s' http_status=%s pending=%d",
                            worker_id,
                            event_id,
                            event.task_id,
                            event.event_type,
                            event.new_status,
                            status,
                            len(_events_by_id),
                        )
                        continue

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
                        "worker.erro worker=%d event_id=%s task_id=%s event=%s status='%s' tentativa=%d retry_s=%.1f",
                        worker_id,
                        event_id,
                        event.task_id,
                        event.event_type,
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
                            "worker.ok worker=%d event_id=%s task_id=%s event=%s status='%s' elapsed_s=%.3f pending=%d",
                            worker_id,
                            event_id,
                            event.task_id,
                            event.event_type,
                            event.new_status,
                            elapsed,
                            len(_events_by_id),
                        )
                    else:
                        logger.info(
                            "worker.noop worker=%d event_id=%s task_id=%s event=%s status='%s' elapsed_s=%.3f pending=%d",
                            worker_id,
                            event_id,
                            event.task_id,
                            event.event_type,
                            event.new_status,
                            elapsed,
                            len(_events_by_id),
                        )
        finally:
            _queue.task_done()
