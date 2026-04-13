"""FastAPI - servidor de webhook para sync ClickUp -> ClickUp."""

import asyncio
import hashlib
import hmac
import json
import logging

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from app.config.settings import WEBHOOK_SECRET, validate_config
from app.core.logger import setup_logging
from app.models.schemas import WebhookPayload
from app.services.dedup import get_count
from app.services.webhook_queue import (
    enqueue_webhook,
    get_queue_stats,
    start_workers,
    stop_workers,
)

# Setup
setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="clickup_sync_environments",
    description="Webhook que clona tasks entre workspaces ClickUp ao detectar mudanca de status.",
    version="1.1.0",
)


@app.on_event("startup")
async def startup() -> None:
    missing = validate_config()
    if missing:
        logger.error("Variaveis obrigatorias faltando: %s", ", ".join(missing))
        logger.error("Configure o .env e reinicie.")
    else:
        logger.info("clickup_sync_environments iniciado com sucesso.")
        logger.info("Tasks ja clonadas (dedup): %d", get_count())

    await start_workers()


@app.on_event("shutdown")
async def shutdown() -> None:
    await stop_workers()


@app.get("/health")
async def health():
    """Endpoint de health check para Railway."""
    missing = validate_config()
    return {
        "status": "ok" if not missing else "misconfigured",
        "cloned_count": get_count(),
        "missing_config": missing,
        **get_queue_stats(),
    }


@app.post("/webhook")
async def receive_webhook(request: Request):
    """Recebe eventos do webhook do ClickUp e enfileira processamento."""
    raw_body = await request.body()

    # Validacao de signature via HMAC-SHA256 (opcional)
    if WEBHOOK_SECRET:
        signature = request.headers.get("X-Signature")
        expected = hmac.new(WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature or "", expected):
            logger.warning("Webhook recebido com signature invalida.")
            return Response(status_code=401)

    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.warning("Webhook com JSON invalido recebido.")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "detail": "invalid json"},
        )

    # ClickUp envia um request de verificacao ao registrar o webhook
    if "event" not in body:
        logger.info("Request de verificacao do ClickUp recebido.")
        return {"status": "ok"}

    try:
        payload = WebhookPayload(**body)
    except Exception as e:
        logger.warning("Payload invalido: %s", e)
        return JSONResponse(
            status_code=400,
            content={"status": "error", "detail": "invalid payload"},
        )

    # Filtra apenas eventos de status update
    if payload.event != "taskStatusUpdated":
        return {"status": "ignored", "reason": "not a status update"}

    if not payload.task_id:
        logger.warning("Evento taskStatusUpdated sem task_id.")
        return {"status": "ignored", "reason": "no task_id"}

    new_status = payload.get_new_status()
    if not new_status:
        logger.warning("Evento de status sem novo status no history_items.")
        return {"status": "ignored", "reason": "no status in history"}

    try:
        enqueued = await enqueue_webhook(payload.task_id, new_status)
    except asyncio.QueueFull:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "detail": "queue overloaded"},
        )

    if not enqueued:
        return {
            "status": "ignored",
            "reason": "already queued",
            "source_task_id": payload.task_id,
        }

    return JSONResponse(
        status_code=202,
        content={
            "status": "queued",
            "source_task_id": payload.task_id,
            "new_status": new_status,
        },
    )
