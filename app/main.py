"""FastAPI - servidor de webhook para sync ClickUp -> ClickUp."""

import asyncio
import hashlib
import hmac
import json
import logging

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from app.config.settings import WEBHOOK_SECRETS, validate_config
from app.core.logger import setup_logging
from app.models.schemas import WebhookPayload
from app.services.webhook_guard import (
    get_runtime_webhook_secrets,
    get_webhook_guard_stats,
    start_webhook_guard,
    stop_webhook_guard,
)
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


def _active_webhook_secrets() -> list[str]:
    merged: list[str] = []
    for secret in WEBHOOK_SECRETS:
        secret_text = str(secret or "").strip()
        if secret_text and secret_text not in merged:
            merged.append(secret_text)
    for secret in get_runtime_webhook_secrets():
        secret_text = str(secret or "").strip()
        if secret_text and secret_text not in merged:
            merged.append(secret_text)
    return merged


@app.on_event("startup")
async def startup() -> None:
    missing = validate_config()
    if missing:
        logger.error("Variaveis obrigatorias faltando: %s", ", ".join(missing))
        logger.error("Configure o .env e reinicie.")
    else:
        logger.info("clickup_sync_environments iniciado com sucesso.")

    await start_workers()
    await start_webhook_guard()


@app.on_event("shutdown")
async def shutdown() -> None:
    await stop_webhook_guard()
    await stop_workers()


@app.get("/health")
async def health():
    """Endpoint de health check para Railway."""
    missing = validate_config()
    return {
        "status": "ok" if not missing else "misconfigured",
        "missing_config": missing,
        **get_queue_stats(),
        **get_webhook_guard_stats(),
    }


@app.post("/webhook")
async def receive_webhook(request: Request):
    """Recebe eventos do webhook do ClickUp e enfileira processamento."""
    raw_body = await request.body()

    # Validacao de signature via HMAC-SHA256 (opcional)
    active_secrets = _active_webhook_secrets()
    if active_secrets:
        signature = request.headers.get("X-Signature")
        is_valid = any(
            hmac.compare_digest(
                signature or "",
                hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest(),
            )
            for secret in active_secrets
        )
        if not is_valid:
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
        logger.debug("Request de verificacao do ClickUp recebido.")
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
        logger.debug(
            "Evento ignorado: event=%s task_id=%s motivo=not_status_update",
            payload.event,
            payload.task_id,
        )
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
        logger.debug(
            "Evento ignorado: task_id=%s status='%s' motivo=already_queued",
            payload.task_id,
            new_status,
        )
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
