"""FastAPI — Servidor de webhook para sync ClickUp → ClickUp."""

import hashlib
import hmac
import json
import logging

from fastapi import FastAPI, Request, Response

from app.config.settings import WEBHOOK_SECRET, validate_config
from app.core.logger import setup_logging
from app.models.schemas import WebhookPayload
from app.services.clone_service import process_status_change
from app.services.dedup import get_count

# ── Setup ─────────────────────────────────────────────────────
setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="clickup_sync_environments",
    description="Webhook que clona tasks entre workspaces ClickUp ao detectar mudança de status.",
    version="1.0.0",
)


# ── Startup ───────────────────────────────────────────────────
@app.on_event("startup")
async def startup() -> None:
    missing = validate_config()
    if missing:
        logger.error("Variáveis obrigatórias faltando: %s", ", ".join(missing))
        logger.error("Configure o .env e reinicie.")
    else:
        logger.info("clickup_sync_environments iniciado com sucesso.")
        logger.info("Tasks já clonadas (dedup): %d", get_count())


# ── Health check ──────────────────────────────────────────────
@app.get("/health")
async def health():
    """Endpoint de health check para Railway."""
    missing = validate_config()
    return {
        "status": "ok" if not missing else "misconfigured",
        "cloned_count": get_count(),
        "missing_config": missing,
    }


# ── Webhook endpoint ─────────────────────────────────────────
@app.post("/webhook")
async def receive_webhook(request: Request):
    """Recebe eventos do webhook do ClickUp."""

    # Lê o body cru (necessário para HMAC)
    raw_body = await request.body()

    # Validação de signature via HMAC-SHA256 (opcional)
    if WEBHOOK_SECRET:
        signature = request.headers.get("X-Signature")
        expected = hmac.new(
            WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature or "", expected):
            logger.warning("Webhook recebido com signature inválida.")
            return Response(status_code=401)

    body = json.loads(raw_body)

    # ClickUp envia um request de verificação ao registrar o webhook
    if "event" not in body:
        logger.info("Request de verificação do ClickUp recebido.")
        return {"status": "ok"}

    try:
        payload = WebhookPayload(**body)
    except Exception as e:
        logger.error("Payload inválido: %s", e)
        return {"status": "error", "detail": "invalid payload"}

    # Filtra apenas eventos de status update
    if payload.event != "taskStatusUpdated":
        logger.debug("Evento '%s' ignorado (não é status update).", payload.event)
        return {"status": "ignored", "reason": "not a status update"}

    if not payload.task_id:
        logger.warning("Evento taskStatusUpdated sem task_id.")
        return {"status": "ignored", "reason": "no task_id"}

    new_status = payload.get_new_status()
    if not new_status:
        logger.warning("Evento de status sem novo status no history_items.")
        return {"status": "ignored", "reason": "no status in history"}

    logger.info(
        "Webhook recebido: task=%s, novo_status='%s'",
        payload.task_id, new_status,
    )

    # Processa a clonagem
    try:
        result = process_status_change(payload.task_id, new_status)

        if result:
            return {
                "status": "cloned",
                "source_task_id": payload.task_id,
                "dest_task_id": result.get("id"),
            }

        return {"status": "ignored", "reason": "not trigger or already cloned"}

    except Exception as e:
        logger.exception("Erro ao processar clone da task %s: %s", payload.task_id, e)
        return {"status": "error", "detail": str(e)}