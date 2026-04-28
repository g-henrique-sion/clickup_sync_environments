"""Monitor de webhooks do ClickUp com auto-recuperacao para ambiente Railway."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from urllib.parse import urlsplit, urlunsplit
from requests import HTTPError

from app.config.settings import (
    DATA_DIR,
    WEBHOOK_ENDPOINT,
    WEBHOOK_EXPECTED_EVENTS,
    WEBHOOK_GUARD_CREATE_IF_MISSING,
    WEBHOOK_GUARD_DELETE_DUPLICATES,
    WEBHOOK_GUARD_ENABLED,
    WEBHOOK_GUARD_FAIL_COUNT_THRESHOLD,
    WEBHOOK_GUARD_INTERVAL_SECONDS,
    WEBHOOK_GUARD_RECREATE_UNHEALTHY,
    WEBHOOK_GUARD_ROTATE_IF_SECRET_UNKNOWN,
    WEBHOOK_SECRETS,
    WEBHOOK_TEAM_IDS,
)
from app.core.clickup_client import create_team_webhook, delete_webhook_any, list_team_webhooks

logger = logging.getLogger(__name__)

_STATE_FILE = os.path.join(DATA_DIR, "webhook_guard_state.json")
_STATE_TMP_FILE = f"{_STATE_FILE}.tmp"

_guard_task: asyncio.Task | None = None
_guard_running = False

_known_webhook_ids: set[str] = set()
_known_webhook_secrets: set[str] = set()
_secret_by_webhook_id: dict[str, str] = {}

_last_check_at: float | None = None
_last_ok_at: float | None = None
_last_error: str | None = None
_total_repairs = 0


def _normalize_endpoint(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
    except Exception:
        return text.rstrip("/")

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


def _extract_webhook_id(webhook: dict) -> str:
    return str(webhook.get("id") or "").strip()


def _extract_webhook_secret(webhook: dict) -> str:
    return str(webhook.get("secret") or "").strip()


def _extract_webhook_endpoint(webhook: dict) -> str:
    return str(webhook.get("endpoint") or "").strip()


def _extract_webhook_events(webhook: dict) -> set[str]:
    raw_events = webhook.get("events") or []
    if not isinstance(raw_events, list):
        return set()
    return {str(event).strip() for event in raw_events if str(event).strip()}


def _is_webhook_active(webhook: dict) -> bool:
    status = str(webhook.get("status") or "").strip().lower()
    if not status:
        # Alguns payloads de listagem nao retornam status.
        # Nesse caso, assume ativo e valida via health/events.
        return True
    return status == "active"


def _get_webhook_fail_count(webhook: dict) -> int:
    health = webhook.get("health") or {}
    if not isinstance(health, dict):
        return 0
    try:
        return int(health.get("fail_count") or 0)
    except (TypeError, ValueError):
        return 0


def _webhook_has_expected_events(webhook: dict) -> bool:
    expected = set(WEBHOOK_EXPECTED_EVENTS)
    if not expected:
        return True
    actual = _extract_webhook_events(webhook)
    return expected.issubset(actual)


def _webhook_is_healthy(webhook: dict) -> bool:
    if not _is_webhook_active(webhook):
        return False
    if not _webhook_has_expected_events(webhook):
        return False
    return _get_webhook_fail_count(webhook) < WEBHOOK_GUARD_FAIL_COUNT_THRESHOLD


def _pick_primary_webhook(candidates: list[dict]) -> dict | None:
    if not candidates:
        return None
    healthy = [w for w in candidates if _webhook_is_healthy(w)]
    if healthy:
        return healthy[0]
    active = [w for w in candidates if _is_webhook_active(w)]
    if active:
        return active[0]
    return candidates[0]


def _load_state_file() -> dict[str, str]:
    if not os.path.exists(_STATE_FILE):
        return {}
    try:
        with open(_STATE_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    data = payload.get("secret_by_webhook_id") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return {}

    state: dict[str, str] = {}
    for webhook_id, secret in data.items():
        webhook_id_str = str(webhook_id or "").strip()
        secret_str = str(secret or "").strip()
        if webhook_id_str and secret_str:
            state[webhook_id_str] = secret_str
    return state


def _persist_state_file(secret_map: dict[str, str]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    payload = {
        "updated_at": int(time.time()),
        "secret_by_webhook_id": secret_map,
    }
    with open(_STATE_TMP_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(_STATE_TMP_FILE, _STATE_FILE)


def _collect_known_secret(webhook: dict) -> str:
    webhook_id = _extract_webhook_id(webhook)
    secret = _extract_webhook_secret(webhook)
    if secret:
        return secret
    return _secret_by_webhook_id.get(webhook_id, "")


def _is_webhook_already_exists_error(exc: Exception) -> bool:
    if not isinstance(exc, HTTPError) or exc.response is None:
        return False
    if exc.response.status_code != 400:
        return False

    body_text = (exc.response.text or "").lower()
    if "already exists" in body_text:
        return True

    try:
        payload = exc.response.json() or {}
    except Exception:
        payload = {}
    ecode = str(payload.get("ECODE") or payload.get("ecode") or "").upper()
    return ecode == "OAUTH_171"


def _repair_team_webhook_sync(team_id: str) -> tuple[set[str], set[str], int]:
    team_repairs = 0
    known_ids: set[str] = set()
    known_secrets: set[str] = set()
    normalized_target_endpoint = _normalize_endpoint(WEBHOOK_ENDPOINT)

    all_webhooks = list_team_webhooks(team_id)
    candidates = [
        webhook
        for webhook in all_webhooks
        if _normalize_endpoint(_extract_webhook_endpoint(webhook))
        == normalized_target_endpoint
    ]

    primary = _pick_primary_webhook(candidates)

    if primary is None and WEBHOOK_GUARD_CREATE_IF_MISSING:
        try:
            created = create_team_webhook(team_id, WEBHOOK_ENDPOINT, WEBHOOK_EXPECTED_EVENTS)
            created_id = _extract_webhook_id(created)
            if created_id:
                team_repairs += 1
                logger.warning(
                    "webhook_guard.repair criado_novo team=%s webhook_id=%s",
                    team_id,
                    created_id,
                )
                candidates.append(created)
                primary = created
        except Exception as e:
            if _is_webhook_already_exists_error(e):
                logger.debug(
                    "webhook_guard.create_skip team=%s motivo=already_exists",
                    team_id,
                )
                refreshed = list_team_webhooks(team_id)
                candidates = [
                    webhook
                    for webhook in refreshed
                    if _normalize_endpoint(_extract_webhook_endpoint(webhook))
                    == normalized_target_endpoint
                ]
                primary = _pick_primary_webhook(candidates)
            else:
                raise

    if primary is not None:
        primary_id = _extract_webhook_id(primary)
        unknown_secret = not _collect_known_secret(primary)
        unhealthy = not _webhook_is_healthy(primary)
        requires_recreate = False
        reason = ""

        if WEBHOOK_GUARD_RECREATE_UNHEALTHY and unhealthy:
            requires_recreate = True
            reason = f"unhealthy fail_count={_get_webhook_fail_count(primary)} status={primary.get('status')}"
        elif (
            WEBHOOK_GUARD_ROTATE_IF_SECRET_UNKNOWN
            and unknown_secret
            and not WEBHOOK_SECRETS
        ):
            requires_recreate = True
            reason = "secret_unknown"

        if requires_recreate:
            try:
                created = create_team_webhook(team_id, WEBHOOK_ENDPOINT, WEBHOOK_EXPECTED_EVENTS)
                created_id = _extract_webhook_id(created)
                if created_id:
                    team_repairs += 1
                    logger.warning(
                        "webhook_guard.repair recriado team=%s old_webhook_id=%s new_webhook_id=%s reason=%s",
                        team_id,
                        primary_id,
                        created_id,
                        reason,
                    )
                    candidates.append(created)
                    primary = created
            except Exception as e:
                if _is_webhook_already_exists_error(e):
                    logger.debug(
                        "webhook_guard.recreate_skip team=%s motivo=already_exists reason=%s",
                        team_id,
                        reason,
                    )
                    refreshed = list_team_webhooks(team_id)
                    candidates = [
                        webhook
                        for webhook in refreshed
                        if _normalize_endpoint(_extract_webhook_endpoint(webhook))
                        == normalized_target_endpoint
                    ]
                    primary = _pick_primary_webhook(candidates)
                else:
                    raise

        primary_id = _extract_webhook_id(primary)
        primary_secret = _collect_known_secret(primary)
        if primary_id:
            known_ids.add(primary_id)
            if primary_secret:
                known_secrets.add(primary_secret)
                _secret_by_webhook_id[primary_id] = primary_secret

        if WEBHOOK_GUARD_DELETE_DUPLICATES:
            for webhook in candidates:
                webhook_id = _extract_webhook_id(webhook)
                if not webhook_id or webhook_id == primary_id:
                    continue
                delete_webhook_any(webhook_id)
                team_repairs += 1
                _secret_by_webhook_id.pop(webhook_id, None)
                logger.warning(
                    "webhook_guard.repair removido_duplicado team=%s webhook_id=%s",
                    team_id,
                    webhook_id,
                )

    return known_ids, known_secrets, team_repairs


def _repair_all_webhooks_sync() -> tuple[set[str], set[str], int, list[str]]:
    all_ids: set[str] = set()
    all_secrets: set[str] = set()
    repairs = 0
    errors: list[str] = []

    for team_id in WEBHOOK_TEAM_IDS:
        team = str(team_id or "").strip()
        if not team:
            continue
        try:
            team_ids, team_secrets, team_repairs = _repair_team_webhook_sync(team)
            all_ids.update(team_ids)
            all_secrets.update(team_secrets)
            repairs += team_repairs
        except Exception as e:
            detail = str(e)
            if isinstance(e, HTTPError) and e.response is not None:
                body = (e.response.text or "").strip().replace("\n", " ")
                if body:
                    detail = f"{detail} body={body[:300]}"
            errors.append(f"{team}: {detail}")
            logger.warning(
                "webhook_guard.team_erro team=%s erro=%s",
                team,
                detail,
            )
    return all_ids, all_secrets, repairs, errors


async def _run_guard_cycle_once() -> None:
    global _known_webhook_ids
    global _known_webhook_secrets
    global _last_check_at
    global _last_ok_at
    global _last_error
    global _total_repairs

    _last_check_at = time.time()
    try:
        ids, secrets, repairs, errors = await asyncio.to_thread(_repair_all_webhooks_sync)
        _known_webhook_ids = ids
        _known_webhook_secrets = secrets
        _total_repairs += repairs
        _persist_state_file(_secret_by_webhook_id)
        _last_ok_at = time.time()
        _last_error = "; ".join(errors) if errors else None
        if repairs:
            logger.warning(
                "webhook_guard.ok repairs=%d known_webhooks=%d",
                repairs,
                len(_known_webhook_ids),
            )
        else:
            logger.debug(
                "webhook_guard.ok repairs=0 known_webhooks=%d",
                len(_known_webhook_ids),
            )
    except Exception as e:
        _last_error = str(e)
        logger.exception("webhook_guard.erro ciclo=%s", _last_check_at)


async def _guard_loop() -> None:
    while _guard_running:
        await asyncio.sleep(WEBHOOK_GUARD_INTERVAL_SECONDS)
        await _run_guard_cycle_once()


async def start_webhook_guard() -> None:
    """Inicia monitor em background para manter webhook sempre ativo."""
    global _guard_task
    global _guard_running

    if _guard_running:
        return

    if not WEBHOOK_GUARD_ENABLED:
        logger.info("webhook_guard desativado por configuracao.")
        return

    if not WEBHOOK_ENDPOINT:
        logger.warning("webhook_guard desativado: WEBHOOK_ENDPOINT nao configurado.")
        return

    if not WEBHOOK_TEAM_IDS:
        logger.warning("webhook_guard desativado: WEBHOOK_TEAM_IDS vazio.")
        return

    _secret_by_webhook_id.update(_load_state_file())

    _guard_running = True
    await _run_guard_cycle_once()
    _guard_task = asyncio.create_task(_guard_loop(), name="webhook-guard")
    logger.info(
        "webhook_guard iniciado endpoint=%s interval_s=%d teams=%d",
        WEBHOOK_ENDPOINT,
        WEBHOOK_GUARD_INTERVAL_SECONDS,
        len(WEBHOOK_TEAM_IDS),
    )


async def stop_webhook_guard() -> None:
    """Encerra monitor de webhook."""
    global _guard_task
    global _guard_running

    if not _guard_running:
        return

    _guard_running = False
    if _guard_task:
        _guard_task.cancel()
        try:
            await _guard_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Erro ao finalizar webhook_guard.")
    _guard_task = None
    logger.debug("webhook_guard finalizado.")


def get_runtime_webhook_secrets() -> set[str]:
    """Segredos ativos conhecidos pelo monitor (em memoria)."""
    return set(_known_webhook_secrets)


def get_webhook_guard_stats() -> dict:
    """Metricas do monitor para /health."""
    return {
        "webhook_guard_enabled": WEBHOOK_GUARD_ENABLED,
        "webhook_guard_running": _guard_running,
        "webhook_guard_endpoint": WEBHOOK_ENDPOINT,
        "webhook_guard_teams": WEBHOOK_TEAM_IDS,
        "webhook_guard_known_webhooks": len(_known_webhook_ids),
        "webhook_guard_known_secrets": len(_known_webhook_secrets),
        "webhook_guard_total_repairs": _total_repairs,
        "webhook_guard_last_check_at": _last_check_at,
        "webhook_guard_last_ok_at": _last_ok_at,
        "webhook_guard_last_error": _last_error,
    }
