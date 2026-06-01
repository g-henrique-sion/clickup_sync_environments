"""Cliente ClickUp â€” leitura (origem) e escrita (destino)."""

import json
import logging
import os
import random
import re
import tempfile
import time
import unicodedata
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter

from app.config.settings import (
    CLICKUP_HTTP_BACKOFF_SECONDS,
    CLICKUP_HTTP_CONNECT_TIMEOUT_SECONDS,
    CLICKUP_HTTP_MAX_BACKOFF_SECONDS,
    CLICKUP_HTTP_MAX_RETRIES,
    CLICKUP_HTTP_POOL_CONNECTIONS,
    CLICKUP_HTTP_POOL_MAXSIZE,
    CLICKUP_HTTP_READ_TIMEOUT_SECONDS,
    CLONE_FIELD_MAP,
    DEST_CLICKUP_TOKEN,
    DEST_LIST_ID,
    DEST_WORKSPACE_ID,
    ENV_SYNC_USE_DIRECT_FIELDS,
    SOURCE_LIST_ID,
    SOURCE_CLICKUP_TOKEN,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.clickup.com/api/v2"
BASE_URL_V3 = "https://api.clickup.com/api/v3"
ATTACHMENT_CUSTOM_FIELD_IDS = {
    "18662b08-1a0f-4a43-8f1d-e7e2209d87d3",
    "8a6a1f6e-cf70-42ed-b96e-ed18ee85c115",
    "3375a419-601c-46e2-b08e-c769fddce71e",
    "1019e5f0-9810-4241-98ba-9c0ed57e95b2",
}


def task_permalink(task_id: str | None) -> str:
    task = str(task_id or "").strip()
    return f"https://app.clickup.com/t/{task}" if task else ""

# â”€â”€ SessÃµes HTTP reutilizÃ¡veis â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_source_session: requests.Session | None = None
_dest_session: requests.Session | None = None
_source_workspace_id: str | None = None
_dest_workspace_id: str | None = None
_dest_attachment_field_name_to_id: dict[str, str] | None = None
_source_attachment_field_name_to_id: dict[str, str] | None = None
_source_list_fields_cache: list[dict] | None = None
_dest_list_fields_cache: list[dict] | None = None
_source_to_dest_field_map_cache: dict[str, str] | None = None
_dest_to_source_field_map_cache: dict[str, str] | None = None

MAX_RETRIES = CLICKUP_HTTP_MAX_RETRIES
RETRY_BACKOFF = CLICKUP_HTTP_BACKOFF_SECONDS
MAX_RETRY_BACKOFF = CLICKUP_HTTP_MAX_BACKOFF_SECONDS
TIMEOUT = (CLICKUP_HTTP_CONNECT_TIMEOUT_SECONDS, CLICKUP_HTTP_READ_TIMEOUT_SECONDS)


def _configure_session(session: requests.Session) -> None:
    adapter = HTTPAdapter(
        pool_connections=CLICKUP_HTTP_POOL_CONNECTIONS,
        pool_maxsize=CLICKUP_HTTP_POOL_MAXSIZE,
        max_retries=0,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)


def _compute_retry_wait_seconds(attempt: int, fallback_seconds: float | None = None) -> float:
    if fallback_seconds is not None:
        return max(0.0, min(float(fallback_seconds), MAX_RETRY_BACKOFF))
    base = RETRY_BACKOFF * (2 ** max(0, attempt - 1))
    jitter = random.uniform(0.0, base * 0.25)
    return max(0.0, min(base + jitter, MAX_RETRY_BACKOFF))


def _webhook_sessions() -> list[requests.Session]:
    sessions: list[requests.Session] = []
    if SOURCE_CLICKUP_TOKEN:
        sessions.append(_get_source_session())
    if DEST_CLICKUP_TOKEN and DEST_CLICKUP_TOKEN != SOURCE_CLICKUP_TOKEN:
        sessions.append(_get_dest_session())
    return sessions


def _request_webhook_with_fallback(method: str, url: str, **kwargs) -> requests.Response:
    last_error: Exception | None = None
    for session in _webhook_sessions():
        try:
            return _request_with_retry(session, method, url, **kwargs)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            last_error = e
            if status in {401, 403, 404}:
                continue
            raise
        except Exception as e:
            last_error = e
            continue

    if last_error:
        raise last_error
    raise RuntimeError("Nenhum token disponivel para operacoes de webhook.")


def _get_source_session() -> requests.Session:
    global _source_session
    if _source_session is None:
        _source_session = requests.Session()
        _configure_session(_source_session)
        _source_session.headers.update({"Authorization": SOURCE_CLICKUP_TOKEN})
    return _source_session


def _get_dest_session() -> requests.Session:
    global _dest_session
    if _dest_session is None:
        _dest_session = requests.Session()
        _configure_session(_dest_session)
        _dest_session.headers.update({"Authorization": DEST_CLICKUP_TOKEN})
    return _dest_session


def _get_source_workspace_id() -> str:
    """Resolve o workspace (team_id) de origem usando SOURCE_LIST_ID."""
    global _source_workspace_id
    if _source_workspace_id:
        return _source_workspace_id

    session = _get_source_session()
    list_resp = _request_with_retry(session, "GET", f"{BASE_URL}/list/{SOURCE_LIST_ID}")
    list_data = list_resp.json()
    space_id = (list_data.get("space") or {}).get("id")

    teams_resp = _request_with_retry(session, "GET", f"{BASE_URL}/team")
    teams = teams_resp.json().get("teams", [])

    if space_id:
        for team in teams:
            team_id = team.get("id")
            if not team_id:
                continue
            spaces_resp = _request_with_retry(
                session, "GET", f"{BASE_URL}/team/{team_id}/space"
            )
            spaces = spaces_resp.json().get("spaces", [])
            if any(s.get("id") == space_id for s in spaces):
                _source_workspace_id = str(team_id)
                return _source_workspace_id

    if len(teams) == 1 and teams[0].get("id"):
        _source_workspace_id = str(teams[0].get("id"))
        return _source_workspace_id

    raise RuntimeError("Nao foi possivel resolver o workspace de origem.")


def _get_dest_workspace_id() -> str:
    """Resolve o workspace (team_id) de destino usando a lista de destino."""
    global _dest_workspace_id
    if _dest_workspace_id:
        return _dest_workspace_id

    if DEST_WORKSPACE_ID:
        _dest_workspace_id = DEST_WORKSPACE_ID
        return _dest_workspace_id

    session = _get_dest_session()
    list_resp = _request_with_retry(session, "GET", f"{BASE_URL}/list/{DEST_LIST_ID}")
    list_data = list_resp.json()
    space_id = (list_data.get("space") or {}).get("id")

    teams_resp = _request_with_retry(session, "GET", f"{BASE_URL}/team")
    teams = teams_resp.json().get("teams", [])

    if space_id:
        for team in teams:
            team_id = team.get("id")
            if not team_id:
                continue
            spaces_resp = _request_with_retry(
                session, "GET", f"{BASE_URL}/team/{team_id}/space"
            )
            spaces = spaces_resp.json().get("spaces", [])
            if any(s.get("id") == space_id for s in spaces):
                _dest_workspace_id = str(team_id)
                return _dest_workspace_id

    if len(teams) == 1 and teams[0].get("id"):
        _dest_workspace_id = str(teams[0].get("id"))
        return _dest_workspace_id

    raise RuntimeError(
        "NÃƒÂ£o foi possÃƒÂ­vel resolver o workspace de destino. "
        "Defina DEST_WORKSPACE_ID no .env."
    )


def _request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    **kwargs,
) -> requests.Response:
    """Executa request com retry/backoff para erros transientes de rede/API."""
    kwargs.setdefault("timeout", TIMEOUT)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(method, url, **kwargs)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                parsed_retry_after: float | None = None
                if retry_after:
                    try:
                        parsed_retry_after = float(retry_after)
                    except (TypeError, ValueError):
                        parsed_retry_after = None
                wait = _compute_retry_wait_seconds(attempt, fallback_seconds=parsed_retry_after)
                logger.warning(
                    "ClickUp rate limit 429. tentativa=%d/%d wait_s=%.2f",
                    attempt,
                    MAX_RETRIES,
                    wait,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(wait)
                    continue

            if resp.status_code in {408, 409, 423, 425} or resp.status_code >= 500:
                wait = _compute_retry_wait_seconds(attempt)
                logger.warning(
                    "ClickUp erro transiente status=%d tentativa=%d/%d wait_s=%.2f",
                    resp.status_code,
                    attempt,
                    MAX_RETRIES,
                    wait,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(wait)
                    continue

            resp.raise_for_status()
            return resp

        except requests.exceptions.Timeout:
            wait = _compute_retry_wait_seconds(attempt)
            logger.warning(
                "Timeout ClickUp tentativa=%d/%d wait_s=%.2f method=%s url=%s",
                attempt,
                MAX_RETRIES,
                wait,
                method,
                url,
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait)
                continue
            raise
        except requests.exceptions.ConnectionError as e:
            wait = _compute_retry_wait_seconds(attempt)
            logger.warning(
                "ConnectionError ClickUp tentativa=%d/%d wait_s=%.2f method=%s url=%s erro=%s",
                attempt,
                MAX_RETRIES,
                wait,
                method,
                url,
                e,
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait)
                continue
            raise

    raise requests.exceptions.ConnectionError(
        f"Falha apos {MAX_RETRIES} tentativas: {method} {url}"
    )


def list_team_webhooks(team_id: str) -> list[dict]:
    """Lista webhooks de um workspace (team) usando token de origem."""
    resp = _request_webhook_with_fallback(
        "GET",
        f"{BASE_URL}/team/{team_id}/webhook",
    )
    payload = resp.json() if resp.content else {}
    return (payload or {}).get("webhooks", []) or []


def create_team_webhook(
    team_id: str,
    endpoint: str,
    events: list[str] | None = None,
) -> dict:
    """Cria webhook em um workspace (team) usando token de origem."""
    payload = {
        "endpoint": endpoint,
        "events": events or ["taskStatusUpdated"],
        "status": "active",
    }
    resp = _request_webhook_with_fallback(
        "POST",
        f"{BASE_URL}/team/{team_id}/webhook",
        json=payload,
    )
    data = resp.json() if resp.content else {}
    webhook = (data or {}).get("webhook", data or {})
    return webhook or {}


def delete_webhook_any(webhook_id: str) -> None:
    """Remove webhook pelo ID usando token de origem."""
    _request_webhook_with_fallback("DELETE", f"{BASE_URL}/webhook/{webhook_id}")


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# LEITURA â€” Workspace de origem
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


def fetch_task(task_id: str) -> dict:
    """Busca dados completos de uma task na workspace de origem."""
    session = _get_source_session()
    url = f"{BASE_URL}/task/{task_id}"
    resp = _request_with_retry(session, "GET", url, params={"include_subtasks": "true"})
    return resp.json()


def fetch_task_from_dest(task_id: str) -> dict:
    """Busca dados completos de uma task na workspace de destino."""
    session = _get_dest_session()
    url = f"{BASE_URL}/task/{task_id}"
    resp = _request_with_retry(session, "GET", url, params={"include_subtasks": "true"})
    return resp.json()


def update_task_status_in_dest(task_id: str, status: str) -> dict:
    """Atualiza o status de uma task no workspace de destino."""
    session = _get_dest_session()
    url = f"{BASE_URL}/task/{task_id}"
    resp = _request_with_retry(session, "PUT", url, json={"status": status})
    return resp.json()


def update_task_status_in_source(task_id: str, status: str) -> dict:
    """Atualiza o status de uma task no workspace de origem."""
    session = _get_source_session()
    url = f"{BASE_URL}/task/{task_id}"
    resp = _request_with_retry(session, "PUT", url, json={"status": status})
    return resp.json()


def update_task_status_any(task_id: str, status: str) -> dict:
    """Atualiza status tentando token de destino e fallback para origem."""
    try:
        return update_task_status_in_dest(task_id, status)
    except Exception:
        return update_task_status_in_source(task_id, status)


def get_custom_field_value(task: dict, cf_id: str):
    """Extrai o valor de um custom field de uma task."""
    for cf in task.get("custom_fields", []):
        if cf.get("id") == cf_id:
            return cf.get("value")
    return None




def _normalize_custom_field_value(value) -> str:
    """Normaliza valores de custom field para comparacao de dedup."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip().lower()
    if isinstance(value, (int, float, bool)):
        return str(value).strip().lower()
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value, sort_keys=True, ensure_ascii=False).strip().lower()
        except Exception:
            return str(value).strip().lower()
    return str(value).strip().lower()


def find_task_in_dest_by_uc_and_name(
    dest_name_field_id: str,
    name_value,
    dest_uc_field_id: str,
    uc_value,
) -> dict | None:
    """Busca task no destino com mesmo Nome Fantasia + UC antiga."""
    expected_name = _normalize_custom_field_value(name_value)
    expected_uc = _normalize_custom_field_value(uc_value)
    if not expected_name or not expected_uc:
        return None

    session = _get_dest_session()
    page = 0
    while True:
        params = {"include_closed": "true", "page": page}
        resp = _request_with_retry(
            session,
            "GET",
            f"{BASE_URL}/list/{DEST_LIST_ID}/task",
            params=params,
        )
        data = resp.json()
        tasks = data.get("tasks", []) or []

        for task in tasks:
            task_name_value = get_custom_field_value(task, dest_name_field_id)
            task_uc_value = get_custom_field_value(task, dest_uc_field_id)
            if (
                _normalize_custom_field_value(task_name_value) == expected_name
                and _normalize_custom_field_value(task_uc_value) == expected_uc
            ):
                return task

        if data.get("last_page") is True or not tasks:
            break
        page += 1

    return None
def _is_file_custom_field(cf: dict) -> bool:
    field_type = (cf.get("type") or "").lower()
    return field_type in {"attachment", "file", "files", "file_attachment", "file_upload"}


def _select_attachment_url(att: dict) -> str | None:
    for key in ("download_url", "url_w_query", "url"):
        val = att.get(key)
        if val:
            return val
    return None


def _guess_attachment_filename(att: dict) -> str:
    name = att.get("title") or att.get("name") or att.get("filename") or "arquivo"
    ext = att.get("extension")
    if ext and not name.lower().endswith(f".{ext.lower()}"):
        name = f"{name}.{ext}"
    return name


def _build_custom_field_attachment_filename(cf: dict, task_name: str, item: dict) -> str:
    """Gera nome do arquivo como: nome do campo - nome da task."""
    field_name = str(cf.get("name") or cf.get("id") or "campo").strip()
    task_name = str(task_name or "Sem nome").strip()
    base_name = f"{field_name} - {task_name}"

    original_name = _guess_attachment_filename(item)
    _, ext = os.path.splitext(original_name)
    return f"{base_name}{ext}" if ext else base_name


def _normalize_field_name(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _normalize_field_type(value: str | None) -> str:
    field_type = str(value or "").strip().lower()
    alias = {
        "dropdown": "drop_down",
        "text": "short_text",
        "textarea": "short_text",
        "long_text": "short_text",
        "website": "url",
        "money": "currency",
        "file": "attachment",
        "files": "attachment",
        "file_attachment": "attachment",
        "file_upload": "attachment",
    }
    return alias.get(field_type, field_type)


def _is_writable_custom_field_type(field_type: str) -> bool:
    blocked = {"formula", "rollup", "progress", "automatic_progress", "button"}
    return field_type not in blocked


def _are_field_types_compatible(source_field: dict, dest_field: dict) -> bool:
    source_type = _normalize_field_type(source_field.get("type"))
    dest_type = _normalize_field_type(dest_field.get("type"))
    if not source_type or not dest_type:
        return False
    if source_type == dest_type:
        return True
    # Algumas listas antigas variam entre text/short_text.
    if {source_type, dest_type} <= {"short_text", "text", "textarea"}:
        return True
    return False


def _get_source_list_fields() -> list[dict]:
    global _source_list_fields_cache
    if _source_list_fields_cache is not None:
        return _source_list_fields_cache
    session = _get_source_session()
    resp = _request_with_retry(session, "GET", f"{BASE_URL}/list/{SOURCE_LIST_ID}/field")
    _source_list_fields_cache = (resp.json() or {}).get("fields", []) or []
    return _source_list_fields_cache


def _get_dest_list_fields() -> list[dict]:
    global _dest_list_fields_cache
    if _dest_list_fields_cache is not None:
        return _dest_list_fields_cache
    session = _get_dest_session()
    resp = _request_with_retry(session, "GET", f"{BASE_URL}/list/{DEST_LIST_ID}/field")
    _dest_list_fields_cache = (resp.json() or {}).get("fields", []) or []
    return _dest_list_fields_cache


def _get_source_to_dest_field_map() -> dict[str, str]:
    """Mapeia source_cf_id -> dest_cf_id combinando map explícito + auto por nome/tipo."""
    global _source_to_dest_field_map_cache
    if _source_to_dest_field_map_cache is not None:
        return _source_to_dest_field_map_cache

    source_fields = _get_source_list_fields()
    dest_fields = _get_dest_list_fields()
    source_by_id = {str(f.get("id") or ""): f for f in source_fields if f.get("id")}
    dest_by_id = {str(f.get("id") or ""): f for f in dest_fields if f.get("id")}

    if ENV_SYNC_USE_DIRECT_FIELDS:
        source_ids = set(source_by_id.keys())
        dest_ids = set(dest_by_id.keys())
        shared_ids = sorted(source_ids.intersection(dest_ids))
        mapping = {field_id: field_id for field_id in shared_ids}
        _source_to_dest_field_map_cache = mapping
        logger.info(
            "Mapeamento source->dest (modo direto) carregado: total=%d",
            len(mapping),
        )
        return mapping

    dest_by_name: dict[str, list[dict]] = {}
    for field in dest_fields:
        key = _normalize_field_name(field.get("name"))
        if not key:
            continue
        dest_by_name.setdefault(key, []).append(field)

    mapping: dict[str, str] = {}

    # 1) Respeita mapeamento explícito do .env
    for src_cf_id, dest_cf_id in CLONE_FIELD_MAP.items():
        src_id = str(src_cf_id or "").strip()
        dst_id = str(dest_cf_id or "").strip()
        if not src_id or not dst_id:
            continue
        if src_id not in source_by_id:
            logger.debug("Mapeamento ignorado (source ausente): %s -> %s", src_id, dst_id)
            continue
        if dst_id not in dest_by_id:
            logger.debug("Mapeamento ignorado (dest ausente): %s -> %s", src_id, dst_id)
            continue
        mapping[src_id] = dst_id

    # 2) Auto-mapeia campos faltantes por nome/tipo
    for src_id, source_field in source_by_id.items():
        if src_id in mapping:
            continue
        source_type = _normalize_field_type(source_field.get("type"))
        if not _is_writable_custom_field_type(source_type):
            continue

        key = _normalize_field_name(source_field.get("name"))
        if not key:
            continue
        candidates = [
            c
            for c in dest_by_name.get(key, [])
            if _are_field_types_compatible(source_field, c)
            and _is_writable_custom_field_type(_normalize_field_type(c.get("type")))
        ]
        if len(candidates) == 1:
            mapping[src_id] = str(candidates[0].get("id"))
        elif len(candidates) > 1:
            # Evita ambiguidade silenciosa; escolhe determinístico e loga.
            ordered = sorted(candidates, key=lambda c: str(c.get("id") or ""))
            chosen = str(ordered[0].get("id"))
            mapping[src_id] = chosen
            logger.warning(
                "Auto-mapeamento ambíguo para campo '%s' (%s). Escolhido dest=%s.",
                source_field.get("name"),
                src_id,
                chosen,
            )

    _source_to_dest_field_map_cache = mapping
    logger.info(
        "Mapeamento source->dest carregado: total=%d (explicito=%d, auto=%d)",
        len(mapping),
        len(
            {
                k: v
                for k, v in mapping.items()
                if str(k).strip() in CLONE_FIELD_MAP and CLONE_FIELD_MAP.get(str(k).strip())
            }
        ),
        max(0, len(mapping) - len(CLONE_FIELD_MAP)),
    )
    return mapping


def _get_dest_to_source_field_map() -> dict[str, str]:
    """Mapeia dest_cf_id -> source_cf_id (inversão do map calculado)."""
    global _dest_to_source_field_map_cache
    if _dest_to_source_field_map_cache is not None:
        return _dest_to_source_field_map_cache

    reverse: dict[str, str] = {}
    for source_id, dest_id in _get_source_to_dest_field_map().items():
        if source_id and dest_id:
            reverse[str(dest_id)] = str(source_id)
    _dest_to_source_field_map_cache = reverse
    return reverse


def _extract_field_name_from_attachment_filename(filename: str) -> str:
    name = str(filename or "").strip()
    if " - " not in name:
        return ""
    return _normalize_field_name(name.split(" - ", 1)[0])


def _get_dest_attachment_field_name_map() -> dict[str, str]:
    """Mapeia nome do campo de anexo (destino) -> id do campo."""
    global _dest_attachment_field_name_to_id
    if _dest_attachment_field_name_to_id is not None:
        return _dest_attachment_field_name_to_id

    if ENV_SYNC_USE_DIRECT_FIELDS:
        fields = _get_dest_list_fields()
        mapping: dict[str, str] = {}
        for field in fields:
            if not _is_file_custom_field(field):
                continue
            field_id = str(field.get("id") or "").strip()
            if not field_id:
                continue
            key = _normalize_field_name(field.get("name"))
            if key:
                mapping[key] = field_id
        _dest_attachment_field_name_to_id = mapping
        return mapping

    source_fields = {str(f.get("id")): f for f in _get_source_list_fields() if f.get("id")}
    source_to_dest = _get_source_to_dest_field_map()
    target_dest_ids = {
        str(dest_id)
        for source_id, dest_id in source_to_dest.items()
        if _is_file_custom_field({"type": source_fields.get(str(source_id), {}).get("type")})
    }

    fields = _get_dest_list_fields()

    mapping: dict[str, str] = {}
    for field in fields:
        field_id = str(field.get("id") or "")
        if field_id not in target_dest_ids:
            continue
        key = _normalize_field_name(field.get("name"))
        if key:
            mapping[key] = field_id

    _dest_attachment_field_name_to_id = mapping
    return mapping


def _get_source_attachment_field_name_map() -> dict[str, str]:
    """Mapeia nome do campo de anexo (source) -> id do campo."""
    global _source_attachment_field_name_to_id
    if _source_attachment_field_name_to_id is not None:
        return _source_attachment_field_name_to_id

    if ENV_SYNC_USE_DIRECT_FIELDS:
        fields = _get_source_list_fields()
        mapping: dict[str, str] = {}
        for field in fields:
            if not _is_file_custom_field(field):
                continue
            field_id = str(field.get("id") or "").strip()
            if not field_id:
                continue
            key = _normalize_field_name(field.get("name"))
            if key:
                mapping[key] = field_id
        _source_attachment_field_name_to_id = mapping
        return mapping

    source_to_dest = _get_source_to_dest_field_map()
    target_source_ids = set(source_to_dest.keys())
    fields = _get_source_list_fields()

    mapping: dict[str, str] = {}
    for field in fields:
        field_id = str(field.get("id") or "")
        if field_id not in target_source_ids:
            continue
        if not _is_file_custom_field(field):
            continue
        key = _normalize_field_name(field.get("name"))
        if key:
            mapping[key] = field_id

    _source_attachment_field_name_to_id = mapping
    return mapping


def _download_attachment_to_temp(url: str, filename: str) -> tuple[str, str]:
    session = _get_source_session()
    resp = _request_with_retry(session, "GET", url, stream=True)
    content_type = resp.headers.get("Content-Type") or "application/octet-stream"

    _, ext = os.path.splitext(filename)
    fd, path = tempfile.mkstemp(prefix="clickup_attach_", suffix=ext)
    with os.fdopen(fd, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return path, content_type


def _download_attachment_to_temp_from_dest(url: str, filename: str) -> tuple[str, str]:
    session = _get_dest_session()
    resp = _request_with_retry(session, "GET", url, stream=True)
    content_type = resp.headers.get("Content-Type") or "application/octet-stream"

    _, ext = os.path.splitext(filename)
    fd, path = tempfile.mkstemp(prefix="clickup_attach_", suffix=ext)
    with os.fdopen(fd, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return path, content_type


def _upload_task_attachment(dest_task_id: str, file_path: str, filename: str, content_type: str) -> bool:
    session = _get_dest_session()
    url = f"{BASE_URL}/task/{dest_task_id}/attachment"
    with open(file_path, "rb") as f:
        for key in ("attachment", "file", "attachment[0]"):
            f.seek(0)
            files = {key: (filename, f, content_type)}
            try:
                _request_with_retry(session, "POST", url, files=files)
                return True
            except requests.HTTPError as e:
                resp = e.response
                detail = resp.text if resp is not None else str(e)
                logger.warning("Falha ao enviar anexo (%s): %s", key, detail)
    return False


def _upload_task_attachment_to_source(
    source_task_id: str, file_path: str, filename: str, content_type: str
) -> bool:
    session = _get_source_session()
    url = f"{BASE_URL}/task/{source_task_id}/attachment"
    with open(file_path, "rb") as f:
        for key in ("attachment", "file", "attachment[0]"):
            f.seek(0)
            files = {key: (filename, f, content_type)}
            try:
                _request_with_retry(session, "POST", url, files=files)
                return True
            except requests.HTTPError as e:
                resp = e.response
                detail = resp.text if resp is not None else str(e)
                logger.warning("Falha ao enviar anexo para source (%s): %s", key, detail)
    return False


def _upload_custom_field_attachment(
    dest_field_id: str, file_path: str, filename: str, content_type: str
) -> str | None:
    session = _get_dest_session()
    workspace_id = _get_dest_workspace_id()
    url = f"{BASE_URL_V3}/workspaces/{workspace_id}/custom_fields/{dest_field_id}/attachments"
    with open(file_path, "rb") as f:
        for key in ("attachment", "file", "attachment[0]"):
            f.seek(0)
            files = {key: (filename, f, content_type)}
            try:
                resp = _request_with_retry(session, "POST", url, files=files)
                break
            except requests.HTTPError as e:
                resp_err = e.response
                detail = resp_err.text if resp_err is not None else str(e)
                logger.warning(
                    "Falha ao enviar anexo de custom field (%s): %s", key, detail
                )
        else:
            return None
    data = resp.json()
    if isinstance(data, dict):
        return data.get("id") or (data.get("attachment") or {}).get("id")
    return None


def _upload_custom_field_attachment_to_source(
    source_field_id: str, file_path: str, filename: str, content_type: str
) -> str | None:
    session = _get_source_session()
    workspace_id = _get_source_workspace_id()
    url = f"{BASE_URL_V3}/workspaces/{workspace_id}/custom_fields/{source_field_id}/attachments"
    with open(file_path, "rb") as f:
        for key in ("attachment", "file", "attachment[0]"):
            f.seek(0)
            files = {key: (filename, f, content_type)}
            try:
                resp = _request_with_retry(session, "POST", url, files=files)
                break
            except requests.HTTPError as e:
                resp_err = e.response
                detail = resp_err.text if resp_err is not None else str(e)
                logger.warning(
                    "Falha ao enviar anexo de custom field source (%s): %s",
                    key,
                    detail,
                )
        else:
            return None
    data = resp.json()
    if isinstance(data, dict):
        return data.get("id") or (data.get("attachment") or {}).get("id")
    return None


def _set_custom_field_value(task_id: str, field_id: str, value) -> None:
    session = _get_dest_session()
    url = f"{BASE_URL}/task/{task_id}/field/{field_id}"
    _request_with_retry(session, "POST", url, json={"value": value})


def _set_custom_field_value_in_source(task_id: str, field_id: str, value) -> None:
    session = _get_source_session()
    url = f"{BASE_URL}/task/{task_id}/field/{field_id}"
    _request_with_retry(session, "POST", url, json={"value": value})


def set_task_custom_field_any(task_id: str, field_id: str, value) -> bool:
    """Define custom field em task com fallback de token e retry para FIELD_115."""
    url = f"{BASE_URL}/task/{task_id}/field/{field_id}"
    payload = {"value": value}

    def _is_field_hierarchy_error(exc: Exception) -> bool:
        if not isinstance(exc, requests.HTTPError) or exc.response is None:
            return False
        if exc.response.status_code != 400:
            return False
        body = (exc.response.text or "").lower()
        return "field_115" in body or "task location hierarchy" in body

    last_error: Exception | None = None
    max_attempts = 6
    for attempt in range(1, max_attempts + 1):
        hierarchy_error_detected = False
        for token_label, session_factory in (
            ("dest", _get_dest_session),
            ("source", _get_source_session),
        ):
            try:
                _request_with_retry(session_factory(), "POST", url, json=payload)
                if attempt > 1:
                    logger.info(
                        "Custom field definido apos retry: task_id=%s field_id=%s attempt=%d token=%s",
                        task_id,
                        field_id,
                        attempt,
                        token_label,
                    )
                return True
            except Exception as exc:
                last_error = exc
                if _is_field_hierarchy_error(exc):
                    hierarchy_error_detected = True
                continue

        if hierarchy_error_detected and attempt < max_attempts:
            wait_s = min(8.0, 0.8 * attempt)
            logger.warning(
                "Custom field ainda indisponivel (FIELD_115). task_id=%s field_id=%s tentativa=%d/%d retry_s=%.1f",
                task_id,
                field_id,
                attempt,
                max_attempts,
                wait_s,
            )
            time.sleep(wait_s)
            continue
        break

    logger.error(
        "Falha ao definir custom field. task_id=%s field_id=%s erro=%s",
        task_id,
        field_id,
        last_error,
    )
    return False


def _update_task_with_session(
    session: requests.Session,
    task_id: str,
    payload: dict,
) -> dict:
    url = f"{BASE_URL}/task/{task_id}"
    resp = _request_with_retry(session, "PUT", url, json=payload)
    return resp.json()


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# ESCRITA â€” Workspace de destino
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


def _create_task_in_list_with_session(
    session: requests.Session,
    list_id: str,
    name: str,
    description: str | None = None,
    custom_fields: list[dict] | None = None,
    custom_item_id: int | None = None,
    status: str | None = None,
    *,
    log_label: str = "lista",
) -> dict:
    url = f"{BASE_URL}/list/{list_id}/task"

    payload: dict = {"name": name}
    if description:
        payload["description"] = description
    if custom_fields:
        payload["custom_fields"] = custom_fields
    if custom_item_id is not None:
        payload["custom_item_id"] = int(custom_item_id)
    if status:
        payload["status"] = str(status).strip()

    resp = _request_with_retry(session, "POST", url, json=payload)
    data = resp.json()
    logger.info(
        "Task criada no %s (lista %s): name='%s' id=%s link=%s",
        log_label,
        list_id,
        data.get("name"),
        data.get("id"),
        task_permalink(data.get("id")),
    )
    return data


def create_task_in_dest(
    name: str,
    description: str | None = None,
    custom_fields: list[dict] | None = None,
    custom_item_id: int | None = None,
    status: str | None = None,
) -> dict:
    """Cria uma task na lista de destino."""
    session = _get_dest_session()
    return _create_task_in_list_with_session(
        session=session,
        list_id=DEST_LIST_ID,
        name=name,
        description=description,
        custom_fields=custom_fields,
        custom_item_id=custom_item_id,
        status=status,
        log_label="destino",
    )


def create_task_in_source_list(
    list_id: str,
    name: str,
    description: str | None = None,
    custom_fields: list[dict] | None = None,
    custom_item_id: int | None = None,
    status: str | None = None,
) -> dict:
    """Cria uma task em uma lista da workspace de origem."""
    session = _get_source_session()
    return _create_task_in_list_with_session(
        session=session,
        list_id=list_id,
        name=name,
        description=description,
        custom_fields=custom_fields,
        custom_item_id=custom_item_id,
        status=status,
        log_label="source",
    )


def create_task_in_any_list(
    list_id: str,
    name: str,
    description: str | None = None,
    custom_fields: list[dict] | None = None,
    custom_item_id: int | None = None,
    status: str | None = None,
) -> dict:
    """Cria task em qualquer lista tentando token destino e fallback para source."""
    try:
        return _create_task_in_list_with_session(
            session=_get_dest_session(),
            list_id=list_id,
            name=name,
            description=description,
            custom_fields=custom_fields,
            custom_item_id=custom_item_id,
            status=status,
            log_label="token_dest",
        )
    except Exception:
        return _create_task_in_list_with_session(
            session=_get_source_session(),
            list_id=list_id,
            name=name,
            description=description,
            custom_fields=custom_fields,
            custom_item_id=custom_item_id,
            status=status,
            log_label="token_source",
        )


def set_task_custom_item_any(task_id: str, custom_item_id: int) -> dict:
    """Define o tipo da task via custom_item_id (1 = Milestone)."""
    payload = {"custom_item_id": int(custom_item_id)}
    try:
        data = _update_task_with_session(_get_dest_session(), task_id, payload)
    except Exception:
        data = _update_task_with_session(_get_source_session(), task_id, payload)
    logger.info(
        "Tipo da task atualizado: task_id=%s custom_item_id=%s",
        task_id,
        custom_item_id,
    )
    return data


def add_task_link_any(task_id: str, links_to: str) -> dict | None:
    """Cria relacionamento (Task Link) entre duas tasks."""
    path = f"{BASE_URL}/task/{task_id}/link/{links_to}"
    try:
        resp = _request_with_retry(_get_dest_session(), "POST", path)
        data = resp.json() if resp.content else {}
        logger.info("Relacionamento criado: %s <-> %s", task_id, links_to)
        return data
    except requests.HTTPError as e:
        detail = (e.response.text or "") if e.response is not None else str(e)
        if "already" in detail.lower() and "link" in detail.lower():
            logger.info("Relacionamento ja existente: %s <-> %s", task_id, links_to)
            return {}
        # fallback para token source
        try:
            resp = _request_with_retry(_get_source_session(), "POST", path)
            data = resp.json() if resp.content else {}
            logger.info("Relacionamento criado: %s <-> %s", task_id, links_to)
            return data
        except requests.HTTPError as e2:
            detail2 = (e2.response.text or "") if e2.response is not None else str(e2)
            if "already" in detail2.lower() and "link" in detail2.lower():
                logger.info("Relacionamento ja existente: %s <-> %s", task_id, links_to)
                return {}
            raise
    except Exception:
        resp = _request_with_retry(_get_source_session(), "POST", path)
        data = resp.json() if resp.content else {}
        logger.info("Relacionamento criado: %s <-> %s", task_id, links_to)
        return data


def _normalize_status_text(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


def _fetch_list_with_session(session: requests.Session, list_id: str) -> dict:
    resp = _request_with_retry(session, "GET", f"{BASE_URL}/list/{list_id}")
    return resp.json()


def get_list_fields_any(list_id: str) -> list[dict]:
    """Retorna custom fields de uma lista com fallback de token."""
    try:
        resp = _request_with_retry(_get_dest_session(), "GET", f"{BASE_URL}/list/{list_id}/field")
        return (resp.json() or {}).get("fields", []) or []
    except Exception as e_dest:
        try:
            resp = _request_with_retry(
                _get_source_session(), "GET", f"{BASE_URL}/list/{list_id}/field"
            )
            return (resp.json() or {}).get("fields", []) or []
        except Exception:
            logger.warning(
                "Falha ao ler campos da lista %s. Seguindo sem filtro de campos. erro_dest=%s",
                list_id,
                e_dest,
            )
            return []


def get_list_statuses_any(list_id: str) -> list[dict]:
    """Retorna statuses de uma lista usando token destino com fallback source."""
    try:
        data = _fetch_list_with_session(_get_dest_session(), list_id)
    except Exception:
        data = _fetch_list_with_session(_get_source_session(), list_id)
    return (data or {}).get("statuses") or []


def resolve_list_status_name_any(list_id: str, wanted_status: str) -> str | None:
    """Resolve o nome exato de status de uma lista por comparacao normalizada."""
    target = _normalize_status_text(wanted_status)
    if not target:
        return None
    for status in get_list_statuses_any(list_id):
        name = str(status.get("status") or status.get("name") or "").strip()
        if name and _normalize_status_text(name) == target:
            return name
    return None


def _pick_destination_status_name(
    target_statuses: list[dict],
    source_status_name: str | None,
) -> str | None:
    if not target_statuses:
        return None

    source_norm = _normalize_status_text(source_status_name)
    if source_norm:
        for status in target_statuses:
            name = str(status.get("status") or status.get("name") or "").strip()
            if _normalize_status_text(name) == source_norm:
                return name

    for preferred_type in ("open", "unstarted", "custom", "done", "closed"):
        for status in target_statuses:
            if str(status.get("type") or "").strip().lower() != preferred_type:
                continue
            name = str(status.get("status") or status.get("name") or "").strip()
            if name:
                return name

    for status in target_statuses:
        name = str(status.get("status") or status.get("name") or "").strip()
        if name:
            return name
    return None


def _move_task_home_list_with_session(
    *,
    session: requests.Session,
    workspace_id: str,
    task_id: str,
    target_list_id: str,
    source_status_name: str | None,
) -> dict:
    target_list_data = _fetch_list_with_session(session, target_list_id)
    target_statuses = target_list_data.get("statuses") or []
    destination_status_name = _pick_destination_status_name(target_statuses, source_status_name)
    destination_status_id = ""
    if destination_status_name:
        destination_status_norm = _normalize_status_text(destination_status_name)
        for status in target_statuses:
            status_name = str(status.get("status") or status.get("name") or "").strip()
            if _normalize_status_text(status_name) != destination_status_norm:
                continue
            destination_status_id = str(status.get("id") or "").strip()
            if destination_status_id:
                break

    payload: dict = {"move_custom_fields": True}
    if source_status_name and destination_status_name:
        payload["status_mappings"] = [
            {
                # Na pratica a API v3 aceita os nomes de status aqui.
                "source_status": str(source_status_name).strip(),
                "destination_status": str(destination_status_name).strip(),
            }
        ]

    move_url = (
        f"{BASE_URL_V3}/workspaces/{workspace_id}/tasks/{task_id}/home_list/{target_list_id}"
    )
    move_resp = None
    move_attempts = 4

    def _source_status_id() -> str:
        try:
            source_task_resp = _request_with_retry(
                session,
                "GET",
                f"{BASE_URL}/task/{task_id}",
                params={"include_subtasks": "true"},
            )
            source_task = source_task_resp.json() or {}
            return str(((source_task.get("status") or {}).get("id") or "")).strip()
        except Exception:
            return ""

    for attempt in range(1, move_attempts + 1):
        try:
            move_resp = _request_with_retry(session, "PUT", move_url, json=payload)
            break
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status == 400 and payload.get("status_mappings"):
                source_status_id = _source_status_id()
                if source_status_id and destination_status_id:
                    payload_ids = {
                        "move_custom_fields": True,
                        "status_mappings": [
                            {
                                "source_status": source_status_id,
                                "destination_status": destination_status_id,
                            }
                        ],
                    }
                    try:
                        move_resp = _request_with_retry(
                            session,
                            "PUT",
                            move_url,
                            json=payload_ids,
                        )
                        logger.info(
                            "Move home_list: fallback status_mappings por IDs aplicado task_id=%s target_list_id=%s",
                            task_id,
                            target_list_id,
                        )
                        break
                    except requests.HTTPError as exc_ids:
                        status_ids = (
                            exc_ids.response.status_code
                            if exc_ids.response is not None
                            else 0
                        )
                        if status_ids != 400:
                            raise
                # Fallback final: move sem status_mappings.
                try:
                    move_resp = _request_with_retry(
                        session,
                        "PUT",
                        move_url,
                        json={"move_custom_fields": True},
                    )
                    logger.warning(
                        "Move home_list: fallback sem status_mappings task_id=%s target_list_id=%s",
                        task_id,
                        target_list_id,
                    )
                    break
                except requests.HTTPError as exc_nomap:
                    status_nomap = (
                        exc_nomap.response.status_code
                        if exc_nomap.response is not None
                        else 0
                    )
                    if status_nomap != 400:
                        raise
            if status == 404 and attempt < move_attempts:
                wait_s = min(6.0, 1.25 * attempt)
                logger.warning(
                    "Move home_list 404 (consistencia eventual). task_id=%s target_list_id=%s tentativa=%d/%d wait_s=%.2f",
                    task_id,
                    target_list_id,
                    attempt,
                    move_attempts,
                    wait_s,
                )
                time.sleep(wait_s)
                continue
            raise

    if move_resp is None:
        raise RuntimeError(
            f"Falha ao mover task para lista apos {move_attempts} tentativas: task_id={task_id} target_list_id={target_list_id}"
        )
    move_data = move_resp.json() if move_resp.content else {}

    task_resp = _request_with_retry(
        session,
        "GET",
        f"{BASE_URL}/task/{task_id}",
        params={"include_subtasks": "true"},
    )
    task_data = task_resp.json()
    logger.info(
        "Task movida de lista: task_id=%s new_home_list_id=%s status_destino='%s'",
        task_id,
        target_list_id,
        destination_status_name or "",
    )
    if move_data:
        task_data["_move_response"] = move_data
    return task_data


def move_task_to_list_any(
    task_id: str,
    target_list_id: str,
    source_status_name: str | None = None,
) -> dict:
    """Move uma task para nova home list mantendo o mesmo ID da task."""
    try:
        return _move_task_home_list_with_session(
            session=_get_dest_session(),
            workspace_id=_get_dest_workspace_id(),
            task_id=task_id,
            target_list_id=target_list_id,
            source_status_name=source_status_name,
        )
    except Exception:
        return _move_task_home_list_with_session(
            session=_get_source_session(),
            workspace_id=_get_source_workspace_id(),
            task_id=task_id,
            target_list_id=target_list_id,
            source_status_name=source_status_name,
        )


def delete_task_in_dest(task_id: str) -> None:
    """Remove task da lista destino."""
    session = _get_dest_session()
    _request_with_retry(session, "DELETE", f"{BASE_URL}/task/{task_id}")
    logger.info("Task removida no destino: id=%s", task_id)


def delete_task_in_source(task_id: str) -> None:
    """Remove task da workspace source."""
    session = _get_source_session()
    _request_with_retry(session, "DELETE", f"{BASE_URL}/task/{task_id}")
    logger.info("Task removida no source: id=%s", task_id)


def build_custom_fields_payload(source_task: dict) -> list[dict]:
    """Monta payload de custom fields para clone origem -> destino."""
    if ENV_SYNC_USE_DIRECT_FIELDS:
        fields = []
        for cf in source_task.get("custom_fields", []) or []:
            field_id = str(cf.get("id") or "").strip()
            if not field_id:
                continue
            field_type = _normalize_field_type(cf.get("type"))
            if _is_file_custom_field(cf):
                continue
            if field_type and not _is_writable_custom_field_type(field_type):
                continue
            value = cf.get("value")
            if value is None:
                continue
            fields.append({"id": field_id, "value": value})
        return fields

    source_to_dest_field_map = _get_source_to_dest_field_map()
    if not source_to_dest_field_map:
        return []

    type_by_id = {
        str(cf.get("id")): _normalize_field_type(cf.get("type"))
        for cf in source_task.get("custom_fields", [])
    }

    fields = []
    for src_cf_id, dest_cf_id in source_to_dest_field_map.items():
        source_field_type = type_by_id.get(str(src_cf_id))
        if _is_file_custom_field({"type": source_field_type}):
            continue
        if source_field_type and not _is_writable_custom_field_type(source_field_type):
            continue
        value = get_custom_field_value(source_task, src_cf_id)
        if value is not None:
            fields.append({"id": dest_cf_id, "value": value})

    if not fields and source_to_dest_field_map:
        logger.warning(
            "Nenhum custom field encontrado na task de origem. "
            "Total no source_task=%d, map_calculado=%d. Verifique mapeamento.",
            len(source_task.get("custom_fields", [])),
            len(source_to_dest_field_map),
        )

    return fields


def build_reverse_custom_fields_payload(dest_task: dict) -> list[dict]:
    """Monta payload de custom fields para retorno destino -> origem."""
    if ENV_SYNC_USE_DIRECT_FIELDS:
        fields = []
        for cf in dest_task.get("custom_fields", []) or []:
            field_id = str(cf.get("id") or "").strip()
            if not field_id:
                continue
            field_type = _normalize_field_type(cf.get("type"))
            if _is_file_custom_field(cf):
                continue
            if field_type and not _is_writable_custom_field_type(field_type):
                continue
            value = cf.get("value")
            if value is None:
                continue
            fields.append({"id": field_id, "value": value})
        return fields

    reverse_field_map = _get_dest_to_source_field_map()
    if not reverse_field_map:
        return []

    type_by_id = {
        str(cf.get("id")): _normalize_field_type(cf.get("type"))
        for cf in dest_task.get("custom_fields", [])
    }

    fields = []
    for dest_cf_id, source_cf_id in reverse_field_map.items():
        dest_field_type = type_by_id.get(str(dest_cf_id))
        if _is_file_custom_field({"type": dest_field_type}):
            continue
        if dest_field_type and not _is_writable_custom_field_type(dest_field_type):
            continue
        value = get_custom_field_value(dest_task, dest_cf_id)
        if value is not None:
            fields.append({"id": source_cf_id, "value": value})

    return fields


def clone_attachments(source_task: dict, dest_task_id: str) -> dict[str, int | bool]:
    """Clona anexos de custom fields para custom fields destino (com fallback)."""
    source_custom_fields = {
        str(cf.get("id")): cf for cf in (source_task.get("custom_fields", []) or [])
    }
    source_to_dest_field_map = _get_source_to_dest_field_map()
    source_task_name = source_task.get("name", "Sem nome")

    sent_count = 0
    attempted_count = 0
    failed_count = 0
    skipped_no_url_count = 0
    sent_filenames: set[str] = set()
    source_attachment_fields = [
        (source_cf_id, cf)
        for source_cf_id, cf in source_custom_fields.items()
        if _is_file_custom_field(cf)
    ]

    for src_cf_id, cf in source_attachment_fields:
        value = cf.get("value")
        if value is None:
            continue

        items: list[dict] = []
        if isinstance(value, list):
            items = [v for v in value if isinstance(v, dict)]
        elif isinstance(value, dict):
            items = [value]

        if not items:
            logger.warning(
                "Campo de anexo %s com formato de valor nao suportado. Ignorando.",
                src_cf_id,
            )
            continue

        for item in items:
            url = _select_attachment_url(item)
            if not url:
                logger.warning("Campo %s possui item sem URL de download. Ignorando.", src_cf_id)
                skipped_no_url_count += 1
                continue
            attempted_count += 1
            filename = _build_custom_field_attachment_filename(cf, source_task_name, item)
            temp_path, content_type = _download_attachment_to_temp(url, filename)
            try:
                sent = False
                ok = _upload_task_attachment(dest_task_id, temp_path, filename, content_type)
                if ok:
                    sent = True

                if sent:
                    sent_count += 1
                    sent_filenames.add(filename)
                else:
                    failed_count += 1
                    logger.warning("Falha ao enviar anexo do campo %s: %s", src_cf_id, filename)
            finally:
                try:
                    os.unlink(temp_path)
                except OSError:
                    logger.debug("Falha ao remover arquivo temporario: %s", temp_path)

    # Fallback importante para roundtrip:
    # quando a task origem nao possui mais os custom fields de anexo
    # (ex.: lista intermediaria), preserva anexos task-level somente como
    # anexos da task (sem inferir campo por nome de arquivo).
    task_level_attachments = source_task.get("attachments", []) or []
    for att in task_level_attachments:
        url = _select_attachment_url(att)
        if not url:
            skipped_no_url_count += 1
            continue

        filename = _guess_attachment_filename(att)
        if filename in sent_filenames:
            continue
        attempted_count += 1

        temp_path, content_type = _download_attachment_to_temp(url, filename)
        try:
            sent = False
            ok = _upload_task_attachment(dest_task_id, temp_path, filename, content_type)
            if ok:
                sent = True

            if sent:
                sent_count += 1
                sent_filenames.add(filename)
            else:
                failed_count += 1
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                logger.debug("Falha ao remover arquivo temporario: %s", temp_path)

    logger.info(
        "Clone de anexos concluido: enviados=%d, campos_origem_anexo=%d",
        sent_count,
        len(source_attachment_fields),
    )
    return {
        "attempted": attempted_count,
        "sent": sent_count,
        "failed": failed_count,
        "skipped_no_url": skipped_no_url_count,
        "attachment_fields": len(source_attachment_fields),
        "ok": failed_count == 0,
    }


def clone_attachments_dest_to_source(
    dest_task: dict, source_task_id: str
) -> dict[str, int | bool]:
    """Clona anexos do destino para source via custom fields mapeados (com fallback)."""
    dest_custom_fields = {
        str(cf.get("id")): cf for cf in (dest_task.get("custom_fields", []) or [])
    }
    dest_task_name = dest_task.get("name", "Sem nome")

    sent_count = 0
    attempted_count = 0
    failed_count = 0
    skipped_no_url_count = 0
    sent_filenames: set[str] = set()
    dest_attachment_fields = [
        (dest_cf_id, cf)
        for dest_cf_id, cf in dest_custom_fields.items()
        if _is_file_custom_field(cf)
    ]

    for dest_cf_id, cf in dest_attachment_fields:
        value = cf.get("value")
        if value is None:
            continue

        items: list[dict] = []
        if isinstance(value, list):
            items = [v for v in value if isinstance(v, dict)]
        elif isinstance(value, dict):
            items = [value]

        if not items:
            logger.warning(
                "Campo de anexo destino %s com formato de valor nao suportado. Ignorando.",
                dest_cf_id,
            )
            continue

        for item in items:
            url = _select_attachment_url(item)
            if not url:
                skipped_no_url_count += 1
                continue
            attempted_count += 1
            filename = _build_custom_field_attachment_filename(cf, dest_task_name, item)
            temp_path, content_type = _download_attachment_to_temp_from_dest(url, filename)
            try:
                sent = False
                ok = _upload_task_attachment_to_source(
                    source_task_id, temp_path, filename, content_type
                )
                if ok:
                    sent = True

                if sent:
                    sent_count += 1
                    sent_filenames.add(filename)
                else:
                    failed_count += 1
                    logger.warning(
                        "Falha ao enviar anexo de retorno do campo %s: %s",
                        dest_cf_id,
                        filename,
                    )
            finally:
                try:
                    os.unlink(temp_path)
                except OSError:
                    logger.debug("Falha ao remover arquivo temporario: %s", temp_path)

    # Fallback para anexos task-level no destino:
    # preserva anexos como anexos da task source (sem inferir campo por nome).
    task_level_attachments = dest_task.get("attachments", []) or []
    for att in task_level_attachments:
        url = _select_attachment_url(att)
        if not url:
            skipped_no_url_count += 1
            continue

        filename = _guess_attachment_filename(att)
        if filename in sent_filenames:
            continue
        attempted_count += 1

        temp_path, content_type = _download_attachment_to_temp_from_dest(url, filename)
        try:
            sent = False
            ok = _upload_task_attachment_to_source(
                source_task_id, temp_path, filename, content_type
            )
            if ok:
                sent = True

            if sent:
                sent_count += 1
                sent_filenames.add(filename)
            else:
                failed_count += 1
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                logger.debug("Falha ao remover arquivo temporario: %s", temp_path)

    logger.info(
        "Retorno de anexos concluido: enviados=%d, campos_destino_anexo=%d",
        sent_count,
        len(dest_attachment_fields),
    )
    return {
        "attempted": attempted_count,
        "sent": sent_count,
        "failed": failed_count,
        "skipped_no_url": skipped_no_url_count,
        "attachment_fields": len(dest_attachment_fields),
        "ok": failed_count == 0,
    }


def _format_custom_field_value_for_snapshot(cf: dict) -> str:
    value = cf.get("value")
    if value is None:
        return ""

    field_type = _normalize_field_type(cf.get("type"))
    cfg = cf.get("type_config") or {}

    if field_type == "date":
        try:
            dt_utc = datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
            dt_local = dt_utc.astimezone(timezone(timedelta(hours=-3)))
            return dt_local.strftime("%d/%m/%Y - %Hh%M")
        except Exception:
            return str(value)

    if field_type == "drop_down":
        options = cfg.get("options") or []
        by_token: dict[str, str] = {}
        for opt in options:
            label = str(opt.get("name") or opt.get("label") or opt.get("id") or "").strip()
            for token in (opt.get("id"), opt.get("orderindex"), opt.get("value")):
                if token is None:
                    continue
                by_token[str(token)] = label or str(token)
        return by_token.get(str(value), str(value))

    if field_type == "labels":
        options = cfg.get("options") or []
        by_id: dict[str, str] = {}
        for opt in options:
            label = str(opt.get("name") or opt.get("label") or opt.get("id") or "").strip()
            for token in (opt.get("id"), opt.get("orderindex"), opt.get("value")):
                if token is None:
                    continue
                by_id[str(token)] = label or str(token)
        if isinstance(value, list):
            labels = [by_id.get(str(v), str(v)) for v in value]
            return ", ".join([label for label in labels if label])
        return by_id.get(str(value), str(value))

    if isinstance(value, bool):
        return "Sim" if value else "Não"

    if isinstance(value, list):
        if not value:
            return ""
        return ", ".join(str(v) for v in value)

    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    return str(value)


def build_unmapped_custom_fields_snapshot(source_task: dict) -> list[tuple[str, str]]:
    """Retorna [(nome_campo, valor)] de campos sem mapeamento para destino."""
    source_to_dest_map = _get_source_to_dest_field_map()
    rows: list[tuple[str, str]] = []
    for cf in source_task.get("custom_fields", []) or []:
        source_id = str(cf.get("id") or "").strip()
        if not source_id:
            continue
        if source_id in source_to_dest_map:
            continue
        if _is_file_custom_field(cf):
            continue
        rendered = _format_custom_field_value_for_snapshot(cf)
        if not str(rendered).strip():
            continue
        field_name = str(cf.get("name") or source_id).strip().rstrip(":")
        rows.append((field_name, rendered))
    return rows


def post_unmapped_custom_fields_snapshot_comment(source_task: dict, dest_task_id: str) -> int:
    """Publica comentário no destino com campos sem coluna correspondente."""
    rows = build_unmapped_custom_fields_snapshot(source_task)
    if not rows:
        return 0
    lines = ["Campos sem coluna no destino (valores preservados):"]
    lines.extend(f"{name}: {value}" for name, value in rows)
    _create_task_comment(dest_task_id, "\n".join(lines))
    logger.info(
        "Snapshot de campos sem mapeamento publicado no destino: task_id=%s campos=%d",
        dest_task_id,
        len(rows),
    )
    return len(rows)


def _get_task_comments(task_id: str) -> list[dict]:
    """Busca comentÃ¡rios da task com paginaÃ§Ã£o."""
    session = _get_source_session()
    comments: list[dict] = []
    params = {}
    while True:
        resp = _request_with_retry(
            session, "GET", f"{BASE_URL}/task/{task_id}/comment", params=params
        )
        data = resp.json()
        batch = data.get("comments", [])
        if not batch:
            break
        comments.extend(batch)
        last = batch[-1]
        if not last.get("id") or not last.get("date"):
            break
        params = {"start": last.get("date"), "start_id": last.get("id")}
    return comments


def _get_task_comments_from_dest(task_id: str) -> list[dict]:
    """Busca comentarios da task destino com paginacao."""
    session = _get_dest_session()
    comments: list[dict] = []
    params = {}
    while True:
        resp = _request_with_retry(
            session, "GET", f"{BASE_URL}/task/{task_id}/comment", params=params
        )
        data = resp.json()
        batch = data.get("comments", [])
        if not batch:
            break
        comments.extend(batch)
        last = batch[-1]
        if not last.get("id") or not last.get("date"):
            break
        params = {"start": last.get("date"), "start_id": last.get("id")}
    return comments


def _get_comment_replies(comment_id: str) -> list[dict]:
    session = _get_source_session()
    resp = _request_with_retry(session, "GET", f"{BASE_URL}/comment/{comment_id}/reply")
    return resp.json().get("comments", [])


def _get_comment_replies_from_dest(comment_id: str) -> list[dict]:
    session = _get_dest_session()
    resp = _request_with_retry(session, "GET", f"{BASE_URL}/comment/{comment_id}/reply")
    return resp.json().get("comments", [])


def _create_task_comment(task_id: str, text: str) -> dict:
    session = _get_dest_session()
    payload = {"comment_text": text}
    resp = _request_with_retry(session, "POST", f"{BASE_URL}/task/{task_id}/comment", json=payload)
    return resp.json()


def _build_link_fallback_text(
    prefix_text: str,
    link_text: str,
    link_url: str,
    suffix_text: str = "",
) -> str:
    prefix = str(prefix_text or "")
    if prefix and not prefix.endswith((" ", "\n", "\t")):
        prefix = f"{prefix} "
    return (
        f"{prefix}"
        f"{str(link_text or '').strip()} ({str(link_url or '').strip()})"
        f"{str(suffix_text or '')}"
    ).strip()


def _create_task_comment_with_link(
    task_id: str,
    prefix_text: str,
    link_text: str,
    link_url: str,
    suffix_text: str = "",
) -> dict:
    session = _get_dest_session()
    comment_chunks: list[dict] = [
        {"text": str(prefix_text or "")},
        {"text": str(link_text or ""), "attributes": {"link": str(link_url or "")}},
    ]
    if str(suffix_text or ""):
        comment_chunks.append({"text": str(suffix_text or "")})
    payload = {"comment": comment_chunks}
    try:
        resp = _request_with_retry(
            session,
            "POST",
            f"{BASE_URL}/task/{task_id}/comment",
            json=payload,
        )
        return resp.json()
    except Exception:
        fallback_text = _build_link_fallback_text(
            prefix_text,
            link_text,
            link_url,
            suffix_text=suffix_text,
        )
        return _create_task_comment(task_id, fallback_text)


def _create_task_comment_in_source(task_id: str, text: str) -> dict:
    session = _get_source_session()
    payload = {"comment_text": text}
    resp = _request_with_retry(session, "POST", f"{BASE_URL}/task/{task_id}/comment", json=payload)
    return resp.json()


def _create_task_comment_in_source_with_link(
    task_id: str,
    prefix_text: str,
    link_text: str,
    link_url: str,
    suffix_text: str = "",
) -> dict:
    session = _get_source_session()
    comment_chunks: list[dict] = [
        {"text": str(prefix_text or "")},
        {"text": str(link_text or ""), "attributes": {"link": str(link_url or "")}},
    ]
    if str(suffix_text or ""):
        comment_chunks.append({"text": str(suffix_text or "")})
    payload = {"comment": comment_chunks}
    try:
        resp = _request_with_retry(
            session,
            "POST",
            f"{BASE_URL}/task/{task_id}/comment",
            json=payload,
        )
        return resp.json()
    except Exception:
        fallback_text = _build_link_fallback_text(
            prefix_text,
            link_text,
            link_url,
            suffix_text=suffix_text,
        )
        return _create_task_comment_in_source(task_id, fallback_text)


def create_task_comment_any(task_id: str, text: str) -> dict:
    """Cria comentario em task com fallback entre tokens."""
    payload = {"comment_text": text}
    try:
        resp = _request_with_retry(
            _get_dest_session(),
            "POST",
            f"{BASE_URL}/task/{task_id}/comment",
            json=payload,
        )
        return resp.json()
    except Exception:
        resp = _request_with_retry(
            _get_source_session(),
            "POST",
            f"{BASE_URL}/task/{task_id}/comment",
            json=payload,
        )
        return resp.json()


def _extract_member_ids(payload: dict) -> set[int]:
    members = []
    if isinstance(payload, dict):
        members = payload.get("members") or payload.get("users") or []

    member_ids: set[int] = set()
    for member in members:
        if not isinstance(member, dict):
            continue
        user = member.get("user")
        raw_id = None
        if isinstance(user, dict):
            raw_id = user.get("id")
        if raw_id is None:
            raw_id = member.get("id")
        try:
            member_ids.add(int(str(raw_id)))
        except Exception:
            continue
    return member_ids


def _fetch_task_member_ids_any(task_id: str) -> set[int] | None:
    """Retorna IDs de membros com acesso a task, quando disponivel."""
    url = f"{BASE_URL}/task/{task_id}/member"
    for session in _webhook_sessions():
        try:
            resp = _request_with_retry(session, "GET", url)
            return _extract_member_ids(resp.json() if resp.content else {})
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status in {401, 403, 404}:
                continue
            logger.debug(
                "Falha ao buscar membros da task %s (status=%s).",
                task_id,
                status,
            )
            return None
        except Exception:
            continue
    return None


def _build_mentions_fallback_text(
    message: str,
    mention_separator: str,
    mention_ids: list[int],
    fallback_names: list[str],
) -> str:
    mentions_text: list[str] = []
    if mention_ids:
        for index, mention_id in enumerate(mention_ids):
            if index < len(fallback_names):
                label = fallback_names[index]
            else:
                label = str(mention_id)
            label = str(label).strip()
            if not label:
                continue
            mentions_text.append(f"@{label}")
    else:
        for name in fallback_names:
            label = str(name).strip()
            if not label:
                continue
            mentions_text.append(f"@{label}")

    fallback_mentions = " ".join(mentions_text)
    return f"{message}{mention_separator}{fallback_mentions}".strip()


def create_task_comment_with_tag_any(
    task_id: str,
    text_prefix: str,
    user_id: str | int,
    user_name_fallback: str = "",
) -> dict:
    """Cria comentario com mencao (tag) a usuario especifico."""
    return create_task_comment_with_tags_any(
        task_id=task_id,
        text_prefix=text_prefix,
        user_ids=[str(user_id).strip()],
        user_names_fallback=[user_name_fallback],
    )


def create_task_comment_with_tags_any(
    task_id: str,
    text_prefix: str,
    user_ids: list[str | int],
    user_names_fallback: list[str] | None = None,
) -> dict:
    """Cria comentario com mencao (tag) para um ou mais usuarios."""
    message = str(text_prefix or "").rstrip()
    # ClickUp pode colapsar espaco unico antes de "tag" em alguns clientes.
    # Usamos separador com dois espacos para manter afastamento visual consistente.
    mention_separator = "  "

    mention_ids: list[int] = []
    for raw_id in user_ids or []:
        text_id = str(raw_id or "").strip()
        if not text_id:
            continue
        try:
            mention_ids.append(int(text_id))
        except Exception:
            continue

    fallback_names = [
        str(name).strip()
        for name in (user_names_fallback or [])
        if str(name or "").strip()
    ]

    if not mention_ids:
        fallback_text = _build_mentions_fallback_text(
            message=message,
            mention_separator=mention_separator,
            mention_ids=[],
            fallback_names=fallback_names,
        )
        return create_task_comment_any(task_id, fallback_text)

    mentionable_member_ids = _fetch_task_member_ids_any(task_id)
    comment_chunks: list[dict] = [{"text": message}]
    for index, mention_id in enumerate(mention_ids):
        comment_chunks.append({"text": mention_separator})
        if mentionable_member_ids is None or mention_id in mentionable_member_ids:
            comment_chunks.append({"type": "tag", "user": {"id": mention_id}})
            continue

        fallback_label = (
            fallback_names[index]
            if index < len(fallback_names)
            else str(mention_id)
        )
        comment_chunks.append({"text": f"@{str(fallback_label).strip()}"})

    payload = {
        "comment": comment_chunks,
        "notify_all": False,
    }

    try:
        resp = _request_with_retry(
            _get_dest_session(),
            "POST",
            f"{BASE_URL}/task/{task_id}/comment",
            json=payload,
        )
        return resp.json()
    except Exception:
        try:
            resp = _request_with_retry(
                _get_source_session(),
                "POST",
                f"{BASE_URL}/task/{task_id}/comment",
                json=payload,
            )
            return resp.json()
        except Exception:
            fallback_text = _build_mentions_fallback_text(
                message=message,
                mention_separator=mention_separator,
                mention_ids=mention_ids,
                fallback_names=fallback_names,
            )
            return create_task_comment_any(task_id, fallback_text)


def _create_comment_reply(parent_comment_id: str, text: str) -> dict:
    session = _get_dest_session()
    payload = {"comment_text": text}
    resp = _request_with_retry(
        session, "POST", f"{BASE_URL}/comment/{parent_comment_id}/reply", json=payload
    )
    return resp.json()


def _create_comment_reply_with_link(
    parent_comment_id: str,
    prefix_text: str,
    link_text: str,
    link_url: str,
    suffix_text: str = "",
) -> dict:
    session = _get_dest_session()
    comment_chunks: list[dict] = [
        {"text": str(prefix_text or "")},
        {"text": str(link_text or ""), "attributes": {"link": str(link_url or "")}},
    ]
    if str(suffix_text or ""):
        comment_chunks.append({"text": str(suffix_text or "")})
    payload = {"comment": comment_chunks}
    try:
        resp = _request_with_retry(
            session,
            "POST",
            f"{BASE_URL}/comment/{parent_comment_id}/reply",
            json=payload,
        )
        return resp.json()
    except Exception:
        fallback_text = _build_link_fallback_text(
            prefix_text,
            link_text,
            link_url,
            suffix_text=suffix_text,
        )
        return _create_comment_reply(parent_comment_id, fallback_text)


def _create_comment_reply_in_source(parent_comment_id: str, text: str) -> dict:
    session = _get_source_session()
    payload = {"comment_text": text}
    resp = _request_with_retry(
        session, "POST", f"{BASE_URL}/comment/{parent_comment_id}/reply", json=payload
    )
    return resp.json()


def _create_comment_reply_in_source_with_link(
    parent_comment_id: str,
    prefix_text: str,
    link_text: str,
    link_url: str,
    suffix_text: str = "",
) -> dict:
    session = _get_source_session()
    comment_chunks: list[dict] = [
        {"text": str(prefix_text or "")},
        {"text": str(link_text or ""), "attributes": {"link": str(link_url or "")}},
    ]
    if str(suffix_text or ""):
        comment_chunks.append({"text": str(suffix_text or "")})
    payload = {"comment": comment_chunks}
    try:
        resp = _request_with_retry(
            session,
            "POST",
            f"{BASE_URL}/comment/{parent_comment_id}/reply",
            json=payload,
        )
        return resp.json()
    except Exception:
        fallback_text = _build_link_fallback_text(
            prefix_text,
            link_text,
            link_url,
            suffix_text=suffix_text,
        )
        return _create_comment_reply_in_source(parent_comment_id, fallback_text)


_FORMATTED_COMMENT_RE = re.compile(
    r"^(?P<author>[^\n]+?)\s-\s(?P<body>.*)\n\n(?P<dt>\d{2}/\d{2}/\d{4}\s-\s\d{2}h\d{2})$",
    re.DOTALL,
)
_FILENAME_ONLY_RE = re.compile(
    r"^[^\\/:*?\"<>|\n\r]+\.[A-Za-z0-9]{2,12}$"
)
_EXPLICIT_ATTACHMENT_WITH_LINK_RE = re.compile(
    r"^arquivo anexado na task:\s*(?P<filename>.+?)\s*\((?P<link>https?://[^\s)]+)\)\s*$",
    re.IGNORECASE,
)


def _author_name(user: dict) -> str:
    return (user or {}).get("username") or "desconhecido"


def _format_comment_datetime(date_ms) -> str:
    try:
        dt_utc = datetime.fromtimestamp(int(date_ms) / 1000, tz=timezone.utc)
        dt_local = dt_utc.astimezone(timezone(timedelta(hours=-3)))
        return dt_local.strftime("%d/%m/%Y - %Hh%M")
    except Exception:
        return "sem data - sem hora"


def _collapse_to_innermost_formatted_comment(text: str) -> str | None:
    """Colapsa comentario formatado aninhado para evitar assinaturas/data repetidas."""
    current = (text or "").strip()
    if not current:
        return None

    last_match: re.Match | None = None
    for _ in range(10):
        match = _FORMATTED_COMMENT_RE.match(current)
        if not match:
            break
        last_match = match
        current = match.group("body").strip()

    if not last_match:
        return None

    author = last_match.group("author").strip()
    body = current
    dt_text = last_match.group("dt").strip()
    return f"{author} - {body}\n\n{dt_text}"


def _normalize_attachment_name(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _extract_attachment_only_filename(body_text: str) -> str | None:
    text = str(body_text or "").strip()
    if not text:
        return None
    if text.lower().startswith("arquivo anexado na task:"):
        return None
    if "\n" in text or "\r" in text:
        return None
    if not _FILENAME_ONLY_RE.match(text):
        return None
    return text


def _build_task_attachment_lookup(task_data: dict) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for att in task_data.get("attachments", []) or []:
        filename = _guess_attachment_filename(att)
        if not filename:
            continue
        key = _normalize_attachment_name(filename)
        if not key:
            continue
        if key in lookup:
            continue
        lookup[key] = _select_attachment_url(att) or ""
    return lookup


def _rewrite_attachment_only_comment(
    body_text: str,
    attachment_lookup: dict[str, str] | None,
) -> str:
    filename = _extract_attachment_only_filename(body_text)
    if not filename:
        return str(body_text or "").strip()
    if not attachment_lookup:
        return str(body_text or "").strip()

    key = _normalize_attachment_name(filename)
    link = attachment_lookup.get(key)
    if link is None:
        return str(body_text or "").strip()
    if link:
        return f"Arquivo anexado na task: {filename} ({link})"
    return f"Arquivo anexado na task: {filename}"


def _split_formatted_comment(text: str) -> tuple[str, str, str] | None:
    match = _FORMATTED_COMMENT_RE.match(str(text or ""))
    if not match:
        return None
    return (
        match.group("author").strip(),
        match.group("body").strip(),
        match.group("dt").strip(),
    )


def _attachment_link_data_from_body(
    body_text: str,
    attachment_lookup: dict[str, str] | None,
) -> tuple[str, str] | None:
    filename = _extract_attachment_only_filename(body_text)
    if not filename or not attachment_lookup:
        return None
    link = attachment_lookup.get(_normalize_attachment_name(filename))
    if not link:
        return None
    return filename, link


def _attachment_link_data_from_formatted_comment(
    formatted_text: str,
    attachment_lookup: dict[str, str] | None,
) -> tuple[str, str] | None:
    parts = _split_formatted_comment(formatted_text)
    if not parts:
        return None
    _, body, _ = parts

    data = _attachment_link_data_from_body(body, attachment_lookup)
    if data:
        return data

    match = _EXPLICIT_ATTACHMENT_WITH_LINK_RE.match(body)
    if not match:
        return None
    filename = str(match.group("filename") or "").strip()
    link = str(match.group("link") or "").strip()
    if not filename or not link:
        return None
    return filename, link


def _post_formatted_task_comment_dest(
    task_id: str,
    formatted_text: str,
    attachment_lookup: dict[str, str] | None,
) -> dict:
    link_data = _attachment_link_data_from_formatted_comment(formatted_text, attachment_lookup)
    parts = _split_formatted_comment(formatted_text)
    if not link_data or not parts:
        return _create_task_comment(task_id, formatted_text)

    filename, link = link_data
    author, _, dt_text = parts
    return _create_task_comment_with_link(
        task_id,
        f"{author} - Arquivo anexado na task: ",
        filename,
        link,
        suffix_text=f"\n\n{dt_text}",
    )


def _post_formatted_task_comment_source(
    task_id: str,
    formatted_text: str,
    attachment_lookup: dict[str, str] | None,
) -> dict:
    link_data = _attachment_link_data_from_formatted_comment(formatted_text, attachment_lookup)
    parts = _split_formatted_comment(formatted_text)
    if not link_data or not parts:
        return _create_task_comment_in_source(task_id, formatted_text)

    filename, link = link_data
    author, _, dt_text = parts
    return _create_task_comment_in_source_with_link(
        task_id,
        f"{author} - Arquivo anexado na task: ",
        filename,
        link,
        suffix_text=f"\n\n{dt_text}",
    )


def _post_formatted_reply_dest(
    parent_comment_id: str,
    formatted_text: str,
    attachment_lookup: dict[str, str] | None,
) -> dict:
    link_data = _attachment_link_data_from_formatted_comment(formatted_text, attachment_lookup)
    parts = _split_formatted_comment(formatted_text)
    if not link_data or not parts:
        return _create_comment_reply(parent_comment_id, formatted_text)

    filename, link = link_data
    author, _, dt_text = parts
    return _create_comment_reply_with_link(
        parent_comment_id,
        f"{author} - Arquivo anexado na task: ",
        filename,
        link,
        suffix_text=f"\n\n{dt_text}",
    )


def _post_formatted_reply_source(
    parent_comment_id: str,
    formatted_text: str,
    attachment_lookup: dict[str, str] | None,
) -> dict:
    link_data = _attachment_link_data_from_formatted_comment(formatted_text, attachment_lookup)
    parts = _split_formatted_comment(formatted_text)
    if not link_data or not parts:
        return _create_comment_reply_in_source(parent_comment_id, formatted_text)

    filename, link = link_data
    author, _, dt_text = parts
    return _create_comment_reply_in_source_with_link(
        parent_comment_id,
        f"{author} - Arquivo anexado na task: ",
        filename,
        link,
        suffix_text=f"\n\n{dt_text}",
    )


def _format_comment_text_for_sync(
    user: dict,
    text: str,
    date_ms,
    *,
    attachment_lookup: dict[str, str] | None = None,
) -> str:
    """Formata comentario de forma idempotente para evitar acumulo de assinatura/data."""
    raw_text = (text or "").strip()
    normalized_existing = _collapse_to_innermost_formatted_comment(raw_text)
    if normalized_existing:
        match = _FORMATTED_COMMENT_RE.match(normalized_existing)
        if match:
            author = match.group("author").strip()
            dt_text = match.group("dt").strip()
            body = match.group("body").strip()
            body = _rewrite_attachment_only_comment(body, attachment_lookup)
            return f"{author} - {body}\n\n{dt_text}"
        return normalized_existing

    username = _author_name(user)
    dt_text = _format_comment_datetime(date_ms)
    raw_text = _rewrite_attachment_only_comment(raw_text, attachment_lookup)
    return f"{username} - {raw_text}\n\n{dt_text}"


def clone_comments(source_task_id: str, dest_task_id: str) -> dict[str, int | bool]:
    """Clona comentarios e replies da task origem para a task destino."""
    comments = _get_task_comments(source_task_id)
    if not comments:
        return {"comments": 0, "replies": 0, "ok": True}

    # Ordena do mais antigo para o mais novo
    comments.sort(key=lambda c: int(c.get("date", 0)))
    try:
        dest_task_data = fetch_task_from_dest(dest_task_id)
    except Exception:
        try:
            dest_task_data = fetch_task(dest_task_id)
        except Exception:
            dest_task_data = {}
    attachment_lookup = _build_task_attachment_lookup(dest_task_data)

    id_map: dict[str, str] = {}
    created_comments = 0
    created_replies = 0
    for c in comments:
        formatted = _format_comment_text_for_sync(
            c.get("user"),
            c.get("comment_text") or "",
            c.get("date"),
            attachment_lookup=attachment_lookup,
        )
        created = _post_formatted_task_comment_dest(dest_task_id, formatted, attachment_lookup)
        src_id = c.get("id")
        dest_id = str(created.get("id"))
        if src_id and dest_id:
            id_map[src_id] = dest_id
        created_comments += 1

    for c in comments:
        src_id = c.get("id")
        dest_parent = id_map.get(src_id)
        if not src_id or not dest_parent:
            continue
        replies = _get_comment_replies(src_id)
        replies.sort(key=lambda r: int(r.get("date", 0)))
        for r in replies:
            formatted = _format_comment_text_for_sync(
                r.get("user"),
                r.get("comment_text") or "",
                r.get("date"),
                attachment_lookup=attachment_lookup,
            )
            _post_formatted_reply_dest(dest_parent, formatted, attachment_lookup)
            created_replies += 1

    return {"comments": created_comments, "replies": created_replies, "ok": True}


def clone_comments_dest_to_source(
    dest_task_id: str, source_task_id: str
) -> dict[str, int | bool]:
    """Clona comentarios e replies da task destino para task source."""
    comments = _get_task_comments_from_dest(dest_task_id)
    if not comments:
        return {"comments": 0, "replies": 0, "ok": True}

    comments.sort(key=lambda c: int(c.get("date", 0)))
    try:
        source_task_data = fetch_task(source_task_id)
    except Exception:
        try:
            source_task_data = fetch_task_from_dest(source_task_id)
        except Exception:
            source_task_data = {}
    attachment_lookup = _build_task_attachment_lookup(source_task_data)

    id_map: dict[str, str] = {}
    created_comments = 0
    created_replies = 0
    for c in comments:
        formatted = _format_comment_text_for_sync(
            c.get("user"),
            c.get("comment_text") or "",
            c.get("date"),
            attachment_lookup=attachment_lookup,
        )
        created = _post_formatted_task_comment_source(
            source_task_id,
            formatted,
            attachment_lookup,
        )
        src_id = c.get("id")
        source_id = str(created.get("id"))
        if src_id and source_id:
            id_map[src_id] = source_id
        created_comments += 1

    for c in comments:
        src_id = c.get("id")
        source_parent = id_map.get(src_id)
        if not src_id or not source_parent:
            continue
        replies = _get_comment_replies_from_dest(src_id)
        replies.sort(key=lambda r: int(r.get("date", 0)))
        for r in replies:
            formatted = _format_comment_text_for_sync(
                r.get("user"),
                r.get("comment_text") or "",
                r.get("date"),
                attachment_lookup=attachment_lookup,
            )
            _post_formatted_reply_source(source_parent, formatted, attachment_lookup)
            created_replies += 1

    return {"comments": created_comments, "replies": created_replies, "ok": True}

