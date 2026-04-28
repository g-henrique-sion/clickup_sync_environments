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


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# ESCRITA â€” Workspace de destino
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


def create_task_in_dest(
    name: str,
    description: str | None = None,
    custom_fields: list[dict] | None = None,
) -> dict:
    """Cria uma task na lista de destino."""
    session = _get_dest_session()
    url = f"{BASE_URL}/list/{DEST_LIST_ID}/task"

    payload: dict = {"name": name}

    if description:
        payload["description"] = description

    if custom_fields:
        payload["custom_fields"] = custom_fields

    resp = _request_with_retry(session, "POST", url, json=payload)
    data = resp.json()
    logger.info("Task criada no destino: %s (id=%s)", data.get("name"), data.get("id"))
    return data


def create_task_in_source_list(
    list_id: str,
    name: str,
    description: str | None = None,
    custom_fields: list[dict] | None = None,
) -> dict:
    """Cria uma task em uma lista da workspace de origem."""
    session = _get_source_session()
    url = f"{BASE_URL}/list/{list_id}/task"

    payload: dict = {"name": name}
    if description:
        payload["description"] = description
    if custom_fields:
        payload["custom_fields"] = custom_fields

    resp = _request_with_retry(session, "POST", url, json=payload)
    data = resp.json()
    logger.info(
        "Task criada no source (lista %s): %s (id=%s)",
        list_id,
        data.get("name"),
        data.get("id"),
    )
    return data


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
    """Monta payload de custom fields para o clone source->dest."""
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
    """Monta payload de custom fields para retorno destino -> source."""
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


def clone_attachments(source_task: dict, dest_task_id: str) -> None:
    """Clona anexos de custom fields para custom fields destino (com fallback)."""
    source_custom_fields = {
        str(cf.get("id")): cf for cf in (source_task.get("custom_fields", []) or [])
    }
    source_to_dest_field_map = _get_source_to_dest_field_map()
    source_task_name = source_task.get("name", "Sem nome")

    sent_count = 0
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

        dest_cf_id = source_to_dest_field_map.get(src_cf_id)
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
                continue
            filename = _build_custom_field_attachment_filename(cf, source_task_name, item)
            temp_path, content_type = _download_attachment_to_temp(url, filename)
            try:
                sent = False

                if dest_cf_id:
                    uploaded_id = _upload_custom_field_attachment(
                        dest_cf_id, temp_path, filename, content_type
                    )
                    if uploaded_id:
                        try:
                            _set_custom_field_value(
                                dest_task_id,
                                dest_cf_id,
                                {"add": [uploaded_id]},
                            )
                            sent = True
                        except Exception as e:
                            logger.warning(
                                "Falha ao associar anexo ao campo destino %s: %s",
                                dest_cf_id,
                                e,
                            )

                if not sent:
                    ok = _upload_task_attachment(dest_task_id, temp_path, filename, content_type)
                    if ok:
                        sent = True

                if sent:
                    sent_count += 1
                    sent_filenames.add(filename)
                else:
                    logger.warning("Falha ao enviar anexo do campo %s: %s", src_cf_id, filename)
            finally:
                try:
                    os.unlink(temp_path)
                except OSError:
                    logger.debug("Falha ao remover arquivo temporario: %s", temp_path)

    # Fallback importante para roundtrip:
    # quando a task origem nao possui mais os custom fields de anexo
    # (ex.: lista intermediaria), reaproveita anexos de task pelo nome do campo.
    dest_field_by_name = _get_dest_attachment_field_name_map()
    task_level_attachments = source_task.get("attachments", []) or []
    for att in task_level_attachments:
        url = _select_attachment_url(att)
        if not url:
            continue

        filename = _guess_attachment_filename(att)
        if filename in sent_filenames:
            continue
        inferred_name = _extract_field_name_from_attachment_filename(filename)
        dest_cf_id = dest_field_by_name.get(inferred_name)

        temp_path, content_type = _download_attachment_to_temp(url, filename)
        try:
            sent = False
            if dest_cf_id:
                uploaded_id = _upload_custom_field_attachment(
                    dest_cf_id, temp_path, filename, content_type
                )
                if uploaded_id:
                    try:
                        _set_custom_field_value(
                            dest_task_id,
                            dest_cf_id,
                            {"add": [uploaded_id]},
                        )
                        sent = True
                    except Exception as e:
                        logger.warning(
                            "Falha ao associar anexo de fallback ao campo destino %s: %s",
                            dest_cf_id,
                            e,
                        )

            if not sent:
                ok = _upload_task_attachment(dest_task_id, temp_path, filename, content_type)
                if ok:
                    sent = True

            if sent:
                sent_count += 1
                sent_filenames.add(filename)
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


def clone_attachments_dest_to_source(dest_task: dict, source_task_id: str) -> None:
    """Clona anexos do destino para source via custom fields mapeados (com fallback)."""
    dest_to_source_field_map = _get_dest_to_source_field_map()

    dest_custom_fields = {
        str(cf.get("id")): cf for cf in (dest_task.get("custom_fields", []) or [])
    }
    dest_task_name = dest_task.get("name", "Sem nome")

    sent_count = 0
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
                continue
            filename = _build_custom_field_attachment_filename(cf, dest_task_name, item)
            temp_path, content_type = _download_attachment_to_temp_from_dest(url, filename)
            try:
                sent = False
                source_cf_id = dest_to_source_field_map.get(dest_cf_id)
                if source_cf_id:
                    uploaded_id = _upload_custom_field_attachment_to_source(
                        source_cf_id, temp_path, filename, content_type
                    )
                    if uploaded_id:
                        try:
                            _set_custom_field_value_in_source(
                                source_task_id,
                                source_cf_id,
                                {"add": [uploaded_id]},
                            )
                            sent = True
                        except Exception as e:
                            logger.warning(
                                "Falha ao associar anexo ao campo source %s: %s",
                                source_cf_id,
                                e,
                            )

                if not sent:
                    ok = _upload_task_attachment_to_source(
                        source_task_id, temp_path, filename, content_type
                    )
                    if ok:
                        sent = True

                if sent:
                    sent_count += 1
                    sent_filenames.add(filename)
                else:
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
    # preserva anexos sem mapeamento de campo source.
    source_field_by_name = _get_source_attachment_field_name_map()
    task_level_attachments = dest_task.get("attachments", []) or []
    for att in task_level_attachments:
        url = _select_attachment_url(att)
        if not url:
            continue

        filename = _guess_attachment_filename(att)
        if filename in sent_filenames:
            continue
        inferred_name = _extract_field_name_from_attachment_filename(filename)
        source_cf_id = source_field_by_name.get(inferred_name)

        temp_path, content_type = _download_attachment_to_temp_from_dest(url, filename)
        try:
            sent = False
            if source_cf_id:
                uploaded_id = _upload_custom_field_attachment_to_source(
                    source_cf_id, temp_path, filename, content_type
                )
                if uploaded_id:
                    try:
                        _set_custom_field_value_in_source(
                            source_task_id,
                            source_cf_id,
                            {"add": [uploaded_id]},
                        )
                        sent = True
                    except Exception as e:
                        logger.warning(
                            "Falha ao associar anexo de fallback ao campo source %s: %s",
                            source_cf_id,
                            e,
                        )

            if not sent:
                ok = _upload_task_attachment_to_source(
                    source_task_id, temp_path, filename, content_type
                )
                if ok:
                    sent = True

            if sent:
                sent_count += 1
                sent_filenames.add(filename)
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


def _create_task_comment_in_source(task_id: str, text: str) -> dict:
    session = _get_source_session()
    payload = {"comment_text": text}
    resp = _request_with_retry(session, "POST", f"{BASE_URL}/task/{task_id}/comment", json=payload)
    return resp.json()


def _create_comment_reply(parent_comment_id: str, text: str) -> dict:
    session = _get_dest_session()
    payload = {"comment_text": text}
    resp = _request_with_retry(
        session, "POST", f"{BASE_URL}/comment/{parent_comment_id}/reply", json=payload
    )
    return resp.json()


def _create_comment_reply_in_source(parent_comment_id: str, text: str) -> dict:
    session = _get_source_session()
    payload = {"comment_text": text}
    resp = _request_with_retry(
        session, "POST", f"{BASE_URL}/comment/{parent_comment_id}/reply", json=payload
    )
    return resp.json()


_FORMATTED_COMMENT_RE = re.compile(
    r"^(?P<author>[^\n]+?)\s-\s(?P<body>.*)\n\n(?P<dt>\d{2}/\d{2}/\d{4}\s-\s\d{2}h\d{2})$",
    re.DOTALL,
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


def _format_comment_text_for_sync(user: dict, text: str, date_ms) -> str:
    """Formata comentario de forma idempotente para evitar acumulo de assinatura/data."""
    raw_text = (text or "").strip()
    normalized_existing = _collapse_to_innermost_formatted_comment(raw_text)
    if normalized_existing:
        return normalized_existing

    username = _author_name(user)
    dt_text = _format_comment_datetime(date_ms)
    return f"{username} - {raw_text}\n\n{dt_text}"


def clone_comments(source_task_id: str, dest_task_id: str) -> None:
    """Clona comentarios e replies da task origem para a task destino."""
    comments = _get_task_comments(source_task_id)
    if not comments:
        logger.info("Sem comentarios para clonar.")
        return

    # Ordena do mais antigo para o mais novo
    comments.sort(key=lambda c: int(c.get("date", 0)))

    id_map: dict[str, str] = {}
    for c in comments:
        formatted = _format_comment_text_for_sync(
            c.get("user"),
            c.get("comment_text") or "",
            c.get("date"),
        )
        created = _create_task_comment(dest_task_id, formatted)
        src_id = c.get("id")
        dest_id = str(created.get("id"))
        if src_id and dest_id:
            id_map[src_id] = dest_id

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
            )
            _create_comment_reply(dest_parent, formatted)


def clone_comments_dest_to_source(dest_task_id: str, source_task_id: str) -> None:
    """Clona comentarios e replies da task destino para task source."""
    comments = _get_task_comments_from_dest(dest_task_id)
    if not comments:
        logger.info("Sem comentarios para retornar ao source.")
        return

    comments.sort(key=lambda c: int(c.get("date", 0)))

    id_map: dict[str, str] = {}
    for c in comments:
        formatted = _format_comment_text_for_sync(
            c.get("user"),
            c.get("comment_text") or "",
            c.get("date"),
        )
        created = _create_task_comment_in_source(source_task_id, formatted)
        src_id = c.get("id")
        source_id = str(created.get("id"))
        if src_id and source_id:
            id_map[src_id] = source_id

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
            )
            _create_comment_reply_in_source(source_parent, formatted)

