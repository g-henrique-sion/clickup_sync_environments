"""Cliente ClickUp — leitura (origem) e escrita (destino)."""

import logging
import os
import tempfile
import time
from datetime import datetime, timezone

import requests

from app.config.settings import (
    CLONE_FIELD_MAP,
    DEST_CLICKUP_TOKEN,
    DEST_LIST_ID,
    DEST_WORKSPACE_ID,
    SOURCE_CLICKUP_TOKEN,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.clickup.com/api/v2"
BASE_URL_V3 = "https://api.clickup.com/api/v3"

# ── Sessões HTTP reutilizáveis ────────────────────────────────
_source_session: requests.Session | None = None
_dest_session: requests.Session | None = None
_dest_workspace_id: str | None = None

MAX_RETRIES = 3
RETRY_BACKOFF = 2  # segundos (multiplica a cada tentativa)
TIMEOUT = 30


def _get_source_session() -> requests.Session:
    global _source_session
    if _source_session is None:
        _source_session = requests.Session()
        _source_session.headers.update({"Authorization": SOURCE_CLICKUP_TOKEN})
    return _source_session


def _get_dest_session() -> requests.Session:
    global _dest_session
    if _dest_session is None:
        _dest_session = requests.Session()
        _dest_session.headers.update({"Authorization": DEST_CLICKUP_TOKEN})
    return _dest_session


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
        "NÃ£o foi possÃ­vel resolver o workspace de destino. "
        "Defina DEST_WORKSPACE_ID no .env."
    )


def _request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    **kwargs,
) -> requests.Response:
    """Executa request com retry e backoff para 429/5xx."""
    kwargs.setdefault("timeout", TIMEOUT)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(method, url, **kwargs)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", RETRY_BACKOFF * attempt))
                logger.warning("Rate limited (429). Aguardando %ds...", wait)
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = RETRY_BACKOFF * attempt
                logger.warning(
                    "Erro %d do servidor. Tentativa %d/%d. Aguardando %ds...",
                    resp.status_code, attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except requests.exceptions.Timeout:
            wait = RETRY_BACKOFF * attempt
            logger.warning(
                "Timeout na requisição. Tentativa %d/%d. Aguardando %ds...",
                attempt, MAX_RETRIES, wait,
            )
            time.sleep(wait)

    raise requests.exceptions.ConnectionError(
        f"Falha após {MAX_RETRIES} tentativas: {method} {url}"
    )


# ══════════════════════════════════════════════════════════════
# LEITURA — Workspace de origem
# ══════════════════════════════════════════════════════════════


def fetch_task(task_id: str) -> dict:
    """Busca dados completos de uma task na workspace de origem."""
    session = _get_source_session()
    url = f"{BASE_URL}/task/{task_id}"
    resp = _request_with_retry(session, "GET", url, params={"include_subtasks": "true"})
    return resp.json()


def get_custom_field_value(task: dict, cf_id: str):
    """Extrai o valor de um custom field de uma task."""
    for cf in task.get("custom_fields", []):
        if cf.get("id") == cf_id:
            return cf.get("value")
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


def _upload_task_attachment(dest_task_id: str, file_path: str, filename: str, content_type: str) -> bool:
    session = _get_dest_session()
    url = f"{BASE_URL}/task/{dest_task_id}/attachment"
    with open(file_path, "rb") as f:
        for key in ("attachment", "file", "attachment[0]"):
            files = {key: (filename, f, content_type)}
            try:
                _request_with_retry(session, "POST", url, files=files)
                return True
            except requests.HTTPError as e:
                resp = e.response
                detail = resp.text if resp is not None else str(e)
                logger.warning("Falha ao enviar anexo (%s): %s", key, detail)
    return False


def _upload_custom_field_attachment(
    dest_field_id: str, file_path: str, filename: str, content_type: str
) -> str | None:
    session = _get_dest_session()
    workspace_id = _get_dest_workspace_id()
    url = f"{BASE_URL_V3}/workspaces/{workspace_id}/custom_fields/{dest_field_id}/attachments"
    with open(file_path, "rb") as f:
        for key in ("file", "attachment", "attachment[0]"):
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


def _set_custom_field_value(task_id: str, field_id: str, value) -> None:
    session = _get_dest_session()
    url = f"{BASE_URL}/task/{task_id}/field/{field_id}"
    _request_with_retry(session, "POST", url, json={"value": value})


# ══════════════════════════════════════════════════════════════
# ESCRITA — Workspace de destino
# ══════════════════════════════════════════════════════════════


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


def build_custom_fields_payload(source_task: dict) -> list[dict]:
    """Monta o payload de custom fields para o clone usando o CLONE_FIELD_MAP."""
    if not CLONE_FIELD_MAP:
        return []

    type_by_id = {
        cf.get("id"): (cf.get("type") or "")
        for cf in source_task.get("custom_fields", [])
    }

    fields = []
    for src_cf_id, dest_cf_id in CLONE_FIELD_MAP.items():
        if _is_file_custom_field({"type": type_by_id.get(src_cf_id)}):
            continue
        value = get_custom_field_value(source_task, src_cf_id)
        if value is not None:
            fields.append({"id": dest_cf_id, "value": value})

    if not fields and CLONE_FIELD_MAP:
        logger.warning(
            "Nenhum custom field encontrado na task de origem. "
            "Total no source_task=%d, map=%d. Verifique IDs do mapeamento.",
            len(source_task.get("custom_fields", [])),
            len(CLONE_FIELD_MAP),
        )

    return fields


def clone_attachments(source_task: dict, dest_task_id: str) -> None:
    """Clona anexos da task e arquivos de custom fields."""
    # 1) Anexos da task
    for att in source_task.get("attachments", []) or []:
        url = _select_attachment_url(att)
        if not url:
            logger.warning("Anexo sem URL detectado. Ignorando.")
            continue
        filename = _guess_attachment_filename(att)
        temp_path, content_type = _download_attachment_to_temp(url, filename)
        try:
            ok = _upload_task_attachment(dest_task_id, temp_path, filename, content_type)
            if not ok:
                logger.warning("Anexo nÃ£o enviado: %s", filename)
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                logger.debug("Falha ao remover arquivo temporÃ¡rio: %s", temp_path)

    # 2) Arquivos de custom fields (tipo attachment/file)
    # ObservaÃ§Ã£o: a API nÃ£o oferece suporte confiÃ¡vel para setar "attachment" em custom fields,
    # entÃ£o fazemos fallback para anexar os arquivos na task.
    for cf in source_task.get("custom_fields", []) or []:
        if not _is_file_custom_field(cf):
            continue

        src_cf_id = cf.get("id")
        dest_cf_id = CLONE_FIELD_MAP.get(src_cf_id)
        if not dest_cf_id:
            continue

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
                "Custom field %s possui valor em formato nÃ£o suportado. Ignorando.",
                src_cf_id,
            )
            continue

        for item in items:
            url = _select_attachment_url(item)
            if not url:
                logger.warning("Arquivo de custom field sem URL. Ignorando.")
                continue
            filename = _guess_attachment_filename(item)
            temp_path, content_type = _download_attachment_to_temp(url, filename)
            try:
                ok = _upload_task_attachment(dest_task_id, temp_path, filename, content_type)
                if not ok:
                    logger.warning("Anexo de custom field nÃ£o enviado: %s", filename)
            finally:
                try:
                    os.unlink(temp_path)
                except OSError:
                    logger.debug("Falha ao remover arquivo temporÃ¡rio: %s", temp_path)


def _get_task_comments(task_id: str) -> list[dict]:
    """Busca comentários da task com paginação."""
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


def _get_comment_replies(comment_id: str) -> list[dict]:
    session = _get_source_session()
    resp = _request_with_retry(session, "GET", f"{BASE_URL}/comment/{comment_id}/reply")
    return resp.json().get("comments", [])


def _create_task_comment(task_id: str, text: str) -> dict:
    session = _get_dest_session()
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


def clone_comments(source_task_id: str, dest_task_id: str) -> None:
    """Clona comentarios e replies da task origem para a task destino."""
    comments = _get_task_comments(source_task_id)
    if not comments:
        logger.info("Sem comentarios para clonar.")
        return

    # Ordena do mais antigo para o mais novo
    comments.sort(key=lambda c: int(c.get("date", 0)))

    def _format_header(prefix: str, user: dict, date_ms) -> str:
        username = (user or {}).get("username") or "desconhecido"
        try:
            if date_ms:
                dt = datetime.fromtimestamp(int(date_ms) / 1000, tz=timezone.utc)
                date_str = dt.isoformat()
            else:
                date_str = "sem data"
        except Exception:
            date_str = "sem data"
        return f"[{prefix}: {username} | {date_str}]"

    id_map: dict[str, str] = {}
    for c in comments:
        text = c.get("comment_text") or ""
        header = _format_header("Comentario original de", c.get("user"), c.get("date"))
        text = f"{header}\n{text}" if text else header
        created = _create_task_comment(dest_task_id, text)
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
            text = r.get("comment_text") or ""
            header = _format_header("Resposta original de", r.get("user"), r.get("date"))
            text = f"{header}\n{text}" if text else header
            _create_comment_reply(dest_parent, text)
