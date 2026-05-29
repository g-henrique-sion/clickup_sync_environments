"""Schemas Pydantic para o payload do webhook do ClickUp."""

from pydantic import BaseModel


class WebhookHistoryItem(BaseModel):
    """Um item do array history_items no payload do webhook."""
    id: str | None = None
    field: str | None = None
    date: str | int | None = None
    before: dict | str | None = None
    after: dict | str | None = None


class WebhookPayload(BaseModel):
    """Payload raiz do webhook do ClickUp.

    Docs: https://clickup.com/api/developer-portal/webhooktaskstatusupdate
    """
    event: str
    task_id: str | None = None
    history_items: list[WebhookHistoryItem] = []
    webhook_id: str | None = None

    def get_new_status(self) -> str | None:
        """Extrai o novo status do history_items (campo 'status')."""
        for item in self.history_items:
            if item.field == "status" and isinstance(item.after, dict):
                return item.after.get("status")
            if item.field == "status" and isinstance(item.after, str):
                return item.after
        return None

    def get_old_status(self) -> str | None:
        """Extrai o status anterior do history_items (campo 'status')."""
        for item in self.history_items:
            if item.field == "status" and isinstance(item.before, dict):
                return item.before.get("status")
            if item.field == "status" and isinstance(item.before, str):
                return item.before
        return None

    def get_status_change_date_ms(self) -> int | None:
        """Extrai timestamp em ms da troca de status no history_items."""
        for item in self.history_items:
            if item.field != "status":
                continue
            raw_date = item.date
            if raw_date is None:
                continue
            try:
                return int(str(raw_date).strip())
            except (TypeError, ValueError):
                continue
        return None
