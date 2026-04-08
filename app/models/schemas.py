"""Schemas Pydantic para o payload do webhook do ClickUp."""

from pydantic import BaseModel


class WebhookHistoryItem(BaseModel):
    """Um item do array history_items no payload do webhook."""
    id: str | None = None
    field: str | None = None
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
