"""CLI para gerenciar webhooks no ClickUp.

Uso:
    python manage_webhook.py create <TEAM_ID> <ENDPOINT_URL>
    python manage_webhook.py list   <TEAM_ID>
    python manage_webhook.py delete <WEBHOOK_ID>

Exemplos:
    python manage_webhook.py create 1234567 https://meu-app.up.railway.app/webhook
    python manage_webhook.py list 1234567
    python manage_webhook.py delete abc-123-def

O TEAM_ID é o ID do workspace de ORIGEM (onde as tasks mudam de status).
Para encontrar: ClickUp → Settings → canto inferior esquerdo mostra o Team ID,
ou use a API: GET https://api.clickup.com/api/v2/team
"""

import sys

import requests
from dotenv import load_dotenv

load_dotenv()

from app.config.settings import SOURCE_CLICKUP_TOKEN, SOURCE_LIST_ID

BASE_URL = "https://api.clickup.com/api/v2"


def _headers():
    return {"Authorization": SOURCE_CLICKUP_TOKEN, "Content-Type": "application/json"}


def create_webhook(team_id: str, endpoint: str) -> None:
    """Registra um webhook no ClickUp para taskStatusUpdated."""
    url = f"{BASE_URL}/team/{team_id}/webhook"
    payload = {
        "endpoint": endpoint,
        "events": ["taskStatusUpdated"],
        "status": "active",
    }

    resp = requests.post(url, json=payload, headers=_headers(), timeout=30)
    resp.raise_for_status()
    data = resp.json()

    webhook = data.get("webhook", data)
    print(f"\nWebhook criado com sucesso!")
    print(f"  ID:       {webhook.get('id')}")
    print(f"  Endpoint: {webhook.get('endpoint')}")
    print(f"  Events:   {webhook.get('events')}")
    print(f"  Secret:   {webhook.get('secret', 'N/A')}")
    print(f"\nGuarde o Secret no .env como WEBHOOK_SECRET para validação.")


def list_webhooks(team_id: str) -> None:
    """Lista todos os webhooks do workspace."""
    url = f"{BASE_URL}/team/{team_id}/webhook"
    resp = requests.get(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    data = resp.json()

    webhooks = data.get("webhooks", [])
    if not webhooks:
        print("Nenhum webhook encontrado.")
        return

    print(f"\n{len(webhooks)} webhook(s) encontrado(s):\n")
    for wh in webhooks:
        status = wh.get("status", "?")
        print(f"  ID:       {wh.get('id')}")
        print(f"  Endpoint: {wh.get('endpoint')}")
        print(f"  Events:   {wh.get('events')}")
        print(f"  Status:   {status}")
        print(f"  Health:   {wh.get('health', {})}")
        print()


def delete_webhook(webhook_id: str) -> None:
    """Remove um webhook pelo ID."""
    url = f"{BASE_URL}/webhook/{webhook_id}"
    resp = requests.delete(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    print(f"Webhook {webhook_id} removido com sucesso.")


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    if not SOURCE_CLICKUP_TOKEN:
        print("ERRO: SOURCE_CLICKUP_TOKEN não configurado no .env")
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "create":
        if len(sys.argv) < 4:
            print("Uso: python manage_webhook.py create <TEAM_ID> <ENDPOINT_URL>")
            sys.exit(1)
        create_webhook(sys.argv[2], sys.argv[3])

    elif cmd == "list":
        list_webhooks(sys.argv[2])

    elif cmd == "delete":
        delete_webhook(sys.argv[2])

    else:
        print(f"Comando desconhecido: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
