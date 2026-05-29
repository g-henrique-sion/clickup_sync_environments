"""Compatibility layer for webhook-event processing.

The automation logic now lives in `app.automations.*`.
This module is kept to avoid changing existing imports.
"""

from app.automations.engine import process_clickup_event, process_status_change

__all__ = ["process_clickup_event", "process_status_change"]
