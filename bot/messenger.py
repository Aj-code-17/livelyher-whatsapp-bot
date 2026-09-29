"""Meta Graph API client for the WhatsApp Cloud API.

  POST https://graph.facebook.com/v21.0/{phone_number_id}/messages
  {"messaging_product": "whatsapp", "to": "<phone>", "type": "text", "text": {"body": "..."}}
"""

from __future__ import annotations

import hashlib
import hmac
import logging

import requests

log = logging.getLogger("messenger")

GRAPH = "https://graph.facebook.com/v21.0"


class MetaClient:
    def __init__(self, page_access_token: str, app_secret: str = ""):
        self.token = page_access_token
        self.app_secret = app_secret

    # ------------------------------------------------------ WhatsApp Cloud API
    def send_whatsapp_text(self, phone_number_id: str, to: str, text: str) -> None:
        if not phone_number_id:
            log.error("send_whatsapp_text called with no phone_number_id!")
            return
        resp = requests.post(
            f"{GRAPH}/{phone_number_id}/messages",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            json={
                "messaging_product": "whatsapp",
                "to": to,
                "type": "text",
                "text": {"body": text[:4096]},  # WhatsApp per-message limit
            },
            timeout=15,
        )
        if not resp.ok:
            log.error("WA send failed: %s %s", resp.status_code, resp.text)
        resp.raise_for_status()

    def mark_whatsapp_read(self, phone_number_id: str, message_id: str) -> None:
        """Blue-ticks the incoming message."""
        try:
            requests.post(
                f"{GRAPH}/{phone_number_id}/messages",
                headers={"Authorization": f"Bearer {self.token}"},
                json={"messaging_product": "whatsapp",
                      "status": "read", "message_id": message_id},
                timeout=10,
            )
        except requests.RequestException:
            pass

    # ---------------------------------------------------------- validation
    def verify_signature(self, body: bytes, signature_header: str | None) -> bool:
        """Verify X-Hub-Signature-256. No app secret configured → dev mode, allow."""
        if not self.app_secret:
            return True
        if not signature_header or not signature_header.startswith("sha256="):
            return False
        expected = hmac.new(self.app_secret.encode(), body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, signature_header[len("sha256="):])
