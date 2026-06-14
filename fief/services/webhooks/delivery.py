import hmac
import time
from hashlib import sha256

import httpx

from fief import __version__
from fief.models import Webhook, WebhookLog
from fief.repositories import WebhookLogRepository
from fief.services.webhooks.models import WebhookEvent


class WebhookDeliveryError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class WebhookDelivery:
    def __init__(self, webhook_log_repository: WebhookLogRepository) -> None:
        self.webhook_log_repository = webhook_log_repository

    async def deliver(self, webhook: Webhook, event: WebhookEvent, attempt: int = 1):
        await self._send(webhook, event.type, event.model_dump_json(), attempt)

    async def replay(
        self, webhook: Webhook, webhook_log: WebhookLog, attempt: int = 1
    ) -> None:
        """Re-deliver the original payload of a past `WebhookLog`.

        The stored payload bytes are re-sent as-is (with a fresh signature) and a
        brand-new `WebhookLog` is recorded for this attempt. The original log is never
        mutated.
        """
        await self._send(webhook, webhook_log.event, webhook_log.payload, attempt)

    async def _send(
        self, webhook: Webhook, event_type: str, payload: str, attempt: int
    ) -> None:
        async with httpx.AsyncClient() as client:
            signature, ts = self._get_signature(payload, webhook.secret)

            webhook_log = WebhookLog(
                webhook_id=webhook.id,
                event=event_type,
                attempt=attempt,
                payload=payload,
                success=False,
            )

            try:
                response = await client.post(
                    webhook.url,
                    content=payload,
                    headers={
                        "User-Agent": f"fief-server-webhooks/{__version__}",
                        "Content-Type": "application/json",
                        "X-Fief-Webhook-Signature": signature,
                        "X-Fief-Webhook-Timestamp": str(ts),
                    },
                    follow_redirects=False,
                )
                webhook_log.response = response.text
                response.raise_for_status()
                webhook_log.success = True
            except httpx.HTTPError as e:
                webhook_log.error_type = type(e).__name__
                webhook_log.error_message = str(e)
                raise WebhookDeliveryError(str(e)) from e
            finally:
                await self.webhook_log_repository.create(webhook_log)

    def _get_signature(self, payload: str, secret: str) -> tuple[str, int]:
        ts = int(time.time())
        message = f"{ts}.{payload}"

        hash = hmac.new(
            secret.encode("utf-8"),
            msg=message.encode("utf-8"),
            digestmod=sha256,
        )
        signature = hash.hexdigest()
        return signature, ts
