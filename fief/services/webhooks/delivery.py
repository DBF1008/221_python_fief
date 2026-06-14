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

    async def deliver(
        self,
        webhook: Webhook,
        event: WebhookEvent,
        attempt: int = 1,
        payload: str | None = None,
    ):
        async with httpx.AsyncClient() as client:
            # When replaying, reuse the original serialized payload verbatim so
            # the bytes delivered on the wire match what was originally sent and
            # the HMAC signature corresponds to those exact bytes.
            serialized_payload = payload if payload is not None else event.model_dump_json()
            signature, ts = self._get_signature(serialized_payload, webhook.secret)

            webhook_log = WebhookLog(
                webhook_id=webhook.id,
                event=event.type,
                attempt=attempt,
                payload=serialized_payload,
                success=False,
            )

            try:
                response = await client.post(
                    webhook.url,
                    content=serialized_payload,
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
