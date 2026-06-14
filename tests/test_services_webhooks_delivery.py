import httpx
import pytest
import respx
from sqlalchemy import select

from fief import __version__, schemas
from fief.db import AsyncSession
from fief.models import WebhookLog
from fief.repositories import WebhookLogRepository
from fief.services.webhooks.delivery import WebhookDelivery, WebhookDeliveryError
from fief.services.webhooks.models import ClientCreated, WebhookEvent
from tests.data import TestData


@pytest.fixture
def webhook_delivery(main_session: AsyncSession) -> WebhookDelivery:
    return WebhookDelivery(WebhookLogRepository(main_session))


@pytest.fixture
def webhook_event(test_data: TestData) -> WebhookEvent:
    object = test_data["clients"]["default_tenant"]
    return WebhookEvent(
        type=ClientCreated.key(),
        data=schemas.client.Client.model_validate(object).model_dump(),
    )


@pytest.mark.asyncio
class TestWebhookDelivery:
    async def test_deliver_success(
        self,
        respx_mock: respx.MockRouter,
        webhook_delivery: WebhookDelivery,
        webhook_event: WebhookEvent,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        webhook = test_data["webhooks"]["all"]
        route_mock = respx_mock.post(webhook.url).mock(
            return_value=httpx.Response(200, text="Ok")
        )

        await webhook_delivery.deliver(webhook, webhook_event)

        assert route_mock.called
        request, _ = route_mock.calls.last

        assert "X-Fief-Webhook-Signature" in request.headers
        assert "X-Fief-Webhook-Timestamp" in request.headers
        assert request.content == webhook_event.model_dump_json().encode("utf-8")

        assert request.headers["user-agent"] == f"fief-server-webhooks/{__version__}"

        webhook_log_repository = WebhookLogRepository(main_session)
        webhook_logs = await webhook_log_repository.list(
            select(WebhookLog).order_by(WebhookLog.created_at.desc())
        )

        assert len(webhook_logs) == len(test_data["webhook_logs"]) + 1
        webhook_log = webhook_logs[0]
        assert webhook_log.webhook_id == webhook.id
        assert webhook_log.event == webhook_event.type
        assert webhook_log.response == "Ok"
        assert webhook_log.error_type is None
        assert webhook_log.error_message is None
        assert webhook_log.success

    async def test_deliver_status_error(
        self,
        respx_mock: respx.MockRouter,
        webhook_delivery: WebhookDelivery,
        webhook_event: WebhookEvent,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        webhook = test_data["webhooks"]["all"]
        route_mock = respx_mock.post(webhook.url).mock(
            return_value=httpx.Response(400, text="Bad Request")
        )

        with pytest.raises(WebhookDeliveryError):
            await webhook_delivery.deliver(webhook, webhook_event)

        assert route_mock.called

        webhook_log_repository = WebhookLogRepository(main_session)
        webhook_logs = await webhook_log_repository.list(
            select(WebhookLog).order_by(WebhookLog.created_at.desc())
        )

        assert len(webhook_logs) == len(test_data["webhook_logs"]) + 1
        webhook_log = webhook_logs[0]
        assert webhook_log.webhook_id == webhook.id
        assert webhook_log.event == webhook_event.type
        assert webhook_log.response == "Bad Request"
        assert webhook_log.error_type == "HTTPStatusError"
        assert webhook_log.error_message is not None
        assert "Client error '400 Bad Request'" in webhook_log.error_message
        assert not webhook_log.success

    async def test_deliver_general_error(
        self,
        respx_mock: respx.MockRouter,
        webhook_delivery: WebhookDelivery,
        webhook_event: WebhookEvent,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        webhook = test_data["webhooks"]["all"]
        route_mock = respx_mock.post(webhook.url).mock(
            side_effect=httpx.HTTPError("Something went wrong")
        )

        with pytest.raises(WebhookDeliveryError):
            await webhook_delivery.deliver(webhook, webhook_event)

        assert route_mock.called

        webhook_log_repository = WebhookLogRepository(main_session)
        webhook_logs = await webhook_log_repository.list(
            select(WebhookLog).order_by(WebhookLog.created_at.desc())
        )

        assert len(webhook_logs) == len(test_data["webhook_logs"]) + 1
        webhook_log = webhook_logs[0]
        assert webhook_log.webhook_id == webhook.id
        assert webhook_log.event == webhook_event.type
        assert webhook_log.response is None
        assert webhook_log.error_type == "HTTPError"
        assert webhook_log.error_message == "Something went wrong"
        assert not webhook_log.success


@pytest.mark.asyncio
class TestWebhookReplay:
    async def test_replay_success(
        self,
        respx_mock: respx.MockRouter,
        webhook_delivery: WebhookDelivery,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        webhook = test_data["webhooks"]["all"]
        webhook_log = test_data["webhook_logs"]["all_log1"]
        route_mock = respx_mock.post(webhook.url).mock(
            return_value=httpx.Response(200, text="Ok")
        )

        await webhook_delivery.replay(webhook, webhook_log)

        assert route_mock.called
        request, _ = route_mock.calls.last
        # The original payload bytes are re-sent verbatim.
        assert request.content == webhook_log.payload.encode("utf-8")

        webhook_log_repository = WebhookLogRepository(main_session)
        webhook_logs = await webhook_log_repository.list(
            select(WebhookLog).order_by(WebhookLog.created_at.desc())
        )
        assert len(webhook_logs) == len(test_data["webhook_logs"]) + 1

        new_log = webhook_logs[0]
        assert new_log.id != webhook_log.id
        assert new_log.webhook_id == webhook.id
        assert new_log.event == webhook_log.event
        assert new_log.payload == webhook_log.payload
        assert new_log.response == "Ok"
        assert new_log.success

        # The original log must be left untouched.
        original = await webhook_log_repository.get_by_id(webhook_log.id)
        assert original is not None
        assert original.success is True
        assert original.attempt == webhook_log.attempt

    async def test_replay_status_error(
        self,
        respx_mock: respx.MockRouter,
        webhook_delivery: WebhookDelivery,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        webhook = test_data["webhooks"]["all"]
        webhook_log = test_data["webhook_logs"]["all_log1"]
        respx_mock.post(webhook.url).mock(
            return_value=httpx.Response(400, text="Bad Request")
        )

        with pytest.raises(WebhookDeliveryError):
            await webhook_delivery.replay(webhook, webhook_log)

        webhook_log_repository = WebhookLogRepository(main_session)
        webhook_logs = await webhook_log_repository.list(
            select(WebhookLog).order_by(WebhookLog.created_at.desc())
        )
        assert len(webhook_logs) == len(test_data["webhook_logs"]) + 1

        new_log = webhook_logs[0]
        assert new_log.id != webhook_log.id
        assert new_log.success is False
        assert new_log.error_type == "HTTPStatusError"

        # The original (successful) log must be left untouched.
        original = await webhook_log_repository.get_by_id(webhook_log.id)
        assert original is not None
        assert original.success is True
