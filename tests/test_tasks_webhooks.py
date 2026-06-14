import uuid
from unittest.mock import MagicMock

import httpx
import pytest
import respx
from dramatiq import Message
from dramatiq.middleware import CurrentMessage
from pytest_mock import MockerFixture
from sqlalchemy import select

from fief.db import AsyncSession
from fief.models import WebhookLog
from fief.repositories import WebhookLogRepository
from fief.services.webhooks.delivery import WebhookDeliveryError
from fief.services.webhooks.models import (
    ClientCreated,
    UserCreated,
    UserRoleDeleted,
    WebhookEvent,
)
from fief.tasks.base import ObjectDoesNotExistTaskError
from fief.tasks.webhooks import (
    DeliverWebhookTask,
    ReplayWebhookTask,
    TriggerWebhooksTask,
)
from tests.data import TestData


@pytest.fixture
def webhook_event() -> WebhookEvent:
    return WebhookEvent(type=ClientCreated.key(), data={})


@pytest.mark.asyncio
class TestTasksDeliverWebhook:
    async def test_deliver_success(
        self,
        mocker: MockerFixture,
        respx_mock: respx.MockRouter,
        webhook_event: WebhookEvent,
        main_session_manager,
        test_data: TestData,
    ):
        get_current_message_mock = mocker.patch.object(
            CurrentMessage, "get_current_message"
        )
        get_current_message_mock.return_value = Message("queue", "actor", (), {}, {})

        webhook = test_data["webhooks"]["all"]
        route_mock = respx_mock.post(webhook.url).mock(return_value=httpx.Response(200))

        deliver_webhook = DeliverWebhookTask(main_session_manager)

        await deliver_webhook.run(str(webhook.id), webhook_event.model_dump_json())

        assert route_mock.called

    async def test_deliver_error(
        self,
        mocker: MockerFixture,
        respx_mock: respx.MockRouter,
        webhook_event: WebhookEvent,
        main_session_manager,
        test_data: TestData,
    ):
        get_current_message_mock = mocker.patch.object(
            CurrentMessage, "get_current_message"
        )
        get_current_message_mock.return_value = Message("queue", "actor", (), {}, {})

        webhook = test_data["webhooks"]["all"]
        respx_mock.post(webhook.url).mock(return_value=httpx.Response(400))

        deliver_webhook = DeliverWebhookTask(main_session_manager)

        with pytest.raises(WebhookDeliveryError):
            await deliver_webhook.run(str(webhook.id), webhook_event.model_dump_json())


@pytest.mark.asyncio
class TestTasksTriggerWebhooks:
    async def test_client_created_event(
        self,
        main_session_manager,
        test_data: TestData,
        send_task_mock: MagicMock,
    ):
        webhook_event = WebhookEvent(type=ClientCreated.key(), data={})

        trigger_webhooks = TriggerWebhooksTask(
            main_session_manager, send_task=send_task_mock
        )

        await trigger_webhooks.run(webhook_event.model_dump_json())

        assert send_task_mock.call_count == 1
        assert send_task_mock.call_args[1]["webhook_id"] == str(
            test_data["webhooks"]["all"].id
        )

    async def test_user_registered_event(
        self,
        main_session_manager,
        test_data: TestData,
        send_task_mock: MagicMock,
    ):
        webhook_event = WebhookEvent(type=UserCreated.key(), data={})

        trigger_webhooks = TriggerWebhooksTask(
            main_session_manager, send_task=send_task_mock
        )

        await trigger_webhooks.run(webhook_event.model_dump_json())

        assert send_task_mock.call_count == 2
        webhook_ids = [
            call_arg[1]["webhook_id"] for call_arg in send_task_mock.call_args_list
        ]
        assert str(test_data["webhooks"]["all"].id) in webhook_ids
        assert str(test_data["webhooks"]["user_created"].id) in webhook_ids

    async def test_user_role_deleted_event(
        self,
        main_session_manager,
        test_data: TestData,
        send_task_mock: MagicMock,
    ):
        webhook_event = WebhookEvent(type=UserRoleDeleted.key(), data={})

        trigger_webhooks = TriggerWebhooksTask(
            main_session_manager, send_task=send_task_mock
        )

        await trigger_webhooks.run(webhook_event.model_dump_json())

        assert send_task_mock.call_count == 2
        webhook_ids = [
            call_arg[1]["webhook_id"] for call_arg in send_task_mock.call_args_list
        ]
        assert str(test_data["webhooks"]["all"].id) in webhook_ids
        assert str(test_data["webhooks"]["object_user_role"].id) in webhook_ids


@pytest.mark.asyncio
class TestTasksReplayWebhook:
    async def test_replay_success(
        self,
        mocker: MockerFixture,
        respx_mock: respx.MockRouter,
        main_session_manager,
        main_session: AsyncSession,
        test_data: TestData,
    ):
        get_current_message_mock = mocker.patch.object(
            CurrentMessage, "get_current_message"
        )
        get_current_message_mock.return_value = Message("queue", "actor", (), {}, {})

        webhook = test_data["webhooks"]["all"]
        webhook_log = test_data["webhook_logs"]["all_log1"]
        route_mock = respx_mock.post(webhook.url).mock(return_value=httpx.Response(200))

        replay_webhook = ReplayWebhookTask(main_session_manager)
        await replay_webhook.run(str(webhook.id), str(webhook_log.id))

        assert route_mock.called

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
        assert new_log.attempt == 1
        assert new_log.success

        original = await webhook_log_repository.get_by_id(webhook_log.id)
        assert original is not None
        assert original.success is True

    async def test_replay_failure(
        self,
        mocker: MockerFixture,
        respx_mock: respx.MockRouter,
        main_session_manager,
        main_session: AsyncSession,
        test_data: TestData,
    ):
        get_current_message_mock = mocker.patch.object(
            CurrentMessage, "get_current_message"
        )
        get_current_message_mock.return_value = Message("queue", "actor", (), {}, {})

        webhook = test_data["webhooks"]["all"]
        webhook_log = test_data["webhook_logs"]["all_log1"]
        respx_mock.post(webhook.url).mock(return_value=httpx.Response(400))

        replay_webhook = ReplayWebhookTask(main_session_manager)
        with pytest.raises(WebhookDeliveryError):
            await replay_webhook.run(str(webhook.id), str(webhook_log.id))

        webhook_log_repository = WebhookLogRepository(main_session)
        webhook_logs = await webhook_log_repository.list(
            select(WebhookLog).order_by(WebhookLog.created_at.desc())
        )
        assert len(webhook_logs) == len(test_data["webhook_logs"]) + 1

        new_log = webhook_logs[0]
        assert new_log.id != webhook_log.id
        assert not new_log.success
        assert new_log.error_type == "HTTPStatusError"

        # Replaying a failing target leaves the original log untouched.
        original = await webhook_log_repository.get_by_id(webhook_log.id)
        assert original is not None
        assert original.success is True

    async def test_replay_not_existing_webhook(
        self,
        main_session_manager,
        not_existing_uuid: uuid.UUID,
        test_data: TestData,
    ):
        webhook_log = test_data["webhook_logs"]["all_log1"]

        replay_webhook = ReplayWebhookTask(main_session_manager)
        with pytest.raises(ObjectDoesNotExistTaskError):
            await replay_webhook.run(str(not_existing_uuid), str(webhook_log.id))

    async def test_replay_not_existing_log(
        self,
        main_session_manager,
        not_existing_uuid: uuid.UUID,
        test_data: TestData,
    ):
        webhook = test_data["webhooks"]["all"]

        replay_webhook = ReplayWebhookTask(main_session_manager)
        with pytest.raises(ObjectDoesNotExistTaskError):
            await replay_webhook.run(str(webhook.id), str(not_existing_uuid))
