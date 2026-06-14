"""Regression tests for the single-flight token-refresh mechanism in
``fief.apps.api.routers.oauth_providers.get_user_access_token``.

Scenarios covered:
    1. Concurrent requests for the same expired account trigger **one**
       external ``refresh_token`` call and all receive the same result.
    2. A refresh failure is propagated to **every** concurrent waiter.
    3. After a successful concurrent refresh, a later request sees the
       persisted (now-fresh) token and does **not** call the provider again.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import status
from httpx_oauth.oauth2 import BaseOAuth2, RefreshTokenError
from pytest_mock import MockerFixture

from fief.apps.api.routers import oauth_providers as oauth_providers_module
from fief.db import AsyncSession
from fief.errors import APIErrorCode
from fief.repositories import OAuthAccountRepository
from tests.data import TestData


@pytest.fixture(autouse=True)
def _clean_singleflight_state():
    """Ensure module-level single-flight dicts are empty around each test."""
    oauth_providers_module._refresh_locks.clear()
    oauth_providers_module._refresh_results.clear()
    oauth_providers_module._refresh_holder_count.clear()
    yield
    oauth_providers_module._refresh_locks.clear()
    oauth_providers_module._refresh_results.clear()
    oauth_providers_module._refresh_holder_count.clear()


@pytest.mark.asyncio
class TestConcurrentRefreshSingleFlight:
    """Concurrent requests for the same expired account must share one refresh."""

    @pytest.mark.authenticated_admin
    async def test_concurrent_refresh_single_flight(
        self,
        mocker: MockerFixture,
        test_client_api: httpx.AsyncClient,
        test_data: TestData,
    ):
        expires_at = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=3600)
        refresh_result = {
            "access_token": "REFRESHED_ACCESS_TOKEN",
            "expires_in": 3600,
            "expires_at": int(expires_at.timestamp()),
        }

        async def _delayed_refresh(*args, **kwargs):
            # Simulate network latency so concurrent requests pile up on the lock.
            await asyncio.sleep(0.5)
            return refresh_result

        oauth_provider_service_mock = MagicMock(spec=BaseOAuth2)
        oauth_provider_service_mock.refresh_token = AsyncMock(
            side_effect=_delayed_refresh
        )
        mocker.patch(
            "fief.apps.api.routers.oauth_providers.get_oauth_provider_service"
        ).return_value = oauth_provider_service_mock

        oauth_provider = test_data["oauth_providers"]["openid"]
        user = test_data["users"]["regular"]
        url = f"/oauth-providers/{oauth_provider.id}/access-token/{user.id}"

        responses = await asyncio.gather(
            *(test_client_api.get(url) for _ in range(5))
        )

        # The provider was called exactly once despite 5 concurrent requests.
        assert oauth_provider_service_mock.refresh_token.call_count == 1

        for response in responses:
            assert response.status_code == status.HTTP_200_OK
            body = response.json()
            assert body["access_token"] == "REFRESHED_ACCESS_TOKEN"
            assert body["expires_at"] is not None

    @pytest.mark.authenticated_admin
    async def test_concurrent_refresh_failure_propagation(
        self,
        mocker: MockerFixture,
        test_client_api: httpx.AsyncClient,
        test_data: TestData,
    ):
        async def _delayed_fail(*args, **kwargs):
            await asyncio.sleep(0.3)
            raise RefreshTokenError("PROVIDER_DOWN")

        oauth_provider_service_mock = MagicMock(spec=BaseOAuth2)
        oauth_provider_service_mock.refresh_token = AsyncMock(
            side_effect=_delayed_fail
        )
        mocker.patch(
            "fief.apps.api.routers.oauth_providers.get_oauth_provider_service"
        ).return_value = oauth_provider_service_mock

        oauth_provider = test_data["oauth_providers"]["openid"]
        user = test_data["users"]["regular"]
        url = f"/oauth-providers/{oauth_provider.id}/access-token/{user.id}"

        responses = await asyncio.gather(
            *(test_client_api.get(url) for _ in range(3))
        )

        # Only one attempt was made.
        assert oauth_provider_service_mock.refresh_token.call_count == 1

        # Every waiter received the same error.
        for response in responses:
            assert response.status_code == status.HTTP_400_BAD_REQUEST
            assert (
                response.json()["detail"]
                == APIErrorCode.OAUTH_PROVIDER_REFRESH_TOKEN_ERROR
            )


@pytest.mark.asyncio
class TestPostRefreshReuse:
    """After a successful refresh, a later request should not call the provider."""

    @pytest.mark.authenticated_admin
    async def test_post_refresh_reuse(
        self,
        mocker: MockerFixture,
        test_client_api: httpx.AsyncClient,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        expires_at = datetime.now(UTC).replace(microsecond=0) + timedelta(seconds=3600)
        refresh_result = {
            "access_token": "REFRESHED_ACCESS_TOKEN",
            "expires_in": 3600,
            "expires_at": int(expires_at.timestamp()),
        }

        oauth_provider_service_mock = MagicMock(spec=BaseOAuth2)
        oauth_provider_service_mock.refresh_token = AsyncMock(
            return_value=refresh_result
        )
        mocker.patch(
            "fief.apps.api.routers.oauth_providers.get_oauth_provider_service"
        ).return_value = oauth_provider_service_mock

        oauth_provider = test_data["oauth_providers"]["openid"]
        user = test_data["users"]["regular"]
        url = f"/oauth-providers/{oauth_provider.id}/access-token/{user.id}"

        # --- Phase 1: concurrent burst ---
        phase1_responses = await asyncio.gather(
            *(test_client_api.get(url) for _ in range(3))
        )

        assert oauth_provider_service_mock.refresh_token.call_count == 1
        for resp in phase1_responses:
            assert resp.status_code == status.HTTP_200_OK
            assert resp.json()["access_token"] == "REFRESHED_ACCESS_TOKEN"

        # --- Phase 2: single follow-up request ---
        # The first batch committed the refreshed token to the DB, so
        # is_expired() should now be False and the provider must NOT be
        # called again.
        response = await test_client_api.get(url)

        assert oauth_provider_service_mock.refresh_token.call_count == 1  # unchanged
        assert response.status_code == status.HTTP_200_OK
        assert response.json()["access_token"] == "REFRESHED_ACCESS_TOKEN"

        # Verify the DB actually holds the refreshed token.
        oauth_account = test_data["oauth_accounts"]["regular_openid_expired"]
        repository = OAuthAccountRepository(main_session)
        updated = await repository.get_by_id(oauth_account.id)
        assert updated is not None
        assert updated.access_token == "REFRESHED_ACCESS_TOKEN"
        assert updated.expires_at == expires_at
