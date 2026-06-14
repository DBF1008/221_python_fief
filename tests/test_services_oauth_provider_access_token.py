import asyncio
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx_oauth.oauth2 import BaseOAuth2, RefreshTokenError

from fief.services.oauth_provider_access_token import (
    SingleFlight,
    refresh_oauth_account_access_token,
)

pytestmark = pytest.mark.asyncio


async def _drain() -> None:
    """Let pending ``call_soon`` callbacks (e.g. done-callbacks) run."""
    for _ in range(3):
        await asyncio.sleep(0)


class TestSingleFlight:
    async def test_concurrent_calls_share_single_execution(self):
        single_flight = SingleFlight()
        calls = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def work() -> str:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return "RESULT"

        tasks = [asyncio.create_task(single_flight.run("key", work)) for _ in range(5)]
        # All callers are now parked on the single in-flight execution.
        await started.wait()
        release.set()
        results = await asyncio.gather(*tasks)

        # The wrapped work ran exactly once and every caller got the same result.
        assert calls == 1
        assert results == ["RESULT"] * 5

    async def test_distinct_keys_run_independently(self):
        single_flight = SingleFlight()
        calls: list[str] = []
        release = asyncio.Event()

        async def work(tag: str) -> str:
            calls.append(tag)
            await release.wait()
            return tag

        tasks = [
            asyncio.create_task(single_flight.run(key, lambda key=key: work(key)))
            for key in ("a", "b", "c")
        ]
        await _drain()  # let each distinct-key execution begin
        release.set()
        results = await asyncio.gather(*tasks)

        # Different keys are not coalesced: each runs on its own.
        assert sorted(results) == ["a", "b", "c"]
        assert sorted(calls) == ["a", "b", "c"]

    async def test_failure_propagates_to_all_and_clears(self):
        single_flight = SingleFlight()
        calls = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def failing() -> str:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            raise RefreshTokenError("boom")

        tasks = [
            asyncio.create_task(single_flight.run("key", failing)) for _ in range(4)
        ]
        await started.wait()
        release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # The single failure is delivered to every concurrent waiter.
        assert calls == 1
        assert all(isinstance(result, RefreshTokenError) for result in results)

        # The in-flight entry is dropped, so a later call runs the work again
        # (failures are not cached).
        await _drain()
        assert single_flight._in_flight == {}

        async def succeeding() -> str:
            nonlocal calls
            calls += 1
            return "OK"

        assert await single_flight.run("key", succeeding) == "OK"
        assert calls == 2

    async def test_settled_flight_is_not_cached(self):
        single_flight = SingleFlight()
        calls = 0

        async def work() -> int:
            nonlocal calls
            calls += 1
            return calls

        # Sequential calls each run the work afresh — coalescing is for
        # concurrent calls only, never a result cache.
        assert await single_flight.run("key", work) == 1
        assert await single_flight.run("key", work) == 2
        assert calls == 2

    async def test_synchronous_raise_is_propagated(self):
        single_flight = SingleFlight()

        def boom() -> object:
            raise RefreshTokenError("sync")

        # A function that raises *before* returning an awaitable still surfaces
        # the error to the caller.
        with pytest.raises(RefreshTokenError):
            await single_flight.run("key", boom)
        await _drain()
        assert single_flight._in_flight == {}


class TestRefreshOAuthAccountAccessToken:
    async def test_concurrent_refresh_hits_provider_once(self):
        token = {"access_token": "ACCESS_TOKEN", "expires_at": 1_700_000_000}
        started = asyncio.Event()
        release = asyncio.Event()

        async def fake_refresh(refresh_token: str) -> dict:
            started.set()
            await release.wait()
            return token

        oauth_provider_service = MagicMock(spec=BaseOAuth2)
        oauth_provider_service.refresh_token = AsyncMock(side_effect=fake_refresh)
        oauth_account = SimpleNamespace(id=uuid.uuid4(), refresh_token="REFRESH_TOKEN")

        tasks = [
            asyncio.create_task(
                refresh_oauth_account_access_token(
                    oauth_account, oauth_provider_service
                )
            )
            for _ in range(5)
        ]
        await started.wait()
        release.set()
        results = await asyncio.gather(*tasks)

        # Concurrent callers for one account share a single provider refresh,
        # and all receive the same token payload.
        assert oauth_provider_service.refresh_token.call_count == 1
        oauth_provider_service.refresh_token.assert_awaited_with("REFRESH_TOKEN")
        assert all(result is token for result in results)

        # Once the flight has settled, a later refresh hits the provider again
        # (reuse of a still-valid token is the caller's responsibility).
        assert (
            await refresh_oauth_account_access_token(
                oauth_account, oauth_provider_service
            )
            is token
        )
        assert oauth_provider_service.refresh_token.call_count == 2

    async def test_refresh_error_propagates_to_all_waiters(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def failing_refresh(refresh_token: str) -> dict:
            started.set()
            await release.wait()
            raise RefreshTokenError("boom")

        oauth_provider_service = MagicMock(spec=BaseOAuth2)
        oauth_provider_service.refresh_token = AsyncMock(side_effect=failing_refresh)
        oauth_account = SimpleNamespace(id=uuid.uuid4(), refresh_token="REFRESH_TOKEN")

        tasks = [
            asyncio.create_task(
                refresh_oauth_account_access_token(
                    oauth_account, oauth_provider_service
                )
            )
            for _ in range(3)
        ]
        await started.wait()
        release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # A single failed provider call surfaces to every concurrent waiter,
        # preserving the original error type for the router's error mapping.
        assert oauth_provider_service.refresh_token.call_count == 1
        assert all(isinstance(result, RefreshTokenError) for result in results)
