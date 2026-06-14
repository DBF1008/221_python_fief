import asyncio
from collections.abc import Awaitable, Callable, Hashable
from typing import TYPE_CHECKING, Any, TypeVar, cast

if TYPE_CHECKING:
    from httpx_oauth.oauth2 import BaseOAuth2

    from fief.models.oauth_account import OAuthAccount

T = TypeVar("T")


class SingleFlight:
    """Coalesce concurrent async calls that share a key into a single execution.

    When several callers invoke :meth:`run` with the same ``key`` while a previous
    call for that key is still in flight, the wrapped function is executed only
    *once* and every caller receives the same result (or the same exception). The
    in-flight entry is discarded as soon as the execution settles, so a *later*
    call performs the work again — this coalesces concurrent calls only and never
    caches results.
    """

    def __init__(self) -> None:
        self._in_flight: dict[Hashable, asyncio.Task[Any]] = {}

    async def run(self, key: Hashable, func: Callable[[], Awaitable[T]]) -> T:
        task = self._in_flight.get(key)
        if task is None:
            task = asyncio.ensure_future(self._guard(func))
            self._in_flight[key] = task
            # Drop the entry once the execution settles so subsequent (non
            # concurrent) calls run the work afresh.
            task.add_done_callback(
                lambda finished, key=key: self._in_flight.pop(key, None)
            )
        # ``shield`` so that one caller's cancellation (e.g. a disconnected HTTP
        # client) does not cancel the shared execution the other callers await.
        return await asyncio.shield(task)

    @staticmethod
    async def _guard(func: Callable[[], Awaitable[T]]) -> T:
        # Wrapping in a coroutine guarantees a *synchronous* raise from ``func``
        # is captured by the task and propagated to every awaiter, exactly like an
        # asynchronous one — and that the in-flight entry exists before any raise.
        return await func()


_oauth_account_refresh_single_flight = SingleFlight()


async def refresh_oauth_account_access_token(
    oauth_account: "OAuthAccount", oauth_provider_service: "BaseOAuth2"
) -> dict[str, Any]:
    """Refresh an expired OAuth account's access token, coalescing concurrent calls.

    Concurrent refreshes for the same :class:`OAuthAccount` share a single call to
    the provider's ``refresh_token`` (keyed by ``oauth_account.id``), so the same
    refresh token is not spent multiple times and the external provider is not hit
    redundantly (avoiding rate limiting and write-back races). Any
    ``RefreshTokenError`` / ``RefreshTokenNotSupportedError`` raised by the provider
    is propagated unchanged to every waiter.

    This is an in-process coalescer (per worker / event loop); callers remain
    responsible for persisting the result and for deciding when a refresh is needed
    (via :meth:`OAuthAccount.is_expired`).
    """
    return await _oauth_account_refresh_single_flight.run(
        oauth_account.id,
        lambda: oauth_provider_service.refresh_token(
            cast(str, oauth_account.refresh_token)
        ),
    )
