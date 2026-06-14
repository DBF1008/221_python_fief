import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.exceptions import RequestValidationError
from httpx_oauth.oauth2 import RefreshTokenError, RefreshTokenNotSupportedError
from pydantic import UUID4, ValidationError

from fief import schemas
from fief.dependencies.admin_authentication import is_authenticated_admin_api
from fief.dependencies.logger import get_audit_logger
from fief.dependencies.oauth_provider import (
    get_oauth_provider_by_id_or_404,
    get_paginated_oauth_providers,
)
from fief.dependencies.pagination import PaginatedObjects
from fief.dependencies.repositories import get_repository
from fief.dependencies.webhooks import TriggerWebhooks, get_trigger_webhooks
from fief.errors import APIErrorCode
from fief.logger import AuditLogger, logger
from fief.models import AuditLogMessage, OAuthProvider
from fief.models.oauth_account import OAuthAccount
from fief.repositories import (
    OAuthAccountRepository,
    OAuthProviderRepository,
    UserRepository,
)
from fief.schemas.generics import PaginatedResults
from fief.services.oauth_provider import get_oauth_provider_service
from fief.services.webhooks.models import (
    OAuthProviderCreated,
    OAuthProviderDeleted,
    OAuthProviderUpdated,
)

router = APIRouter(dependencies=[Depends(is_authenticated_admin_api)])

# ---------------------------------------------------------------------------
# Single-flight state for OAuth token refresh.
# Prevents concurrent refresh calls for the same OAuthAccount by serialising
# them through a per-account asyncio.Lock and sharing the first caller's
# result with all subsequent waiters via an in-memory cache.
# ---------------------------------------------------------------------------
_refresh_locks: dict[UUID, asyncio.Lock] = {}
_refresh_results: dict[UUID, dict] = {}
# Reference count: how many coroutines currently hold or are waiting on the
# per-account lock.  The cache entry is evicted only once the last holder
# exits, which avoids a race where A's finally-block cleans up before B has
# been scheduled to read the shared result.
_refresh_holder_count: dict[UUID, int] = {}


def _get_refresh_lock(account_id: UUID) -> asyncio.Lock:
    """Return the per-account lock, creating one lazily if needed."""
    lock = _refresh_locks.get(account_id)
    if lock is None:
        lock = asyncio.Lock()
        _refresh_locks[account_id] = lock
    return lock


@asynccontextmanager
async def _account_refresh_lock(account_id: UUID):
    """Async context-manager that holds the per-account refresh lock.

    A reference count tracks how many coroutines are inside the context
    manager (either holding the lock or queued on it).  The shared-result
    cache and the lock itself are evicted only when the *last* holder exits,
    guaranteeing that every waiter has had a chance to read the result.
    """
    lock = _get_refresh_lock(account_id)
    _refresh_holder_count[account_id] = _refresh_holder_count.get(account_id, 0) + 1
    await lock.acquire()
    try:
        yield
    finally:
        lock.release()
        _refresh_holder_count[account_id] -= 1
        if _refresh_holder_count[account_id] == 0:
            del _refresh_holder_count[account_id]
            _refresh_results.pop(account_id, None)
            _refresh_locks.pop(account_id, None)


@router.get(
    "/",
    name="oauth_providers:list",
    response_model=PaginatedResults[schemas.oauth_provider.OAuthProvider],
)
async def list_oauth_providers(
    paginated_oauth_providers: PaginatedObjects[OAuthProvider] = Depends(
        get_paginated_oauth_providers
    ),
) -> PaginatedResults[schemas.oauth_provider.OAuthProvider]:
    oauth_providers, count = paginated_oauth_providers
    return PaginatedResults(
        count=count,
        results=[
            schemas.oauth_provider.OAuthProvider.model_validate(oauth_provider)
            for oauth_provider in oauth_providers
        ],
    )


@router.get(
    "/{id:uuid}",
    name="oauth_providers:get",
    response_model=schemas.oauth_provider.OAuthProvider,
)
async def get_oauth_provider(
    oauth_provider: OAuthProvider = Depends(get_oauth_provider_by_id_or_404),
) -> OAuthProvider:
    return oauth_provider


@router.post(
    "/",
    name="oauth_providers:create",
    response_model=schemas.oauth_provider.OAuthProvider,
    status_code=status.HTTP_201_CREATED,
)
async def create_oauth_provider(
    oauth_provider_create: schemas.oauth_provider.OAuthProviderCreate,
    repository: OAuthProviderRepository = Depends(
        get_repository(OAuthProviderRepository)
    ),
    audit_logger: AuditLogger = Depends(get_audit_logger),
    trigger_webhooks: TriggerWebhooks = Depends(get_trigger_webhooks),
) -> schemas.oauth_provider.OAuthProvider:
    oauth_provider = OAuthProvider(**oauth_provider_create.model_dump())
    oauth_provider = await repository.create(oauth_provider)
    audit_logger.log_object_write(AuditLogMessage.OBJECT_CREATED, oauth_provider)
    trigger_webhooks(
        OAuthProviderCreated,
        oauth_provider,
        schemas.oauth_provider.OAuthProvider,
    )

    return schemas.oauth_provider.OAuthProvider.model_validate(oauth_provider)


@router.patch(
    "/{id:uuid}",
    name="oauth_providers:update",
    response_model=schemas.oauth_provider.OAuthProvider,
)
async def update_oauth_provider(
    oauth_provider_update: schemas.oauth_provider.OAuthProviderUpdate,
    oauth_provider: OAuthProvider = Depends(get_oauth_provider_by_id_or_404),
    repository: OAuthProviderRepository = Depends(
        get_repository(OAuthProviderRepository)
    ),
    audit_logger: AuditLogger = Depends(get_audit_logger),
    trigger_webhooks: TriggerWebhooks = Depends(get_trigger_webhooks),
) -> schemas.oauth_provider.OAuthProvider:
    oauth_provider_update_dict = oauth_provider_update.model_dump(exclude_unset=True)

    try:
        oauth_provider_update_provider = (
            schemas.oauth_provider.OAuthProviderUpdateProvider.model_validate(
                oauth_provider
            )
        )
        schemas.oauth_provider.OAuthProviderUpdateProvider(
            **oauth_provider_update_provider.model_copy(
                update=oauth_provider_update_dict
            ).model_dump()
        )
    except ValidationError as e:
        raise RequestValidationError(e.errors()) from e

    for field, value in oauth_provider_update_dict.items():
        setattr(oauth_provider, field, value)

    await repository.update(oauth_provider)
    audit_logger.log_object_write(AuditLogMessage.OBJECT_UPDATED, oauth_provider)
    trigger_webhooks(
        OAuthProviderUpdated,
        oauth_provider,
        schemas.oauth_provider.OAuthProvider,
    )

    return schemas.oauth_provider.OAuthProvider.model_validate(oauth_provider)


@router.delete(
    "/{id:uuid}",
    name="oauth_providers:delete",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def delete_oauth_provider(
    oauth_provider: OAuthProvider = Depends(get_oauth_provider_by_id_or_404),
    repository: OAuthProviderRepository = Depends(
        get_repository(OAuthProviderRepository)
    ),
    audit_logger: AuditLogger = Depends(get_audit_logger),
    trigger_webhooks: TriggerWebhooks = Depends(get_trigger_webhooks),
):
    await repository.delete(oauth_provider)
    audit_logger.log_object_write(AuditLogMessage.OBJECT_DELETED, oauth_provider)
    trigger_webhooks(
        OAuthProviderDeleted,
        oauth_provider,
        schemas.oauth_provider.OAuthProvider,
    )


@router.get(
    "/{id:uuid}/access-token/{user_id:uuid}",
    name="oauth_providers:get_user_access_token",
    response_model=schemas.oauth_account.OAuthAccountAccessToken,
)
async def get_user_access_token(
    user_id: UUID4,
    oauth_provider: OAuthProvider = Depends(get_oauth_provider_by_id_or_404),
    oauth_account_repository: OAuthAccountRepository = Depends(
        get_repository(OAuthAccountRepository)
    ),
    user_repository: UserRepository = Depends(UserRepository),
    audit_logger: AuditLogger = Depends(get_audit_logger),
) -> OAuthAccount:
    user = await user_repository.get_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    oauth_account = await oauth_account_repository.get_by_provider_and_user(
        oauth_provider.id, user.id
    )
    if oauth_account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    if oauth_account.is_expired():
        async with _account_refresh_lock(oauth_account.id):
            # Another coroutine may have already refreshed while we waited.
            cached = _refresh_results.get(oauth_account.id)
            if cached is not None:
                oauth_account.access_token = cached["access_token"]
                oauth_account.expires_at = cached["expires_at"]
            else:
                # First caller — perform the actual external refresh.
                oauth_provider_service = get_oauth_provider_service(oauth_provider)
                try:
                    access_token_dict = await oauth_provider_service.refresh_token(
                        cast(str, oauth_account.refresh_token)
                    )
                except RefreshTokenNotSupportedError as e:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=APIErrorCode.OAUTH_PROVIDER_REFRESH_TOKEN_NOT_SUPPORTED,
                    ) from e
                except RefreshTokenError as e:
                    logger.warning(
                        "Error while refreshing OAuth Provider access token",
                        message=e.message,
                        error_body=(
                            e.response.text if e.response is not None else None
                        ),
                    )
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=APIErrorCode.OAUTH_PROVIDER_REFRESH_TOKEN_ERROR,
                    ) from e

                oauth_account.access_token = access_token_dict["access_token"]
                try:
                    oauth_account.expires_at = datetime.fromtimestamp(
                        access_token_dict["expires_at"], tz=UTC
                    )
                except KeyError:
                    oauth_account.expires_at = None

                await oauth_account_repository.update(oauth_account)

                # Publish result for any concurrent waiters.
                _refresh_results[oauth_account.id] = {
                    "access_token": oauth_account.access_token,
                    "expires_at": oauth_account.expires_at,
                }

    audit_logger(
        AuditLogMessage.OAUTH_PROVIDER_USER_ACCESS_TOKEN_GET,
        subject_user_id=user.id,
        oauth_provider_id=str(oauth_provider.id),
    )

    return oauth_account
