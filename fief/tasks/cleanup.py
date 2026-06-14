import dramatiq

from fief.logger import logger
from fief.repositories import (
    AuthorizationCodeRepository,
    EmailVerificationRepository,
    LoginSessionRepository,
    OAuthSessionRepository,
    RefreshTokenRepository,
    RegistrationSessionRepository,
    SessionTokenRepository,
)
from fief.repositories.base import (
    DEFAULT_EXPIRED_DELETE_BATCH_SIZE,
    ExpiresAtRepositoryProtocol,
)
from fief.tasks.base import TaskBase

repository_classes: list[type[ExpiresAtRepositoryProtocol]] = [
    AuthorizationCodeRepository,
    EmailVerificationRepository,
    LoginSessionRepository,
    OAuthSessionRepository,
    RefreshTokenRepository,
    RegistrationSessionRepository,
    SessionTokenRepository,
]

# Number of expired rows removed per transaction for each table. Kept here so
# the cleanup throughput can be tuned in a single place.
CLEANUP_BATCH_SIZE = DEFAULT_EXPIRED_DELETE_BATCH_SIZE

# Seconds awaited between two batches to give the database room to breathe.
# Defaults to no pause; raise it to throttle the cleanup on busy instances.
CLEANUP_BATCH_DELAY_SECONDS = 0.0


class CleanupTask(TaskBase):
    __name__ = "cleanup"

    async def run(
        self,
        *,
        batch_size: int = CLEANUP_BATCH_SIZE,
        delay: float = CLEANUP_BATCH_DELAY_SECONDS,
    ) -> dict[str, int]:
        """Delete expired objects from every expirable table.

        Each table is drained in bounded batches (see
        :meth:`ExpiresAtMixin.delete_expired`) so that a large backlog of
        expired rows never results in a single long-running ``DELETE``. Returns
        a mapping of model name to the number of rows deleted, which is also
        logged as a summary.
        """
        deleted_by_model: dict[str, int] = {}
        async with self.get_main_session() as session:
            for repository_class in repository_classes:
                repository = repository_class(session)
                model_name = repository.model.__name__
                deleted = await repository.delete_expired(
                    batch_size=batch_size, delay=delay
                )
                deleted_by_model[model_name] = deleted
                logger.info(
                    "Deleted expired objects",
                    task=self.__name__,
                    model=model_name,
                    deleted=deleted,
                )

        logger.info(
            "Cleanup task complete",
            task=self.__name__,
            total_deleted=sum(deleted_by_model.values()),
            deleted_by_model=deleted_by_model,
        )
        return deleted_by_model


cleanup = dramatiq.actor(CleanupTask())
