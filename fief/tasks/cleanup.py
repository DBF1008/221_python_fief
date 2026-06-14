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
from fief.repositories.base import ExpiresAtRepositoryProtocol
from fief.tasks.base import TaskBase

CLEANUP_BATCH_SIZE = 1000

repository_classes: list[type[ExpiresAtRepositoryProtocol]] = [
    AuthorizationCodeRepository,
    EmailVerificationRepository,
    LoginSessionRepository,
    OAuthSessionRepository,
    RefreshTokenRepository,
    RegistrationSessionRepository,
    SessionTokenRepository,
]


class CleanupTask(TaskBase):
    __name__ = "cleanup"

    async def run(self):
        results: dict[str, int] = {}
        for repository_class in repository_classes:
            model_name = repository_class.model.__name__
            try:
                async with self.get_main_session() as session:
                    repository = repository_class(session)
                    deleted = await repository.delete_expired(
                        batch_size=CLEANUP_BATCH_SIZE
                    )
                    results[model_name] = deleted
                    logger.debug(
                        "Cleaned up expired records",
                        model=model_name,
                        deleted=deleted,
                    )
            except Exception:
                results[model_name] = -1
                logger.exception(
                    "Failed to clean up expired records",
                    model=model_name,
                )
        logger.info("Cleanup completed", task="cleanup", results=results)
        return results


cleanup = dramatiq.actor(CleanupTask())
