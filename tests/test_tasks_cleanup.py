import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import func, select

from fief.db import AsyncSession
from fief.models import SessionToken
from fief.repositories import SessionTokenRepository
from fief.tasks.cleanup import CleanupTask, repository_classes
from tests.data import TestData


async def _count_session_tokens(session: AsyncSession, *, expired: bool) -> int:
    result = await session.execute(
        select(func.count(SessionToken.id)).where(SessionToken.is_expired.is_(expired))
    )
    return result.scalar_one()


async def _seed_session_tokens(
    session: AsyncSession,
    user_id: uuid.UUID,
    *,
    expired: int = 0,
    active: int = 0,
) -> None:
    objects: list[SessionToken] = []
    for _ in range(expired):
        objects.append(
            SessionToken(
                token=f"expired-{uuid.uuid4()}",
                user_id=user_id,
                expires_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
    for _ in range(active):
        objects.append(
            SessionToken(
                token=f"active-{uuid.uuid4()}",
                user_id=user_id,
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )
    session.add_all(objects)
    await session.commit()


@pytest.mark.asyncio
class TestTasksCleanup:
    async def test_cleanup_reports_counts_and_removes_expired(
        self,
        test_data: TestData,
        main_session: AsyncSession,
        main_session_manager,
        send_task_mock: MagicMock,
    ):
        """The task returns a per-model count and drains expired rows."""
        user_id = test_data["users"]["regular"].id
        await _seed_session_tokens(main_session, user_id, expired=4, active=3)

        cleanup = CleanupTask(main_session_manager, send_task=send_task_mock)
        result = await cleanup.run()

        # Result feedback covers every expirable repository.
        assert set(result) == {rc.model.__name__ for rc in repository_classes}
        assert all(isinstance(count, int) and count >= 0 for count in result.values())

        # The seeded expired tokens are reported and removed...
        assert result["SessionToken"] >= 4
        assert await _count_session_tokens(main_session, expired=True) == 0
        # ...while non-expired tokens are preserved.
        assert await _count_session_tokens(main_session, expired=False) >= 3

    async def test_delete_expired_runs_in_multiple_batches(
        self,
        test_data: TestData,
        main_session: AsyncSession,
        mocker: MockerFixture,
    ):
        """A backlog larger than the batch size is removed over several rounds."""
        user_id = test_data["users"]["regular"].id
        # Precondition: nothing is expired yet in this isolated transaction.
        assert await _count_session_tokens(main_session, expired=True) == 0
        baseline_active = await _count_session_tokens(main_session, expired=False)
        await _seed_session_tokens(main_session, user_id, expired=25, active=5)

        repository = SessionTokenRepository(main_session)
        batch_spy = mocker.spy(repository, "delete_expired_batch")

        deleted = await repository.delete_expired(batch_size=10, delay=0)

        assert deleted == 25
        # 25 expired rows in batches of 10 -> 10, 10, 5 == three rounds.
        assert batch_spy.call_count == 3
        assert await _count_session_tokens(main_session, expired=True) == 0
        # The active tokens (baseline + the 5 we seeded) are untouched.
        assert (
            await _count_session_tokens(main_session, expired=False)
            == baseline_active + 5
        )

    async def test_delete_expired_empty_table_issues_no_delete(
        self,
        test_data: TestData,
        main_session: AsyncSession,
        mocker: MockerFixture,
    ):
        """With nothing expired, a single probe runs and no DELETE is issued."""
        assert await _count_session_tokens(main_session, expired=True) == 0

        repository = SessionTokenRepository(main_session)
        batch_spy = mocker.spy(repository, "delete_expired_batch")
        execute_spy = mocker.spy(repository, "_execute_statement")

        deleted = await repository.delete_expired(batch_size=50)

        assert deleted == 0
        # One probing batch finds nothing and the loop stops immediately.
        assert batch_spy.call_count == 1
        # No DELETE statement is ever executed for an empty table.
        execute_spy.assert_not_called()

    async def test_cleanup_handles_partial_pileup(
        self,
        test_data: TestData,
        main_session: AsyncSession,
        main_session_manager,
        send_task_mock: MagicMock,
    ):
        """One table with a large backlog is cleaned in controlled batches."""
        user_id = test_data["users"]["regular"].id
        assert await _count_session_tokens(main_session, expired=True) == 0
        # SessionToken accumulates a large backlog; other tables stay light.
        await _seed_session_tokens(main_session, user_id, expired=23, active=2)

        cleanup = CleanupTask(main_session_manager, send_task=send_task_mock)
        result = await cleanup.run(batch_size=10, delay=0)

        assert result["SessionToken"] == 23
        # The heavily-loaded table dominates every lighter one.
        assert result["SessionToken"] == max(result.values())
        assert await _count_session_tokens(main_session, expired=True) == 0

    async def test_delete_expired_rejects_invalid_batch_size(
        self, main_session: AsyncSession
    ):
        """A non-positive batch size is rejected before touching the database."""
        repository = SessionTokenRepository(main_session)
        with pytest.raises(ValueError):
            await repository.delete_expired(batch_size=0)
        with pytest.raises(ValueError):
            await repository.delete_expired(delay=-1)
