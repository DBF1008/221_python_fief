import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from fief.db import AsyncSession
from fief.models import SessionToken
from fief.repositories import (
    AuthorizationCodeRepository,
    LoginSessionRepository,
    SessionTokenRepository,
)
from fief.tasks.cleanup import CleanupTask
from tests.data import TestData


@pytest.mark.asyncio
class TestTasksCleanup:
    async def test_cleanup(self, main_session_manager, send_task_mock: MagicMock):
        cleanup = CleanupTask(main_session_manager, send_task=send_task_mock)
        await cleanup.run()

    async def test_cleanup_deletes_expired_preserves_valid(
        self,
        main_session_manager,
        main_session: AsyncSession,
        test_data: TestData,
        send_task_mock: MagicMock,
    ):
        """验证过期记录被删除，非过期记录保留。"""
        # 确认过期记录存在
        login_repo = LoginSessionRepository(main_session)
        expired_login = test_data["login_sessions"]["expired"]
        assert await login_repo.get_by_id(expired_login.id) is not None

        auth_repo = AuthorizationCodeRepository(main_session)
        expired_auth = test_data["authorization_codes"]["expired"]
        assert await auth_repo.get_by_id(expired_auth.id) is not None

        # 执行清理
        cleanup = CleanupTask(main_session_manager, send_task=send_task_mock)
        results = await cleanup.run()

        # 刷新 identity map
        main_session.expire_all()

        # 过期记录应已删除
        assert await login_repo.get_by_id(expired_login.id) is None
        assert await auth_repo.get_by_id(expired_auth.id) is None

        # 非过期记录应保留
        valid_login = test_data["login_sessions"]["default"]
        assert await login_repo.get_by_id(valid_login.id) is not None

        # 结果应包含删除计数
        assert results["LoginSession"] >= 1
        assert results["AuthorizationCode"] >= 1

    async def test_cleanup_multi_round(
        self,
        main_session_manager,
        main_session: AsyncSession,
        test_data: TestData,
        send_task_mock: MagicMock,
    ):
        """验证超过 batch_size 的过期记录通过多轮循环全部删除。"""
        user = test_data["users"]["regular"]
        num_expired = 12
        batch_size = 5

        # 创建超过一个 batch 的过期 SessionToken
        for i in range(num_expired):
            main_session.add(
                SessionToken(
                    token=f"expired-multi-{i}-{uuid.uuid4()}",
                    user=user,
                    expires_at=datetime.now(UTC) - timedelta(seconds=3600),
                )
            )
        await main_session.commit()

        # 用小 batch_size 清理，验证多轮循环能删完
        cleanup = CleanupTask(main_session_manager, send_task=send_task_mock)
        with patch("fief.tasks.cleanup.CLEANUP_BATCH_SIZE", batch_size):
            results = await cleanup.run()

        main_session.expire_all()

        # 所有过期 SessionToken 应已删除
        repo = SessionTokenRepository(main_session)
        remaining = await repo.all()
        expired_remaining = [
            t for t in remaining if t.expires_at < datetime.now(UTC)
        ]
        assert len(expired_remaining) == 0
        # 至少删除了 num_expired 条
        assert results["SessionToken"] >= num_expired

    async def test_cleanup_empty_no_expired(
        self,
        main_session_manager,
        send_task_mock: MagicMock,
    ):
        """验证没有过期记录时清理正常返回 0。"""
        cleanup = CleanupTask(main_session_manager, send_task=send_task_mock)

        # 第一次运行清除 test_data 中的过期记录
        await cleanup.run()

        # 第二次运行应全部为 0
        results = await cleanup.run()
        for model_name, count in results.items():
            assert count == 0, f"{model_name} 第二次运行应为 0，实际为 {count}"

    async def test_cleanup_large_accumulation(
        self,
        main_session_manager,
        main_session: AsyncSession,
        test_data: TestData,
        send_task_mock: MagicMock,
    ):
        """验证大量过期记录堆积能被完整清理。"""
        user = test_data["users"]["regular"]
        num_expired = 50

        for i in range(num_expired):
            main_session.add(
                SessionToken(
                    token=f"expired-large-{i}-{uuid.uuid4()}",
                    user=user,
                    expires_at=datetime.now(UTC) - timedelta(seconds=3600),
                )
            )
        await main_session.commit()

        cleanup = CleanupTask(main_session_manager, send_task=send_task_mock)
        with patch("fief.tasks.cleanup.CLEANUP_BATCH_SIZE", 10):
            results = await cleanup.run()

        main_session.expire_all()

        repo = SessionTokenRepository(main_session)
        remaining = await repo.all()
        expired_remaining = [
            t for t in remaining if t.expires_at < datetime.now(UTC)
        ]
        assert len(expired_remaining) == 0
        assert results["SessionToken"] >= num_expired

    async def test_cleanup_isolates_repository_failure(
        self,
        main_session_manager,
        main_session: AsyncSession,
        test_data: TestData,
        send_task_mock: MagicMock,
    ):
        """验证某个 repository 失败不影响其他 repository 正常清理。"""

        async def failing_delete(self, batch_size=1000):
            raise RuntimeError("Simulated failure")

        cleanup = CleanupTask(main_session_manager, send_task=send_task_mock)
        with patch.object(
            LoginSessionRepository, "delete_expired", failing_delete
        ):
            results = await cleanup.run()

        # LoginSession 应报告失败（-1）
        assert results["LoginSession"] == -1

        # 其他 repository 应正常完成（>= 0）
        assert results["AuthorizationCode"] >= 0
        assert results["SessionToken"] >= 0
        assert results["EmailVerification"] >= 0
        assert results["OAuthSession"] >= 0
        assert results["RefreshToken"] >= 0
        assert results["RegistrationSession"] >= 0

        # test_data 中的过期 AuthorizationCode 应已被清理
        main_session.expire_all()
        auth_repo = AuthorizationCodeRepository(main_session)
        expired_auth = test_data["authorization_codes"]["expired"]
        assert await auth_repo.get_by_id(expired_auth.id) is None
