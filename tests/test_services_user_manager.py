from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from fief.crypto.password import password_helper
from fief.db import AsyncSession
from fief.logger import AuditLogger, logger
from fief.models import EmailVerification
from fief.repositories import (
    EmailVerificationRepository,
    UserRepository,
)
from fief.services.user_manager import UserManager
from fief.settings import settings
from tests.data import TestData


@pytest_asyncio.fixture
async def user_manager(
    main_session: AsyncSession,
    send_task_mock: MagicMock,
) -> UserManager:
    user_repository = UserRepository(main_session)
    email_verification_repository = EmailVerificationRepository(main_session)
    return UserManager(
        password_helper=password_helper,
        user_repository=user_repository,
        email_verification_repository=email_verification_repository,
        user_fields=[],
        send_task=send_task_mock,
        audit_logger=AuditLogger(logger),
        trigger_webhooks=MagicMock(),
        user_roles=MagicMock(),
    )


@pytest.mark.asyncio
class TestRequestVerifyEmailCooldown:
    """Regression tests for the email verification resend cooldown."""

    async def test_same_email_within_cooldown_reuses(
        self,
        user_manager: UserManager,
        test_data: TestData,
        send_task_mock: MagicMock,
        main_session: AsyncSession,
    ):
        """Within cooldown window, the existing verification is reused
        and no new email is sent."""
        user = test_data["users"]["not_verified_email"]
        ev = test_data["email_verifications"]["not_verified_email"]
        original_code = ev.code
        original_created_at = ev.created_at

        await user_manager.request_verify_email(user, user.email)

        # No new email should have been sent
        send_task_mock.assert_not_called()

        # Original verification should still exist unchanged
        email_verification_repository = EmailVerificationRepository(main_session)
        verifications = await email_verification_repository.get_by_user(user.id)
        assert len(verifications) == 1
        assert verifications[0].code == original_code
        assert verifications[0].created_at == original_created_at

    async def test_same_email_after_cooldown_creates_new(
        self,
        user_manager: UserManager,
        test_data: TestData,
        send_task_mock: MagicMock,
        main_session: AsyncSession,
    ):
        """After the cooldown window expires, a new verification is created
        and a new email is sent."""
        user = test_data["users"]["not_verified_email"]
        ev = test_data["email_verifications"]["not_verified_email"]
        original_code = ev.code

        # Age the existing verification past the cooldown
        cooldown = settings.email_verification_resend_cooldown_seconds
        ev.created_at = datetime.now(UTC) - timedelta(seconds=cooldown + 1)
        email_verification_repository = EmailVerificationRepository(main_session)
        await email_verification_repository.update(ev)

        await user_manager.request_verify_email(user, user.email)

        # A new email should have been sent
        send_task_mock.assert_called_once()

        # Exactly one verification should exist with a new code
        verifications = await email_verification_repository.get_by_user(user.id)
        assert len(verifications) == 1
        assert verifications[0].code != original_code

    async def test_different_email_always_creates_new(
        self,
        user_manager: UserManager,
        test_data: TestData,
        send_task_mock: MagicMock,
        main_session: AsyncSession,
    ):
        """Email change always invalidates old verifications and creates a
        new one, regardless of cooldown."""
        user = test_data["users"]["regular"]
        new_email = "brand-new@example.com"

        await user_manager.request_verify_email(user, new_email)

        # A new email should have been sent
        send_task_mock.assert_called_once()

        # Exactly one verification for the new email
        email_verification_repository = EmailVerificationRepository(main_session)
        verifications = await email_verification_repository.get_by_user(user.id)
        assert len(verifications) == 1
        assert verifications[0].email == new_email

    async def test_expired_verification_triggers_new(
        self,
        user_manager: UserManager,
        test_data: TestData,
        send_task_mock: MagicMock,
        main_session: AsyncSession,
    ):
        """An expired verification is not reusable even if created within
        the cooldown window — a new verification is created."""
        user = test_data["users"]["not_verified_email"]

        # Remove all existing verifications and create one that is already
        # expired so we can test that expired records are never reused.
        email_verification_repository = EmailVerificationRepository(main_session)
        await email_verification_repository.delete_by_user(user.id)

        # Clear the identity map so the bulk-deleted instances don't
        # conflict with subsequent ORM operations
        main_session.expire_all()

        expired_ev = EmailVerification(
            code="expired_hash",
            email=user.email,
            user=user,
        )
        expired_ev.expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await email_verification_repository.create(expired_ev)

        # Expunge so the next bulk-delete doesn't leave a stale reference
        main_session.expunge(expired_ev)

        await user_manager.request_verify_email(user, user.email)

        # A new email should have been sent because the existing one was expired
        send_task_mock.assert_called_once()

        # Exactly one verification with a fresh expiry
        verifications = await email_verification_repository.get_by_user(user.id)
        assert len(verifications) == 1
        assert not verifications[0].is_expired

    async def test_no_existing_verification_creates_new(
        self,
        user_manager: UserManager,
        test_data: TestData,
        send_task_mock: MagicMock,
        main_session: AsyncSession,
    ):
        """First-time request with no existing verifications creates a new
        one and sends the email."""
        user = test_data["users"]["not_verified_email"]

        # Clear any seed verification records
        email_verification_repository = EmailVerificationRepository(main_session)
        await email_verification_repository.delete_by_user(user.id)

        await user_manager.request_verify_email(user, user.email)

        send_task_mock.assert_called_once()

        verifications = await email_verification_repository.get_by_user(user.id)
        assert len(verifications) == 1

    async def test_rapid_successive_calls_only_one_email(
        self,
        user_manager: UserManager,
        test_data: TestData,
        send_task_mock: MagicMock,
        main_session: AsyncSession,
    ):
        """Simulate rapid double/triple-click: 5 successive calls should
        only result in 1 email sent and 1 verification record."""
        user = test_data["users"]["not_verified_email"]

        # Clear any seed verification records
        email_verification_repository = EmailVerificationRepository(main_session)
        await email_verification_repository.delete_by_user(user.id)

        for _ in range(5):
            await user_manager.request_verify_email(user, user.email)

        # Only the first call should have sent an email
        send_task_mock.assert_called_once()

        # Only one verification record should exist
        verifications = await email_verification_repository.get_by_user(user.id)
        assert len(verifications) == 1
