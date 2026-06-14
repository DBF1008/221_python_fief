import uuid

import pytest
from sqlalchemy import func, select

from fief.db import AsyncSession
from fief.models import Role, User, UserPermission, UserRole
from fief.repositories import UserPermissionRepository
from fief.tasks.base import TaskError
from fief.tasks.roles import OnRoleUpdated
from tests.data import TestData


async def _seed_role_with_users(
    session: AsyncSession, tenant_id: uuid.UUID, *, user_count: int
) -> tuple[Role, list[uuid.UUID]]:
    """Create a fresh role and ``user_count`` users holding it.

    Ids are pre-assigned so they remain usable after the task detaches its objects
    with ``expunge_all()``. Everything is committed on the (rolled-back) test
    session, so it stays isolated from the shared ``test_data``.
    """
    role = Role(id=uuid.uuid4(), name="Batch Role", granted_by_default=False)
    session.add(role)

    user_ids: list[uuid.UUID] = []
    for _ in range(user_count):
        user_id = uuid.uuid4()
        user_ids.append(user_id)
        session.add(
            User(
                id=user_id,
                email=f"{user_id}@batch.test",
                hashed_password="hashed",
                tenant_id=tenant_id,
            )
        )
        session.add(UserRole(user_id=user_id, role_id=role.id))

    await session.commit()
    return role, user_ids


async def _count_user_permissions(
    session: AsyncSession, role_id: uuid.UUID, permission_id: uuid.UUID
) -> int:
    result = await session.execute(
        select(func.count())
        .select_from(UserPermission)
        .where(
            UserPermission.from_role_id == role_id,
            UserPermission.permission_id == permission_id,
        )
    )
    return result.scalar_one()


@pytest.mark.asyncio
class TestTasksOnRoleUpdated:
    async def test_not_existing_role(
        self,
        main_session_manager,
        not_existing_uuid: uuid.UUID,
    ):
        on_user_role_updated = OnRoleUpdated(main_session_manager)

        with pytest.raises(TaskError):
            await on_user_role_updated.run(str(not_existing_uuid), [], [])

    async def test_role_created_added_permission(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        on_user_role_updated = OnRoleUpdated(main_session_manager)

        role = test_data["roles"]["castles_visitor"]
        permission = test_data["permissions"]["castles:create"]
        await on_user_role_updated.run(str(role.id), [str(permission.id)], [])

        user = test_data["users"]["regular"]
        user_permission_repository = UserPermissionRepository(main_session)
        user_permissions = await user_permission_repository.list(
            user_permission_repository.get_by_user_statement(user.id)
        )
        assert len(user_permissions) == 3
        assert permission.id in [
            user_permission.permission_id for user_permission in user_permissions
        ]

    async def test_role_created_deleted_permission(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        on_user_role_updated = OnRoleUpdated(main_session_manager)

        role = test_data["roles"]["castles_visitor"]
        permission = test_data["permissions"]["castles:read"]
        await on_user_role_updated.run(str(role.id), [], [str(permission.id)])

        user = test_data["users"]["regular"]
        user_permission_repository = UserPermissionRepository(main_session)
        user_permissions = await user_permission_repository.list(
            user_permission_repository.get_by_user_statement(user.id)
        )
        assert len(user_permissions) == 1
        assert permission.id not in [
            user_permission.permission_id for user_permission in user_permissions
        ]

    async def test_added_permission_propagated_in_batches_to_many_users(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        """A large number of users is processed across several batches."""
        tenant = test_data["tenants"]["default"]
        permission = test_data["permissions"]["castles:create"]
        role, user_ids = await _seed_role_with_users(
            main_session, tenant.id, user_count=50
        )

        # A small batch size forces the task to iterate over many pages.
        on_user_role_updated = OnRoleUpdated(main_session_manager, batch_size=7)
        await on_user_role_updated.run(str(role.id), [str(permission.id)], [])

        # Every user holding the role got the permission exactly once. The unique
        # constraint guarantees at most one row per user, so a count equal to the
        # number of users means each user received it.
        assert await _count_user_permissions(main_session, role.id, permission.id) == 50

        user_permission_repository = UserPermissionRepository(main_session)
        for user_id in (user_ids[0], user_ids[len(user_ids) // 2], user_ids[-1]):
            user_permissions = await user_permission_repository.list(
                user_permission_repository.get_by_user_statement(user_id)
            )
            assert [up.permission_id for up in user_permissions] == [permission.id]

    async def test_repeated_execution_is_idempotent(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        """Re-running the same task must not duplicate rows nor raise.

        A naive re-insert would violate the
        ``(user_id, permission_id, from_role_id)`` unique constraint and abort the
        whole transaction.
        """
        tenant = test_data["tenants"]["default"]
        permission = test_data["permissions"]["castles:create"]
        role, _ = await _seed_role_with_users(main_session, tenant.id, user_count=20)

        on_user_role_updated = OnRoleUpdated(main_session_manager, batch_size=6)

        await on_user_role_updated.run(str(role.id), [str(permission.id)], [])
        assert await _count_user_permissions(main_session, role.id, permission.id) == 20

        await on_user_role_updated.run(str(role.id), [str(permission.id)], [])
        assert await _count_user_permissions(main_session, role.id, permission.id) == 20

    async def test_mixed_added_and_deleted_permissions(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        """Adding and removing permissions in one run yields the correct state."""
        tenant = test_data["tenants"]["default"]
        read_permission = test_data["permissions"]["castles:read"]
        create_permission = test_data["permissions"]["castles:create"]
        update_permission = test_data["permissions"]["castles:update"]
        role, user_ids = await _seed_role_with_users(
            main_session, tenant.id, user_count=15
        )

        # Pre-grant the "read" permission to every user, as a previous version of
        # the role would have done.
        for user_id in user_ids:
            main_session.add(
                UserPermission(
                    user_id=user_id,
                    permission_id=read_permission.id,
                    from_role_id=role.id,
                )
            )
        await main_session.commit()

        on_user_role_updated = OnRoleUpdated(main_session_manager, batch_size=4)
        await on_user_role_updated.run(
            str(role.id),
            [str(create_permission.id), str(update_permission.id)],
            [str(read_permission.id)],
        )

        assert (
            await _count_user_permissions(main_session, role.id, read_permission.id) == 0
        )
        assert (
            await _count_user_permissions(main_session, role.id, create_permission.id)
            == 15
        )
        assert (
            await _count_user_permissions(main_session, role.id, update_permission.id)
            == 15
        )

        user_permission_repository = UserPermissionRepository(main_session)
        sample_permissions = await user_permission_repository.list(
            user_permission_repository.get_by_user_statement(user_ids[0])
        )
        assert {up.permission_id for up in sample_permissions} == {
            create_permission.id,
            update_permission.id,
        }

    async def test_resumes_without_duplicating_already_propagated_permissions(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        """A run that resumes after a partial one fills the gap without duplicates."""
        tenant = test_data["tenants"]["default"]
        permission = test_data["permissions"]["castles:create"]
        role, user_ids = await _seed_role_with_users(
            main_session, tenant.id, user_count=30
        )

        # Simulate a previous run that crashed after committing the first 12 users.
        for user_id in user_ids[:12]:
            main_session.add(
                UserPermission(
                    user_id=user_id,
                    permission_id=permission.id,
                    from_role_id=role.id,
                )
            )
        await main_session.commit()
        assert await _count_user_permissions(main_session, role.id, permission.id) == 12

        on_user_role_updated = OnRoleUpdated(main_session_manager, batch_size=5)
        await on_user_role_updated.run(str(role.id), [str(permission.id)], [])

        # The remaining users are filled in, the already-granted ones untouched.
        assert await _count_user_permissions(main_session, role.id, permission.id) == 30
