import uuid
from unittest.mock import patch

import pytest

from fief.db import AsyncSession
from fief.models import User, UserRole
from fief.repositories import UserPermissionRepository
from fief.tasks.base import TaskError
from fief.tasks.roles import OnRoleUpdated
from tests.data import TestData


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

    async def test_large_batch_add_permission(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        """Chunked processing correctly propagates permissions to many users."""
        on_user_role_updated = OnRoleUpdated(main_session_manager)

        role = test_data["roles"]["castles_visitor"]
        permission = test_data["permissions"]["castles:create"]
        tenant = test_data["tenants"]["default"]

        # Create extra users with this role
        extra_users: list[User] = []
        for i in range(5):
            user = User(
                email=f"batch_test_{i}@bretagne.duchy",
                email_verified=True,
                hashed_password="test",
                tenant=tenant,
            )
            main_session.add(user)
            user_role = UserRole(user=user, role=role)
            main_session.add(user_role)
            extra_users.append(user)
        await main_session.commit()

        # Patch BATCH_SIZE to force multiple chunks
        with patch("fief.tasks.roles.BATCH_SIZE", 2):
            await on_user_role_updated.run(str(role.id), [str(permission.id)], [])

        # Verify ALL users with the role got the permission
        user_permission_repository = UserPermissionRepository(main_session)
        for user in extra_users:
            user_permission = (
                await user_permission_repository.get_by_permission_and_user(
                    user.id, permission.id
                )
            )
            assert user_permission is not None
            assert user_permission.from_role_id == role.id

        # Also verify original 'regular' user still has it
        regular = test_data["users"]["regular"]
        user_permission = (
            await user_permission_repository.get_by_permission_and_user(
                regular.id, permission.id
            )
        )
        assert user_permission is not None
        assert user_permission.from_role_id == role.id

    async def test_idempotent_re_run(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        """Running the same task twice produces no duplicates and no errors."""
        on_user_role_updated = OnRoleUpdated(main_session_manager)

        role = test_data["roles"]["castles_visitor"]
        permission = test_data["permissions"]["castles:create"]

        # Run twice with identical parameters
        await on_user_role_updated.run(str(role.id), [str(permission.id)], [])
        await on_user_role_updated.run(str(role.id), [str(permission.id)], [])

        # Should have exactly one UserPermission for this user+permission+role combo
        user = test_data["users"]["regular"]
        user_permission_repository = UserPermissionRepository(main_session)
        user_permissions = await user_permission_repository.list(
            user_permission_repository.get_by_user_statement(user.id)
        )
        matching = [
            up
            for up in user_permissions
            if up.permission_id == permission.id and up.from_role_id == role.id
        ]
        assert len(matching) == 1

    async def test_mixed_add_and_delete(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        """Adding and deleting permissions in the same call both take effect."""
        on_user_role_updated = OnRoleUpdated(main_session_manager)

        role = test_data["roles"]["castles_visitor"]
        add_perm = test_data["permissions"]["castles:create"]
        delete_perm = test_data["permissions"]["castles:read"]

        await on_user_role_updated.run(
            str(role.id), [str(add_perm.id)], [str(delete_perm.id)]
        )

        user = test_data["users"]["regular"]
        user_permission_repository = UserPermissionRepository(main_session)
        user_permissions = await user_permission_repository.list(
            user_permission_repository.get_by_user_statement(user.id)
        )
        perm_ids = [up.permission_id for up in user_permissions]

        assert add_perm.id in perm_ids
        assert delete_perm.id not in perm_ids

    async def test_partial_overlap_existing_permissions(
        self,
        main_session_manager,
        test_data: TestData,
        main_session: AsyncSession,
    ):
        """Adding permissions that already exist for some users succeeds without duplicates."""
        on_user_role_updated = OnRoleUpdated(main_session_manager)

        role = test_data["roles"]["castles_visitor"]
        # castles:read already exists for 'regular' via castles_visitor role
        existing_perm = test_data["permissions"]["castles:read"]
        new_perm = test_data["permissions"]["castles:update"]

        await on_user_role_updated.run(
            str(role.id), [str(existing_perm.id), str(new_perm.id)], []
        )

        user = test_data["users"]["regular"]
        user_permission_repository = UserPermissionRepository(main_session)
        user_permissions = await user_permission_repository.list(
            user_permission_repository.get_by_user_statement(user.id)
        )
        perm_ids = [up.permission_id for up in user_permissions]

        assert existing_perm.id in perm_ids
        assert new_perm.id in perm_ids

        # Verify no duplicates of existing_perm
        existing_matches = [
            up
            for up in user_permissions
            if up.permission_id == existing_perm.id and up.from_role_id == role.id
        ]
        assert len(existing_matches) == 1
