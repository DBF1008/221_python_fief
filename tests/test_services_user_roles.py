from unittest.mock import MagicMock

import pytest

from fief.db import AsyncSession
from fief.logger import AuditLogger, logger
from fief.repositories import (
    RoleRepository,
    UserPermissionRepository,
    UserRoleRepository,
)
from fief.services.user_roles import (
    UserRoleSyncNotExistingRole,
    UserRolesService,
)
from fief.tasks import on_user_role_created, on_user_role_deleted
from tests.data import TestData


@pytest.fixture
def trigger_webhooks_mock() -> MagicMock:
    return MagicMock()


@pytest.fixture
def user_roles_service(
    main_session: AsyncSession,
    trigger_webhooks_mock: MagicMock,
    send_task_mock: MagicMock,
) -> UserRolesService:
    return UserRolesService(
        UserRoleRepository(main_session),
        UserPermissionRepository(main_session),
        RoleRepository(main_session),
        AuditLogger(logger),
        trigger_webhooks_mock,
        send_task_mock,
    )


@pytest.mark.asyncio
async def test_add_default_roles(
    main_session: AsyncSession,
    test_data: TestData,
    user_roles_service: UserRolesService,
) -> None:
    user = test_data["users"]["regular_secondary"]

    await user_roles_service.add_default_roles(user, run_in_worker=False)

    user_role_repository = UserRoleRepository(main_session)
    user_roles = await user_role_repository.list(
        user_role_repository.get_by_user_statement(user.id)
    )
    assert len(user_roles) == 1

    user_permission_repository = UserPermissionRepository(main_session)
    user_permissions = await user_permission_repository.list(
        user_permission_repository.get_by_user_statement(user.id)
    )
    assert len(user_permissions) == 1


@pytest.mark.asyncio
class TestSetRoles:
    async def test_idempotent(
        self,
        main_session: AsyncSession,
        test_data: TestData,
        user_roles_service: UserRolesService,
        send_task_mock: MagicMock,
        trigger_webhooks_mock: MagicMock,
    ) -> None:
        """Syncing with the exact current role set produces no changes."""
        user = test_data["users"]["regular"]
        visitor_role = test_data["roles"]["castles_visitor"]

        added, removed = await user_roles_service.set_roles(
            user, [visitor_role.id], run_in_worker=True
        )

        assert added == []
        assert removed == []
        send_task_mock.assert_not_called()
        trigger_webhooks_mock.assert_not_called()

        # DB unchanged
        user_role_repository = UserRoleRepository(main_session)
        user_roles = await user_role_repository.list(
            user_role_repository.get_by_user_statement(user.id)
        )
        assert len(user_roles) == 1
        assert user_roles[0].role_id == visitor_role.id

    async def test_add_only(
        self,
        main_session: AsyncSession,
        test_data: TestData,
        user_roles_service: UserRolesService,
        send_task_mock: MagicMock,
        trigger_webhooks_mock: MagicMock,
    ) -> None:
        """Adding a new role while keeping the existing one."""
        user = test_data["users"]["regular"]
        visitor_role = test_data["roles"]["castles_visitor"]
        manager_role = test_data["roles"]["castles_manager"]

        added, removed = await user_roles_service.set_roles(
            user, [visitor_role.id, manager_role.id], run_in_worker=True
        )

        assert len(added) == 1
        assert added[0].role_id == manager_role.id
        assert removed == []

        # Verify DB state
        user_role_repository = UserRoleRepository(main_session)
        user_roles = await user_role_repository.list(
            user_role_repository.get_by_user_statement(user.id)
        )
        assert len(user_roles) == 2
        role_ids = {ur.role_id for ur in user_roles}
        assert visitor_role.id in role_ids
        assert manager_role.id in role_ids

        # Verify side effects: exactly one task dispatched for the added role
        send_task_mock.assert_called_once_with(
            on_user_role_created, str(user.id), str(manager_role.id)
        )
        trigger_webhooks_mock.assert_called_once()

    async def test_remove_only(
        self,
        main_session: AsyncSession,
        test_data: TestData,
        user_roles_service: UserRolesService,
        send_task_mock: MagicMock,
        trigger_webhooks_mock: MagicMock,
    ) -> None:
        """Removing all roles by passing an empty list."""
        user = test_data["users"]["regular"]
        visitor_role = test_data["roles"]["castles_visitor"]

        added, removed = await user_roles_service.set_roles(
            user, [], run_in_worker=True
        )

        assert added == []
        assert len(removed) == 1
        assert removed[0].role_id == visitor_role.id

        # Verify DB state
        user_role_repository = UserRoleRepository(main_session)
        user_roles = await user_role_repository.list(
            user_role_repository.get_by_user_statement(user.id)
        )
        assert len(user_roles) == 0

        # Verify side effects: exactly one task dispatched for the removed role
        send_task_mock.assert_called_once_with(
            on_user_role_deleted, str(user.id), str(visitor_role.id)
        )
        trigger_webhooks_mock.assert_called_once()

    async def test_partial_change(
        self,
        main_session: AsyncSession,
        test_data: TestData,
        user_roles_service: UserRolesService,
        send_task_mock: MagicMock,
        trigger_webhooks_mock: MagicMock,
    ) -> None:
        """Swap castles_visitor for castles_manager."""
        user = test_data["users"]["regular"]
        manager_role = test_data["roles"]["castles_manager"]

        added, removed = await user_roles_service.set_roles(
            user, [manager_role.id], run_in_worker=True
        )

        assert len(added) == 1
        assert added[0].role_id == manager_role.id
        assert len(removed) == 1
        assert removed[0].role_id == test_data["roles"]["castles_visitor"].id

        # Verify DB state
        user_role_repository = UserRoleRepository(main_session)
        user_roles = await user_role_repository.list(
            user_role_repository.get_by_user_statement(user.id)
        )
        assert len(user_roles) == 1
        assert user_roles[0].role_id == manager_role.id

        # Verify side effects: exactly 2 task dispatches (1 add + 1 remove)
        assert send_task_mock.call_count == 2
        assert trigger_webhooks_mock.call_count == 2

    async def test_unknown_role(
        self,
        main_session: AsyncSession,
        test_data: TestData,
        user_roles_service: UserRolesService,
        send_task_mock: MagicMock,
        trigger_webhooks_mock: MagicMock,
    ) -> None:
        """Target includes a non-existing role ID — should reject entirely."""
        import uuid

        user = test_data["users"]["regular"]
        visitor_role = test_data["roles"]["castles_visitor"]
        fake_id = uuid.uuid4()

        with pytest.raises(UserRoleSyncNotExistingRole):
            await user_roles_service.set_roles(
                user, [visitor_role.id, fake_id], run_in_worker=True
            )

        # No side effects fired
        send_task_mock.assert_not_called()
        trigger_webhooks_mock.assert_not_called()

        # DB unchanged — user still has only castles_visitor
        user_role_repository = UserRoleRepository(main_session)
        user_roles = await user_role_repository.list(
            user_role_repository.get_by_user_statement(user.id)
        )
        assert len(user_roles) == 1
        assert user_roles[0].role_id == visitor_role.id

    async def test_no_duplicate_dispatch(
        self,
        test_data: TestData,
        user_roles_service: UserRolesService,
        send_task_mock: MagicMock,
    ) -> None:
        """Verify send_task is called exactly once per role change, not duplicated."""
        user = test_data["users"]["regular"]
        manager_role = test_data["roles"]["castles_manager"]
        visitor_role = test_data["roles"]["castles_visitor"]

        await user_roles_service.set_roles(
            user, [manager_role.id], run_in_worker=True
        )

        # Partial change: remove visitor, add manager
        # send_task should be called exactly twice — once per change
        assert send_task_mock.call_count == 2

        # Verify no duplicate calls by checking all calls
        calls = send_task_mock.call_args_list
        task_names = [call.args[0].actor_name for call in calls]
        assert task_names.count("on_user_role_created") == 1
        assert task_names.count("on_user_role_deleted") == 1
