from unittest.mock import AsyncMock, MagicMock

import pytest

from fief.db import AsyncSession
from fief.logger import AuditLogger, logger
from fief.repositories import (
    RoleRepository,
    UserPermissionRepository,
    UserRoleRepository,
)
from fief.services.user_roles import UserRolesService
from fief.services.webhooks.models import UserRoleCreated, UserRoleDeleted
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
async def test_set_roles_replaces_set_and_syncs_permissions(
    main_session: AsyncSession,
    test_data: TestData,
    user_roles_service: UserRolesService,
    trigger_webhooks_mock: MagicMock,
    send_task_mock: MagicMock,
) -> None:
    user = test_data["users"]["regular"]
    visitor = test_data["roles"]["castles_visitor"]
    manager = test_data["roles"]["castles_manager"]
    expected_manager_permission_count = len(manager.permissions)

    result = await user_roles_service.set_roles(user, [manager], run_in_worker=False)

    # The diff added the manager role and removed the (previously held) visitor role.
    assert {user_role.role_id for user_role in result.added} == {manager.id}
    assert {user_role.role_id for user_role in result.removed} == {visitor.id}
    assert {user_role.role_id for user_role in result.roles} == {manager.id}

    # A webhook fired for each side of the diff; nothing was deferred to a worker.
    triggered_events = [call.args[0] for call in trigger_webhooks_mock.call_args_list]
    assert UserRoleCreated in triggered_events
    assert UserRoleDeleted in triggered_events
    send_task_mock.assert_not_called()

    # The role set was persisted exactly to the target.
    user_role_repository = UserRoleRepository(main_session)
    user_roles = await user_role_repository.list(
        user_role_repository.get_by_user_statement(user.id)
    )
    assert {user_role.role_id for user_role in user_roles} == {manager.id}

    # Permissions were synced downstream: the manager role's permissions are now
    # granted, the visitor role's are gone, and the pre-existing direct
    # permission is untouched.
    user_permission_repository = UserPermissionRepository(main_session)
    user_permissions = await user_permission_repository.list(
        user_permission_repository.get_by_user_statement(user.id)
    )
    from_manager = [up for up in user_permissions if up.from_role_id == manager.id]
    from_visitor = [up for up in user_permissions if up.from_role_id == visitor.id]
    direct = [up for up in user_permissions if up.from_role_id is None]
    assert len(from_manager) == expected_manager_permission_count
    assert len(from_visitor) == 0
    assert len(direct) == 1


@pytest.mark.asyncio
async def test_set_roles_is_idempotent(
    main_session: AsyncSession,
    test_data: TestData,
    user_roles_service: UserRolesService,
    trigger_webhooks_mock: MagicMock,
    send_task_mock: MagicMock,
) -> None:
    user = test_data["users"]["regular"]
    visitor = test_data["roles"]["castles_visitor"]

    # The target set equals the current set, so this is a no-op.
    result = await user_roles_service.set_roles(user, [visitor], run_in_worker=False)

    assert result.added == []
    assert result.removed == []
    assert {user_role.role_id for user_role in result.roles} == {visitor.id}

    # No diff means no side effects at all.
    trigger_webhooks_mock.assert_not_called()
    send_task_mock.assert_not_called()

    user_role_repository = UserRoleRepository(main_session)
    user_roles = await user_role_repository.list(
        user_role_repository.get_by_user_statement(user.id)
    )
    assert {user_role.role_id for user_role in user_roles} == {visitor.id}


@pytest.mark.asyncio
async def test_set_roles_rolls_back_on_error(
    main_session: AsyncSession,
    test_data: TestData,
    user_roles_service: UserRolesService,
    trigger_webhooks_mock: MagicMock,
    send_task_mock: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = test_data["users"]["regular"]
    # Capture everything we need up front: the rollback inside set_roles expires
    # all session instances, so the test_data objects become unreadable after.
    user_id = user.id
    visitor_id = test_data["roles"]["castles_visitor"].id
    manager = test_data["roles"]["castles_manager"]

    user_permission_repository = UserPermissionRepository(main_session)
    permissions_before = await user_permission_repository.list(
        user_permission_repository.get_by_user_statement(user_id)
    )
    permission_ids_before = {up.id for up in permissions_before}

    # Make the single commit fail mid-sync.
    monkeypatch.setattr(
        main_session, "commit", AsyncMock(side_effect=Exception("commit failed"))
    )

    with pytest.raises(Exception, match="commit failed"):
        await user_roles_service.set_roles(user, [manager], run_in_worker=False)

    # The whole diff rolled back atomically: the role set is exactly as before
    # (the add was not persisted and the removal was reverted).
    user_role_repository = UserRoleRepository(main_session)
    user_roles = await user_role_repository.list(
        user_role_repository.get_by_user_statement(user_id)
    )
    assert {user_role.role_id for user_role in user_roles} == {visitor_id}

    # No permission was synced either.
    permissions_after = await user_permission_repository.list(
        user_permission_repository.get_by_user_statement(user_id)
    )
    assert {up.id for up in permissions_after} == permission_ids_before

    # No external side effects fired because the commit never succeeded.
    trigger_webhooks_mock.assert_not_called()
    send_task_mock.assert_not_called()
