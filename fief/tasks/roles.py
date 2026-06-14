import uuid

import dramatiq

from fief.models import Role, UserPermission
from fief.repositories import (
    RoleRepository,
    UserPermissionRepository,
    UserRoleRepository,
)
from fief.tasks.base import ObjectDoesNotExistTaskError, TaskBase

DEFAULT_BATCH_SIZE = 100


class OnRoleUpdated(TaskBase):
    __name__ = "on_role_updated"

    def __init__(self, *args, batch_size: int = DEFAULT_BATCH_SIZE, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.batch_size = batch_size

    async def run(
        self, role_id: str, added_permissions: list[str], deleted_permissions: list[str]
    ):
        role_uuid = uuid.UUID(role_id)
        added_permission_ids = [uuid.UUID(permission) for permission in added_permissions]
        deleted_permission_ids = [
            uuid.UUID(permission) for permission in deleted_permissions
        ]

        async with self.get_main_session() as session:
            role_repository = RoleRepository(session)
            role = await role_repository.get_by_id(role_uuid)
            if role is None:
                raise ObjectDoesNotExistTaskError(Role, role_id)

            if not added_permission_ids and not deleted_permission_ids:
                return

            user_role_repository = UserRoleRepository(session)
            user_permission_repository = UserPermissionRepository(session)

            # Propagate the change to the users holding this role in bounded batches.
            # Each batch is committed independently and every operation is idempotent
            # (added permissions skip the ones already granted, deletions are
            # set-based), so the task keeps a flat memory footprint, short
            # transactions, and can be safely retried/resumed: re-running always
            # converges to the correct final ``UserPermission`` state.
            after: uuid.UUID | None = None
            while True:
                user_ids = await user_role_repository.get_user_ids_by_role_paginated(
                    role_uuid, after=after, limit=self.batch_size
                )
                if not user_ids:
                    break
                after = user_ids[-1]

                if added_permission_ids:
                    existing_pairs = (
                        await user_permission_repository.get_existing_permission_pairs(
                            user_ids, added_permission_ids, role_uuid
                        )
                    )
                    user_permissions = [
                        UserPermission(
                            user_id=user_id,
                            permission_id=permission_id,
                            from_role_id=role_uuid,
                        )
                        for user_id in user_ids
                        for permission_id in added_permission_ids
                        if (user_id, permission_id) not in existing_pairs
                    ]
                    if user_permissions:
                        await user_permission_repository.create_many(user_permissions)

                if deleted_permission_ids:
                    await user_permission_repository.delete_by_permissions_and_role_for_users(
                        deleted_permission_ids, user_ids, role_uuid
                    )

                # Detach this batch's objects so memory stays flat across batches.
                session.expunge_all()


on_role_updated = dramatiq.actor(OnRoleUpdated())
