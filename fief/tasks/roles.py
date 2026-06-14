import uuid

import dramatiq

from fief.models import Role, UserPermission
from fief.repositories import (
    RoleRepository,
    UserPermissionRepository,
    UserRoleRepository,
)
from fief.tasks.base import ObjectDoesNotExistTaskError, TaskBase

BATCH_SIZE = 500


class OnRoleUpdated(TaskBase):
    __name__ = "on_role_updated"

    async def run(
        self, role_id: str, added_permissions: list[str], deleted_permissions: list[str]
    ):
        # Phase 1: Validate role and handle deletions.
        # Deletions use bulk DELETE statements (one per permission),
        # which are already efficient and idempotent.
        async with self.get_main_session() as session:
            role_repository = RoleRepository(session)
            role = await role_repository.get_by_id(uuid.UUID(role_id))

            if role is None:
                raise ObjectDoesNotExistTaskError(Role, role_id)

            # Revoke deleted permissions — each is a single DELETE statement
            user_permission_repository = UserPermissionRepository(session)
            for deleted_permission in deleted_permissions:
                await user_permission_repository.delete_by_permission_and_role(
                    uuid.UUID(deleted_permission), role.id
                )

        # Phase 2: Add permissions in chunks.
        # Each chunk gets its own session/transaction for isolation.
        # If the task crashes mid-way, Dramatiq retries the whole task;
        # idempotency in create_many_ignore_existing ensures already-inserted
        # records are safely skipped.
        if not added_permissions:
            return

        offset = 0
        while True:
            async with self.get_main_session() as session:
                user_role_repository = UserRoleRepository(session)
                user_permission_repository = UserPermissionRepository(session)

                user_roles, total = await user_role_repository.get_by_role_paginated(
                    uuid.UUID(role_id), limit=BATCH_SIZE, skip=offset
                )

                if not user_roles:
                    break

                # Build UserPermission objects for this chunk only
                user_permissions: list[UserPermission] = []
                for user_role in user_roles:
                    for added_permission in added_permissions:
                        user_permissions.append(
                            UserPermission(
                                user_id=user_role.user_id,
                                permission_id=uuid.UUID(added_permission),
                                from_role_id=uuid.UUID(role_id),
                            )
                        )

                # Idempotent insert — filters out already-existing records
                await user_permission_repository.create_many_ignore_existing(
                    user_permissions
                )

            offset += BATCH_SIZE
            if offset >= total:
                break


on_role_updated = dramatiq.actor(OnRoleUpdated())
