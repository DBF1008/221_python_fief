from pydantic import UUID4

from fief import schemas, tasks
from fief.logger import AuditLogger
from fief.models import AuditLogMessage, Role, User, UserRole
from fief.repositories import (
    RoleRepository,
    UserPermissionRepository,
    UserRoleRepository,
)
from fief.services.user_role_permissions import UserRolePermissionsService
from fief.services.webhooks.models import UserRoleCreated, UserRoleDeleted
from fief.services.webhooks.trigger import TriggerWebhooks
from fief.tasks import SendTask


class UserRolesError(Exception): ...


class UserRoleAlreadyExists(UserRolesError): ...


class UserRoleDoesNotExist(UserRolesError): ...


class UserRoleSyncNotExistingRole(UserRolesError): ...


class UserRolesService:
    def __init__(
        self,
        user_role_repository: UserRoleRepository,
        user_permission_repository: UserPermissionRepository,
        role_repository: RoleRepository,
        audit_logger: AuditLogger,
        trigger_webhooks: TriggerWebhooks,
        send_task: SendTask,
    ) -> None:
        self.user_role_repository = user_role_repository
        self.user_permission_repository = user_permission_repository
        self.role_repository = role_repository
        self.audit_logger = audit_logger
        self.trigger_webhooks = trigger_webhooks
        self.send_task = send_task
        self.user_role_permissions = UserRolePermissionsService(
            user_permission_repository
        )

    async def add_role(
        self, user: User, role: Role, *, run_in_worker: bool = True
    ) -> UserRole:
        existing_user_role = await self.user_role_repository.get_by_role_and_user(
            user.id, role.id
        )
        if existing_user_role is not None:
            raise UserRoleAlreadyExists()

        user_role = UserRole(user_id=user.id, role=role)
        await self.user_role_repository.create(user_role)
        self.audit_logger.log_object_write(
            AuditLogMessage.OBJECT_CREATED,
            user_role,
            subject_user_id=user.id,
            role_id=str(role.id),
        )
        self.trigger_webhooks(UserRoleCreated, user_role, schemas.user_role.UserRole)

        if run_in_worker:
            self.send_task(tasks.on_user_role_created, str(user.id), str(role.id))
        else:
            await self.user_role_permissions.add_role_permissions(user, role)

        return user_role

    async def delete_role(
        self, user: User, role: Role, *, run_in_worker: bool = True
    ) -> None:
        user_role = await self.user_role_repository.get_by_role_and_user(
            user.id, role.id
        )
        if user_role is None:
            raise UserRoleDoesNotExist()

        await self.user_role_repository.delete(user_role)
        self.audit_logger.log_object_write(
            AuditLogMessage.OBJECT_DELETED,
            user_role,
            subject_user_id=user.id,
            role_id=str(role.id),
        )
        self.trigger_webhooks(UserRoleDeleted, user_role, schemas.user_role.UserRole)

        self.send_task(tasks.on_user_role_deleted, str(user.id), str(role.id))

        if run_in_worker:
            self.send_task(tasks.on_user_role_deleted, str(user.id), str(role.id))
        else:
            await self.user_role_permissions.delete_role_permissions(user, role)

    async def add_default_roles(
        self, user: User, *, run_in_worker: bool = True
    ) -> None:
        default_roles = await self.role_repository.get_granted_by_default()
        for role in default_roles:
            await self.add_role(user, role, run_in_worker=run_in_worker)

    async def set_roles(
        self,
        user: User,
        target_role_ids: list[UUID4],
        *,
        run_in_worker: bool = True,
    ) -> tuple[list[UserRole], list[UserRole]]:
        """Atomically sync user's roles to match target_role_ids exactly.

        Computes the diff between current and target role sets, applies all
        additions and removals in a single database transaction, then fires
        audit logs, webhooks, and permission sync tasks after commit.

        Returns (added_user_roles, removed_user_roles).
        """
        target_set = set(target_role_ids)

        # 1. Validate ALL target roles exist before making any changes
        roles_by_id: dict[UUID4, Role] = {}
        for role_id in target_set:
            role = await self.role_repository.get_by_id(role_id)
            if role is None:
                raise UserRoleSyncNotExistingRole()
            roles_by_id[role_id] = role

        # 2. Load current user roles and compute diff
        current_user_roles = await self.user_role_repository.list(
            self.user_role_repository.get_by_user_statement(user.id)
        )
        current_role_ids = {ur.role_id for ur in current_user_roles}

        to_add_ids = target_set - current_role_ids
        to_remove_ids = current_role_ids - target_set

        # 3. Fast path: no changes needed
        if not to_add_ids and not to_remove_ids:
            return [], []

        session = self.user_role_repository.session

        # 4. Apply removals
        removed_user_roles = [
            ur for ur in current_user_roles if ur.role_id in to_remove_ids
        ]
        for user_role in removed_user_roles:
            await session.delete(user_role)
        if removed_user_roles:
            await session.flush()

        # 5. Apply additions
        added_user_roles: list[UserRole] = []
        for role_id in to_add_ids:
            user_role = UserRole(user_id=user.id, role=roles_by_id[role_id])
            session.add(user_role)
            added_user_roles.append(user_role)
        if added_user_roles:
            await session.flush()

        # 6. Single commit for all DB changes
        await session.commit()

        # 7. Fire side effects AFTER successful commit
        for user_role in added_user_roles:
            self.audit_logger.log_object_write(
                AuditLogMessage.OBJECT_CREATED,
                user_role,
                subject_user_id=user.id,
                role_id=str(user_role.role_id),
            )
            self.trigger_webhooks(
                UserRoleCreated, user_role, schemas.user_role.UserRole
            )
            if run_in_worker:
                self.send_task(
                    tasks.on_user_role_created,
                    str(user.id),
                    str(user_role.role_id),
                )
            else:
                await self.user_role_permissions.add_role_permissions(
                    user, user_role.role
                )

        for user_role in removed_user_roles:
            self.audit_logger.log_object_write(
                AuditLogMessage.OBJECT_DELETED,
                user_role,
                subject_user_id=user.id,
                role_id=str(user_role.role_id),
            )
            self.trigger_webhooks(
                UserRoleDeleted, user_role, schemas.user_role.UserRole
            )
            if run_in_worker:
                self.send_task(
                    tasks.on_user_role_deleted,
                    str(user.id),
                    str(user_role.role_id),
                )
            else:
                await self.user_role_permissions.delete_role_permissions(
                    user, user_role.role
                )

        return added_user_roles, removed_user_roles
