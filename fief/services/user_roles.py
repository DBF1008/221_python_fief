import dataclasses

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


@dataclasses.dataclass
class UserRolesSyncResult:
    added: list[UserRole]
    removed: list[UserRole]
    roles: list[UserRole]


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

    async def set_roles(
        self, user: User, roles: list[Role], *, run_in_worker: bool = True
    ) -> UserRolesSyncResult:
        existing_user_roles = await self.user_role_repository.list(
            self.user_role_repository.get_by_user_statement(user.id)
        )
        existing_by_role_id = {
            user_role.role_id: user_role for user_role in existing_user_roles
        }
        target_by_role_id = {role.id: role for role in roles}

        role_ids_to_add = target_by_role_id.keys() - existing_by_role_id.keys()
        role_ids_to_remove = existing_by_role_id.keys() - target_by_role_id.keys()

        user_roles_to_add = [
            UserRole(user_id=user.id, role=target_by_role_id[role_id])
            for role_id in role_ids_to_add
        ]
        user_roles_to_remove = [
            existing_by_role_id[role_id] for role_id in role_ids_to_remove
        ]
        resulting_user_roles = [
            user_role
            for user_role in existing_user_roles
            if user_role.role_id not in role_ids_to_remove
        ] + user_roles_to_add

        # Nothing to change: the request is idempotent, so we skip the
        # transaction and all side effects entirely.
        if not user_roles_to_add and not user_roles_to_remove:
            return UserRolesSyncResult(added=[], removed=[], roles=resulting_user_roles)

        # Apply the whole diff in a single transaction so a mid-way failure can
        # never leave the role set half-updated.
        session = self.user_role_repository.session
        try:
            for user_role in user_roles_to_add:
                session.add(user_role)
            for user_role in user_roles_to_remove:
                await session.delete(user_role)
            await session.commit()
        except Exception:
            await session.rollback()
            raise

        # Only once the diff is durably committed do we emit the side effects
        # (audit, webhooks, downstream permission sync) in a single consistent
        # pass, mirroring `add_role` / `delete_role`.
        for user_role in user_roles_to_add:
            self.audit_logger.log_object_write(
                AuditLogMessage.OBJECT_CREATED,
                user_role,
                subject_user_id=user.id,
                role_id=str(user_role.role.id),
            )
            self.trigger_webhooks(
                UserRoleCreated, user_role, schemas.user_role.UserRole
            )
            if run_in_worker:
                self.send_task(
                    tasks.on_user_role_created, str(user.id), str(user_role.role.id)
                )
            else:
                await self.user_role_permissions.add_role_permissions(
                    user, user_role.role
                )

        for user_role in user_roles_to_remove:
            self.audit_logger.log_object_write(
                AuditLogMessage.OBJECT_DELETED,
                user_role,
                subject_user_id=user.id,
                role_id=str(user_role.role.id),
            )
            self.trigger_webhooks(
                UserRoleDeleted, user_role, schemas.user_role.UserRole
            )
            if run_in_worker:
                self.send_task(
                    tasks.on_user_role_deleted, str(user.id), str(user_role.role.id)
                )
            else:
                await self.user_role_permissions.delete_role_permissions(
                    user, user_role.role
                )

        return UserRolesSyncResult(
            added=user_roles_to_add,
            removed=user_roles_to_remove,
            roles=resulting_user_roles,
        )

    async def add_default_roles(
        self, user: User, *, run_in_worker: bool = True
    ) -> None:
        default_roles = await self.role_repository.get_granted_by_default()
        for role in default_roles:
            await self.add_role(user, role, run_in_worker=run_in_worker)
