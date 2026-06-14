from collections.abc import Sequence

from pydantic import UUID4
from sqlalchemy import delete, select
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import Select

from fief.models import UserPermission
from fief.repositories.base import BaseRepository, UUIDRepositoryMixin


class UserPermissionRepository(
    BaseRepository[UserPermission], UUIDRepositoryMixin[UserPermission]
):
    model = UserPermission

    def get_by_user_statement(
        self, user: UUID4, *, direct_only: bool = False
    ) -> Select:
        statement = (
            select(UserPermission)
            .where(UserPermission.user_id == user)
            .options(
                joinedload(UserPermission.permission),
                joinedload(UserPermission.from_role),
            )
        )

        if direct_only:
            statement = statement.where(UserPermission.from_role == None)

        return statement

    async def get_by_permission_and_user(
        self, user: UUID4, permission: UUID4, *, direct_only: bool = False
    ) -> UserPermission | None:
        statement = (
            select(UserPermission)
            .where(
                UserPermission.user_id == user,
                UserPermission.permission_id == permission,
            )
            .options(
                joinedload(UserPermission.permission),
                joinedload(UserPermission.from_role),
            )
        )

        if direct_only:
            statement = statement.where(UserPermission.from_role == None)

        return await self.get_one_or_none(statement)

    async def delete_by_user_and_role(self, user: UUID4, from_role: UUID4) -> None:
        statement = delete(UserPermission).where(
            UserPermission.user_id == user, UserPermission.from_role_id == from_role
        )
        await self._execute_statement(statement)

    async def delete_by_permission_and_role(
        self, permission: UUID4, from_role: UUID4
    ) -> None:
        statement = delete(UserPermission).where(
            UserPermission.permission_id == permission,
            UserPermission.from_role_id == from_role,
        )
        await self._execute_statement(statement)

    async def delete_by_role(self, from_role: UUID4) -> None:
        statement = delete(UserPermission).where(
            UserPermission.from_role_id == from_role
        )
        await self._execute_statement(statement)

    async def get_existing_permission_pairs(
        self,
        user_ids: Sequence[UUID4],
        permission_ids: Sequence[UUID4],
        from_role: UUID4,
    ) -> set[tuple[UUID4, UUID4]]:
        """Return the ``(user_id, permission_id)`` pairs already granted by a role.

        Restricted to the given users and permissions so it can be used to make the
        propagation of a role's permissions idempotent: only the missing pairs need
        to be inserted, which avoids violating the
        ``(user_id, permission_id, from_role_id)`` unique constraint on retries.
        """
        if not user_ids or not permission_ids:
            return set()

        statement = select(
            UserPermission.user_id, UserPermission.permission_id
        ).where(
            UserPermission.from_role_id == from_role,
            UserPermission.user_id.in_(user_ids),
            UserPermission.permission_id.in_(permission_ids),
        )
        result = await self._execute_query(statement)
        return {(row.user_id, row.permission_id) for row in result}

    async def delete_by_permissions_and_role_for_users(
        self,
        permission_ids: Sequence[UUID4],
        user_ids: Sequence[UUID4],
        from_role: UUID4,
    ) -> None:
        """Revoke the given role-granted permissions for a bounded set of users.

        Scoping the delete to a batch of users keeps the transaction short when a
        role is held by a large number of users.
        """
        if not permission_ids or not user_ids:
            return

        statement = delete(UserPermission).where(
            UserPermission.from_role_id == from_role,
            UserPermission.permission_id.in_(permission_ids),
            UserPermission.user_id.in_(user_ids),
        )
        await self._execute_statement(statement)
