from pydantic import UUID4
from sqlalchemy import delete, select, tuple_
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

    _EXISTENCE_CHECK_BATCH_SIZE = 300

    async def create_many_ignore_existing(
        self, objects: list[UserPermission]
    ) -> list[UserPermission]:
        """Insert UserPermission objects, skipping any that already exist.

        Achieves idempotency without database-specific SQL by pre-filtering
        existing records before inserting.
        """
        if not objects:
            return objects

        input_keys = [
            (obj.user_id, obj.permission_id, obj.from_role_id) for obj in objects
        ]

        # Query existing records in sub-batches to stay within SQLite's
        # variable number limit (SQLITE_MAX_VARIABLE_NUMBER).
        existing_keys: set[tuple] = set()
        for i in range(0, len(input_keys), self._EXISTENCE_CHECK_BATCH_SIZE):
            batch = input_keys[i : i + self._EXISTENCE_CHECK_BATCH_SIZE]
            statement = select(
                UserPermission.user_id,
                UserPermission.permission_id,
                UserPermission.from_role_id,
            ).where(
                tuple_(
                    UserPermission.user_id,
                    UserPermission.permission_id,
                    UserPermission.from_role_id,
                ).in_(batch)
            )
            result = await self._execute_query(statement)
            existing_keys.update(row for row in result)

        new_objects = [
            obj
            for obj in objects
            if (obj.user_id, obj.permission_id, obj.from_role_id) not in existing_keys
        ]

        if new_objects:
            self.session.add_all(new_objects)
            await self.session.commit()

        return objects
