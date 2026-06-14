from pydantic import UUID4
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import Select

from fief.models import UserRole
from fief.repositories.base import BaseRepository, UUIDRepositoryMixin


class UserRoleRepository(BaseRepository[UserRole], UUIDRepositoryMixin[UserRole]):
    model = UserRole

    def get_by_user_statement(self, user: UUID4) -> Select:
        statement = (
            select(UserRole)
            .where(UserRole.user_id == user)
            .options(joinedload(UserRole.role))
        )

        return statement

    async def get_by_role_and_user(self, user: UUID4, role: UUID4) -> UserRole | None:
        return await self.get_one_or_none(
            select(UserRole)
            .where(UserRole.user_id == user, UserRole.role_id == role)
            .options(joinedload(UserRole.role))
        )

    async def get_by_role(self, role: UUID4) -> list[UserRole]:
        return await self.list(select(UserRole).where(UserRole.role_id == role))

    async def get_user_ids_by_role_paginated(
        self, role: UUID4, *, after: UUID4 | None = None, limit: int = 100
    ) -> list[UUID4]:
        """Return up to ``limit`` user ids holding ``role``, ordered by user id.

        Uses keyset pagination on ``user_id`` (``(role_id, user_id)`` is unique) so
        callers can iterate over every user of a role in bounded batches without the
        offset drift of skip/limit pagination. Pass the last returned id as ``after``
        to fetch the next page; an empty list signals the end.
        """
        statement = (
            select(UserRole.user_id)
            .where(UserRole.role_id == role)
            .order_by(UserRole.user_id)
            .limit(limit)
        )
        if after is not None:
            statement = statement.where(UserRole.user_id > after)
        result = await self._execute_query(statement)
        return list(result.scalars().all())
