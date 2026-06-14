# 优化 Cleanup 过期数据清理任务 — 实现方案

## Context

当前 `CleanupTask` 在一个 session 中顺序调用 7 个 repository 的 `delete_expired()`，每次执行 `DELETE FROM <table> WHERE is_expired = true` 全量删除。当 `SessionToken`、`OAuthSession` 等表积累大量过期行时，这种单条大事务 DELETE 会造成数据库锁压力和抖动。需要改为分批删除、可限流、有统计反馈的链路。

## 改动概览

| 文件 | 改动 |
|------|------|
| `fief/repositories/base.py` | 重写 `ExpiresAtMixin.delete_expired()` 为分批删除循环，返回删除计数 |
| `fief/tasks/cleanup.py` | 每个 repository 独立 session + try/except 隔离 + 结构化结果日志 |
| `tests/test_tasks_cleanup.py` | 补 5 个回归测试覆盖多轮删除、空表、非过期数据保留、大批量堆积、错误隔离 |

---

## 1. `fief/repositories/base.py` — 分批删除

### 1.1 更新 Protocol 签名

```python
class ExpiresAtRepositoryProtocol(BaseRepositoryProtocol, Protocol[M_EXPIRES_AT]):
    model: type[M_EXPIRES_AT]
    async def delete_expired(self, batch_size: int = 1000) -> int: ...  # pragma: no cover
```

### 1.2 重写 `ExpiresAtMixin.delete_expired()`

```python
class ExpiresAtMixin(Generic[M_EXPIRES_AT]):
    async def delete_expired(
        self: ExpiresAtRepositoryProtocol[M_EXPIRES_AT],
        batch_size: int = 1000,
    ) -> int:
        total_deleted = 0
        while True:
            dialect = self.session.get_bind().dialect.name
            if dialect == "postgresql":
                # PostgreSQL 不支持 DELETE ... LIMIT，用子查询
                subq = (
                    select(self.model.id)
                    .where(self.model.is_expired.is_(True))
                    .limit(batch_size)
                )
                statement = delete(self.model).where(self.model.id.in_(subq))
            else:
                # MySQL / SQLite 原生支持 DELETE ... LIMIT
                statement = (
                    delete(self.model)
                    .where(self.model.is_expired.is_(True))
                    .limit(batch_size)
                )
            result = await self._execute_statement(statement)
            deleted = result.rowcount
            total_deleted += deleted
            if deleted < batch_size:
                break
        return total_deleted
```

**要点**：
- 每个 batch 通过 `_execute_statement` 自动 commit，是独立事务
- 当 `deleted < batch_size` 时循环终止（没有更多过期行）
- `select` 已在文件顶部 import，无需新增
- 所有 ExpiresAt 模型都继承 `UUIDModel`，保证有 `id` 主键用于子查询

---

## 2. `fief/tasks/cleanup.py` — 独立 session + 错误隔离 + 结果日志

```python
import dramatiq

from fief.logger import logger
from fief.repositories import (
    AuthorizationCodeRepository,
    EmailVerificationRepository,
    LoginSessionRepository,
    OAuthSessionRepository,
    RefreshTokenRepository,
    RegistrationSessionRepository,
    SessionTokenRepository,
)
from fief.repositories.base import ExpiresAtRepositoryProtocol
from fief.tasks.base import TaskBase

CLEANUP_BATCH_SIZE = 1000

repository_classes: list[type[ExpiresAtRepositoryProtocol]] = [
    AuthorizationCodeRepository,
    EmailVerificationRepository,
    LoginSessionRepository,
    OAuthSessionRepository,
    RefreshTokenRepository,
    RegistrationSessionRepository,
    SessionTokenRepository,
]


class CleanupTask(TaskBase):
    __name__ = "cleanup"

    async def run(self):
        results: dict[str, int] = {}
        for repository_class in repository_classes:
            model_name = repository_class.model.__name__
            try:
                async with self.get_main_session() as session:
                    repository = repository_class(session)
                    deleted = await repository.delete_expired(
                        batch_size=CLEANUP_BATCH_SIZE
                    )
                    results[model_name] = deleted
                    logger.debug(
                        "Cleaned up expired records",
                        model=model_name,
                        deleted=deleted,
                    )
            except Exception:
                results[model_name] = -1
                logger.exception(
                    "Failed to clean up expired records",
                    model=model_name,
                )
        logger.info("Cleanup completed", task="cleanup", results=results)
        return results


cleanup = dramatiq.actor(CleanupTask())
```

**要点**：
- 每个 repository 获取独立 session (`async with self.get_main_session()`)，一个失败不影响其他
- `CLEANUP_BATCH_SIZE` 模块级常量，可在测试中通过修改模块属性覆盖
- `results` 字典记录每个 model 的删除行数（-1 表示异常）
- 结构化日志：per-repo debug + 整体 info summary

---

## 3. `tests/test_tasks_cleanup.py` — 回归测试

### 测试场景

| # | 测试名 | 场景 |
|---|--------|------|
| 1 | `test_cleanup_deletes_expired` | 基础场景：验证已有的过期记录（login_sessions["expired"]、authorization_codes["expired"]）被删除，非过期记录保留 |
| 2 | `test_cleanup_multi_round` | 多轮删除：向 SessionToken 表插入 > batch_size 条过期记录，用小 batch_size 验证多轮循环 |
| 3 | `test_cleanup_empty_table` | 空表/无过期行：验证返回 0，无异常 |
| 4 | `test_cleanup_large_accumulation` | 大量堆积：向单表插入远超 batch_size 的过期记录，验证全部清理完毕 |
| 5 | `test_cleanup_isolates_repository_failure` | 错误隔离：mock 某个 repository 抛异常，验证其他 repository 正常清理 |

### 关键实现细节

- 对于需要大量过期记录的测试，直接在测试中动态创建 `SessionToken` / `LoginSession` 等对象并设 `expires_at` 为过去时间
- 使用 `main_session.expire_all()` / `expunge_all()` 刷新 identity map 后再断言
- 通过 `fief.tasks.cleanup` 模块属性修改 `CLEANUP_BATCH_SIZE`（测试后恢复）来控制 batch 大小
- 使用 `main_session_manager` fixture（TaskBase 需要的 async context manager 接口）

---

## 验证方式

1. **运行测试**：`pytest tests/test_tasks_cleanup.py -v`
2. **检查现有测试不受影响**：`pytest tests/ -v`（全量测试）
3. **代码审查要点**：
   - `ExpiresAtMixin.delete_expired()` 的循环在 `deleted < batch_size` 时终止，不会无限循环
   - PostgreSQL 子查询使用 `self.model.id`（所有 ExpiresAt 模型都有 UUID 主键）
   - 每个 batch 是独立 commit（`_execute_statement` 行为），事务粒度可控
