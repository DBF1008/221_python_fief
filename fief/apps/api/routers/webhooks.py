from fastapi import APIRouter, Depends, Response, status

from fief import schemas
from fief.dependencies.admin_authentication import is_authenticated_admin_api
from fief.dependencies.logger import get_audit_logger
from fief.dependencies.pagination import PaginatedObjects
from fief.dependencies.tasks import get_send_task
from fief.dependencies.webhook import (
    get_paginated_webhook_logs,
    get_paginated_webhooks,
    get_webhook_by_id_or_404,
    get_webhook_log_by_id_and_webhook_or_404,
)
from fief.logger import AuditLogger
from fief.models import AuditLogMessage, Webhook, WebhookLog
from fief.repositories import WebhookRepository
from fief.schemas.generics import PaginatedResults
from fief.tasks import SendTask, replay_webhook

router = APIRouter(dependencies=[Depends(is_authenticated_admin_api)])


@router.get(
    "/", name="webhooks:list", response_model=PaginatedResults[schemas.webhook.Webhook]
)
async def list_webhooks(
    paginated_webhooks: PaginatedObjects[Webhook] = Depends(get_paginated_webhooks),
) -> PaginatedResults[schemas.webhook.Webhook]:
    webhooks, count = paginated_webhooks
    return PaginatedResults(
        count=count,
        results=[
            schemas.webhook.Webhook.model_validate(webhook) for webhook in webhooks
        ],
    )


@router.post(
    "/",
    name="webhooks:create",
    response_model=schemas.webhook.WebhookSecret,
    status_code=status.HTTP_201_CREATED,
)
async def create_webhook(
    webhook_create: schemas.webhook.WebhookCreate,
    repository: WebhookRepository = Depends(WebhookRepository),
    audit_logger: AuditLogger = Depends(get_audit_logger),
) -> schemas.webhook.WebhookSecret:
    webhook = Webhook(**webhook_create.model_dump())
    webhook = await repository.create(webhook)
    audit_logger.log_object_write(AuditLogMessage.OBJECT_CREATED, webhook)

    return schemas.webhook.WebhookSecret.model_validate(webhook)


@router.get("/{id:uuid}", name="webhooks:get", response_model=schemas.webhook.Webhook)
async def get_webhook(webhook: Webhook = Depends(get_webhook_by_id_or_404)) -> Webhook:
    return webhook


@router.patch(
    "/{id:uuid}", name="webhooks:update", response_model=schemas.webhook.Webhook
)
async def update_webhook(
    webhook_update: schemas.webhook.WebhookUpdate,
    webhook: Webhook = Depends(get_webhook_by_id_or_404),
    repository: WebhookRepository = Depends(WebhookRepository),
    audit_logger: AuditLogger = Depends(get_audit_logger),
) -> schemas.webhook.Webhook:
    webhook_update_dict = webhook_update.model_dump(exclude_unset=True)
    for field, value in webhook_update_dict.items():
        setattr(webhook, field, value)

    await repository.update(webhook)
    audit_logger.log_object_write(AuditLogMessage.OBJECT_UPDATED, webhook)

    return schemas.webhook.Webhook.model_validate(webhook)


@router.post(
    "/{id:uuid}/secret",
    name="webhooks:secret",
    response_model=schemas.webhook.WebhookSecret,
)
async def regenerate_webhook_secret(
    webhook: Webhook = Depends(get_webhook_by_id_or_404),
    repository: WebhookRepository = Depends(WebhookRepository),
    audit_logger: AuditLogger = Depends(get_audit_logger),
):
    webhook.regenerate_secret()
    await repository.update(webhook)
    audit_logger.log_object_write(AuditLogMessage.OBJECT_UPDATED, webhook)

    return webhook


@router.delete(
    "/{id:uuid}",
    name="webhooks:delete",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def delete_role(
    webhook: Webhook = Depends(get_webhook_by_id_or_404),
    repository: WebhookRepository = Depends(WebhookRepository),
    audit_logger: AuditLogger = Depends(get_audit_logger),
):
    await repository.delete(webhook)
    audit_logger.log_object_write(AuditLogMessage.OBJECT_DELETED, webhook)


@router.get(
    "/{id:uuid}/logs",
    name="webhooks:logs",
    response_model=PaginatedResults[schemas.webhook_log.WebhookLog],
)
async def list_webhook_logs(
    paginated_webhook_logs: PaginatedObjects[WebhookLog] = Depends(
        get_paginated_webhook_logs
    ),
):
    webhook_logs, count = paginated_webhook_logs
    return PaginatedResults(
        count=count,
        results=[
            schemas.webhook_log.WebhookLog.model_validate(webhook_log)
            for webhook_log in webhook_logs
        ],
    )


@router.post(
    "/{id:uuid}/logs/{log_id:uuid}/replay",
    name="webhooks:replay_log",
    response_model=schemas.webhook_log.WebhookLog,
    status_code=status.HTTP_202_ACCEPTED,
)
async def replay_webhook_log(
    webhook_log: WebhookLog = Depends(get_webhook_log_by_id_and_webhook_or_404),
    send_task: SendTask = Depends(get_send_task),
) -> schemas.webhook_log.WebhookLog:
    send_task(
        replay_webhook,
        webhook_id=str(webhook_log.webhook_id),
        webhook_log_id=str(webhook_log.id),
    )
    return schemas.webhook_log.WebhookLog.model_validate(webhook_log)
