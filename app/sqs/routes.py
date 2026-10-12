import base64
import time
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import ClientError
from fastapi import APIRouter, Form, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from app.common import CSRF_TOKEN, csrf_or_403, write_audit
from app.sqs import notices, queues


def _active_purge_requested_at(
    recent_purges: dict[str, datetime], dlq_arn: str
) -> datetime | None:
    requested_at = recent_purges.get(dlq_arn)
    if requested_at is None:
        return None
    elapsed = (datetime.now(UTC) - requested_at).total_seconds()
    return requested_at if elapsed < queues.PURGE_WINDOW_SECONDS else None


def _queue_row(
    mapping: queues.QueueMapping, client, recent_purges: dict[str, datetime]
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "name": mapping.name,
        "dlq_arn": mapping.deadletter_queue_arn,
        "dlq_name": mapping.dlq_name,
        "message_count": 0,
        "move_task": None,
        "task_active": False,
        "purge_requested_at": None,
        "error": None,
    }
    # One broken queue must not hide the others, so a failure stays on its own row.
    try:
        message_count = queues.message_count(mapping, client)
        move_task = queues.list_move_task(mapping, client)
    except ClientError as err:
        row["error"] = err.response.get("Error", {}).get("Code", "Unknown")
        return row
    row["message_count"] = message_count
    row["move_task"] = move_task
    row["task_active"] = queues.is_active(move_task)
    purge_requested_at = _active_purge_requested_at(
        recent_purges, mapping.deadletter_queue_arn
    )
    row["purge_requested_at"] = (
        purge_requested_at.strftime("%H:%M UTC") if purge_requested_at else None
    )
    started = queues.started_at(move_task)
    row["task_started_at"] = (
        started.strftime("%-d %b %Y %H:%M UTC") if started else None
    )
    row["count_may_lag"] = (
        started is not None
        and move_task.get("Status") == "COMPLETED"
        and message_count > 0
        and (datetime.now(UTC) - started).total_seconds()
        < queues.COUNT_LAG_WINDOW_SECONDS
    )
    return row


def create_router(
    app_context: dict[str, Any],
    base_path: str,
    templates: Jinja2Templates,
) -> tuple[APIRouter, Any, Any]:
    queue_mappings = queues.load_queues()
    router = APIRouter()
    recent_purges: dict[str, datetime] = {}
    dlq_names = {m.deadletter_queue_arn: m.dlq_name for m in queue_mappings}

    def _mapping_for_dlq(dlq_arn: str) -> queues.QueueMapping:
        for mapping in queue_mappings:
            if mapping.deadletter_queue_arn == dlq_arn:
                return mapping
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown DLQ")

    @router.get("/")
    def index(request: Request):
        client = queues.get_sqs_client()
        rows = [
            _queue_row(mapping, client, recent_purges) for mapping in queue_mappings
        ]
        for row in rows:
            if row["error"]:
                write_audit(
                    app_context,
                    "aws.error",
                    "failure",
                    {
                        "sqs": {"source_dlq_arn": row["dlq_arn"]},
                        "error": {"code": row["error"]},
                    },
                )
        write_audit(app_context, "tool.opened", "success")
        return templates.TemplateResponse(
            request=request,
            name="sqs/index.html",
            context={
                "request": request,
                "base_path": base_path,
                "max_messages_per_second": queues.MAX_MESSAGES_PER_SECOND,
                "csrf_token": CSRF_TOKEN,
                "service": app_context["service"],
                "environment": app_context["environment"],
                "queues": rows,
                "show_message_content": app_context["show_message_content"],
                "allow_purge": app_context["allow_purge"],
                "banner": notices.banner_for(
                    request.query_params.get("notice"),
                    dlq_names.get(request.query_params.get("dlq_arn", "")),
                ),
                "tool_title": "SQS Web UI",
            },
        )

    @router.post("/redrive")
    def redrive(
        request: Request,
        dlq_arn: str = Form(...),
        csrf_token: str = Form(default=""),
        max_messages_per_second: int | None = Form(
            default=None, ge=1, le=queues.MAX_MESSAGES_PER_SECOND
        ),
    ):
        csrf_or_403(request, csrf_token)
        mapping = _mapping_for_dlq(dlq_arn)
        if _active_purge_requested_at(recent_purges, mapping.deadletter_queue_arn):
            return notices.redirect_with_notice(
                base_path, mapping.deadletter_queue_arn, "purge-in-progress"
            )
        response = queues.start_redrive(
            queues.get_sqs_client(), mapping, max_messages_per_second
        )
        write_audit(
            app_context,
            "redrive.started",
            "success",
            {
                "sqs": {
                    "source_dlq_arn": mapping.deadletter_queue_arn,
                    "destination_arn": mapping.arn,
                },
                "task": {"handle": response.get("TaskHandle")},
            },
        )
        return notices.redirect_with_notice(
            base_path, mapping.deadletter_queue_arn, "redrive-started"
        )

    @router.post("/purge")
    def purge(
        request: Request,
        dlq_arn: str = Form(...),
        confirm_name: str = Form(default=""),
        csrf_token: str = Form(default=""),
    ):
        if not app_context["allow_purge"]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Not available"
            )
        csrf_or_403(request, csrf_token)
        mapping = _mapping_for_dlq(dlq_arn)
        if confirm_name != mapping.dlq_name:
            return notices.redirect_with_notice(
                base_path, mapping.deadletter_queue_arn, "purge-name-mismatch"
            )

        client = queues.get_sqs_client()
        if queues.is_active(queues.list_move_task(mapping, client)):
            return notices.redirect_with_notice(
                base_path, mapping.deadletter_queue_arn, "redrive-running"
            )

        count = queues.purgeable_message_count(mapping, client)
        try:
            queues.purge(client, mapping)
        except queues.PurgeInProgressError:
            # Someone else's purge is running, so lock the row as if it were ours.
            recent_purges[mapping.deadletter_queue_arn] = datetime.now(UTC)
            return notices.redirect_with_notice(
                base_path, mapping.deadletter_queue_arn, "purge-in-progress"
            )

        recent_purges[mapping.deadletter_queue_arn] = datetime.now(UTC)
        write_audit(
            app_context,
            "queue.purged",
            "success",
            {
                "sqs": {"source_dlq_arn": mapping.deadletter_queue_arn},
                "messages": {"count": count},
            },
        )
        return notices.redirect_with_notice(
            base_path, mapping.deadletter_queue_arn, "purge-requested"
        )

    @router.post("/cancel")
    def cancel(
        request: Request,
        dlq_arn: str = Form(...),
        task_handle: str = Form(...),
        csrf_token: str = Form(default=""),
    ):
        csrf_or_403(request, csrf_token)
        mapping = _mapping_for_dlq(dlq_arn)
        queues.cancel_redrive(queues.get_sqs_client(), task_handle)
        write_audit(
            app_context,
            "redrive.cancelled",
            "success",
            {
                "sqs": {"source_dlq_arn": mapping.deadletter_queue_arn},
                "task": {"handle": task_handle},
            },
        )
        return notices.redirect_with_notice(
            base_path, mapping.deadletter_queue_arn, "redrive-cancelled"
        )

    @router.get("/messages")
    def messages(dlq_arn: str):
        if not app_context["show_message_content"]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Not available"
            )

        mapping = _mapping_for_dlq(dlq_arn)
        client = queues.get_sqs_client()
        if queues.is_active(queues.list_move_task(mapping, client)):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Cannot inspect messages while redrive is running",
            )
        message_items = queues.sample_messages(client, mapping)
        message_ids = [m["MessageId"] for m in message_items]
        write_audit(
            app_context,
            "messages.viewed",
            "success",
            {
                "sqs": {"source_dlq_arn": mapping.deadletter_queue_arn},
                "messages": {"count": len(message_items), "ids": message_ids},
            },
        )
        # Binary message attributes arrive as bytes, which JSON can't carry.
        return JSONResponse(
            jsonable_encoder(
                {"messages": message_items},
                custom_encoder={
                    bytes: lambda value: base64.b64encode(value).decode("ascii")
                },
            )
        )

    @router.get("/health")
    def health():
        return {"status": "ok", "time": int(time.time())}

    return router, index, health
