import base64
import json
import os
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import boto3
from botocore.exceptions import ClientError
from fastapi import APIRouter, Form, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.common import CSRF_TOKEN, csrf_or_403, write_audit

# Move-task statuses during which messages are still leaving the DLQ.
ACTIVE_MOVE_STATUSES = frozenset({"RUNNING", "CANCELLING"})


@dataclass(frozen=True)
class QueueMapping:
    name: str
    arn: str
    deadletter_queue_arn: str

    @property
    def dlq_name(self) -> str:
        return self.deadletter_queue_arn.rsplit(":", 1)[1]


def _load_queues() -> list[QueueMapping]:
    raw = os.getenv("SQS_QUEUES", "[]")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as err:
        raise RuntimeError("SQS_QUEUES must be valid JSON") from err

    queue_mappings: list[QueueMapping] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        required = {"name", "arn", "deadletter_queue_arn"}
        if not required.issubset(set(item.keys())):
            continue
        queue_mappings.append(
            QueueMapping(
                name=item["name"],
                arn=item["arn"],
                deadletter_queue_arn=item["deadletter_queue_arn"],
            )
        )
    return queue_mappings


@lru_cache
def get_sqs_client():
    return boto3.client("sqs", region_name=os.getenv("AWS_REGION", "eu-west-2"))


def _dlq_url(mapping: QueueMapping, client) -> str:
    return client.get_queue_url(QueueName=mapping.dlq_name)["QueueUrl"]


def _list_move_task(mapping: QueueMapping, client) -> dict[str, Any] | None:
    response = client.list_message_move_tasks(SourceArn=mapping.deadletter_queue_arn, MaxResults=1)
    tasks = response.get("Results", [])
    return tasks[0] if tasks else None


def _is_active(move_task: dict[str, Any] | None) -> bool:
    return bool(move_task) and move_task.get("Status") in ACTIVE_MOVE_STATUSES


def _queue_row(mapping: QueueMapping, client) -> dict[str, Any]:
    row: dict[str, Any] = {
        "name": mapping.name,
        "dlq_arn": mapping.deadletter_queue_arn,
        "dlq_name": mapping.dlq_name,
        "message_count": 0,
        "move_task": None,
        "task_active": False,
        "error": None,
    }
    # One broken queue must not hide the others, so a failure stays on its own row.
    try:
        attrs = client.get_queue_attributes(
            QueueUrl=_dlq_url(mapping, client),
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]
        move_task = _list_move_task(mapping, client)
    except ClientError as err:
        row["error"] = err.response.get("Error", {}).get("Code", "Unknown")
        return row
    row["message_count"] = int(attrs.get("ApproximateNumberOfMessages", "0"))
    row["move_task"] = move_task
    row["task_active"] = _is_active(move_task)
    return row


def create_router(
    app_context: dict[str, Any],
    base_path: str,
    templates: Jinja2Templates,
) -> tuple[APIRouter, Any, Any]:
    queue_mappings = _load_queues()
    router = APIRouter()

    # AWS limit for StartMessageMoveTask MaxNumberOfMessagesPerSecond.
    max_messages_per_second = 500

    def _mapping_for_dlq(dlq_arn: str) -> QueueMapping:
        for mapping in queue_mappings:
            if mapping.deadletter_queue_arn == dlq_arn:
                return mapping
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown DLQ")

    @router.get("/")
    def index(request: Request):
        client = get_sqs_client()
        rows = [_queue_row(mapping, client) for mapping in queue_mappings]
        for row in rows:
            if row["error"]:
                write_audit(
                    app_context,
                    "aws.error",
                    "failure",
                    {"sqs": {"source_dlq_arn": row["dlq_arn"]}, "error": {"code": row["error"]}},
                )
        write_audit(app_context, "tool.opened", "success")
        return templates.TemplateResponse(
            request=request,
            name="sqs/index.html",
            context={
                "request": request,
                "base_path": base_path,
                "max_messages_per_second": max_messages_per_second,
                "csrf_token": CSRF_TOKEN,
                "service": app_context["service"],
                "environment": app_context["environment"],
                "queues": rows,
                "show_message_content": app_context["show_message_content"],
                "tool_title": "SQS Web UI",
            },
        )

    @router.post("/redrive")
    def redrive(
        request: Request,
        dlq_arn: str = Form(...),
        csrf_token: str = Form(default=""),
        max_messages_per_second: int | None = Form(default=None, ge=1, le=max_messages_per_second),
    ):
        csrf_or_403(request, csrf_token)
        mapping = _mapping_for_dlq(dlq_arn)
        client = get_sqs_client()
        params: dict[str, Any] = {
            "SourceArn": mapping.deadletter_queue_arn,
            "DestinationArn": mapping.arn,
        }
        if max_messages_per_second is not None:
            params["MaxNumberOfMessagesPerSecond"] = max_messages_per_second
        response = client.start_message_move_task(**params)
        write_audit(
            app_context,
            "redrive.started",
            "success",
            {
                "sqs": {"source_dlq_arn": mapping.deadletter_queue_arn, "destination_arn": mapping.arn},
                "task": {"handle": response.get("TaskHandle")},
            },
        )
        return RedirectResponse(f"{base_path}/", status_code=status.HTTP_303_SEE_OTHER)

    @router.post("/cancel")
    def cancel(
        request: Request,
        dlq_arn: str = Form(...),
        task_handle: str = Form(...),
        csrf_token: str = Form(default=""),
    ):
        csrf_or_403(request, csrf_token)
        mapping = _mapping_for_dlq(dlq_arn)
        client = get_sqs_client()
        client.cancel_message_move_task(TaskHandle=task_handle)
        write_audit(
            app_context,
            "redrive.cancelled",
            "success",
            {"sqs": {"source_dlq_arn": mapping.deadletter_queue_arn}, "task": {"handle": task_handle}},
        )
        return RedirectResponse(f"{base_path}/", status_code=status.HTTP_303_SEE_OTHER)

    @router.get("/status")
    def redrive_status(dlq_arn: str):
        mapping = _mapping_for_dlq(dlq_arn)
        return {"redrive_active": _is_active(_list_move_task(mapping, get_sqs_client()))}

    @router.get("/messages")
    def messages(dlq_arn: str):
        if not app_context["show_message_content"]:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not available")

        mapping = _mapping_for_dlq(dlq_arn)
        client = get_sqs_client()
        if _is_active(_list_move_task(mapping, client)):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Cannot inspect messages while redrive is running",
            )

        # Long polling asks every SQS host; short polling samples a few and can return nothing.
        # The visibility timeout must outlast the poll, or SQS hands the same message back to fill the batch.
        wait_seconds = 2
        response = client.receive_message(
            QueueUrl=_dlq_url(mapping, client),
            MaxNumberOfMessages=10,
            VisibilityTimeout=wait_seconds + 1,
            WaitTimeSeconds=wait_seconds,
            MessageSystemAttributeNames=["SentTimestamp"],
            MessageAttributeNames=["All"],
        )
        message_items = list(
            {m["MessageId"]: m for m in response.get("Messages", []) if m.get("MessageId")}.values()
        )
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
                custom_encoder={bytes: lambda value: base64.b64encode(value).decode("ascii")},
            )
        )

    @router.get("/health")
    def health():
        return {"status": "ok", "time": int(time.time())}

    return router, index, health
