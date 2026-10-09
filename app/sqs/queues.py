import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

import boto3
from botocore.exceptions import ClientError

# Move-task statuses during which messages are still leaving the DLQ.
ACTIVE_MOVE_STATUSES = frozenset({"RUNNING", "CANCELLING"})

# AWS gives no completion time, so the lag window counts from the start of the move task.
COUNT_LAG_WINDOW_SECONDS = 5 * 60

# AWS allows one purge per queue every 60 seconds, and a purge can take that long to finish.
# AWS has no purge status to query, so the tool tracks the window itself.
PURGE_WINDOW_SECONDS = 60

# AWS limit for StartMessageMoveTask MaxNumberOfMessagesPerSecond.
MAX_MESSAGES_PER_SECOND = 500


class PurgeInProgressError(Exception):
    pass


@dataclass(frozen=True)
class QueueMapping:
    name: str
    arn: str
    deadletter_queue_arn: str

    @property
    def dlq_name(self) -> str:
        return self.deadletter_queue_arn.rsplit(":", 1)[1]


def load_queues() -> list[QueueMapping]:
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


def dlq_url(mapping: QueueMapping, client) -> str:
    return client.get_queue_url(QueueName=mapping.dlq_name)["QueueUrl"]


def list_move_task(mapping: QueueMapping, client) -> dict[str, Any] | None:
    response = client.list_message_move_tasks(
        SourceArn=mapping.deadletter_queue_arn, MaxResults=1
    )
    tasks = response.get("Results", [])
    return tasks[0] if tasks else None


def is_active(move_task: dict[str, Any] | None) -> bool:
    return bool(move_task) and move_task.get("Status") in ACTIVE_MOVE_STATUSES


def _count_messages(mapping: QueueMapping, client, attribute_names: list[str]) -> int:
    attrs = client.get_queue_attributes(
        QueueUrl=dlq_url(mapping, client),
        AttributeNames=attribute_names,
    )["Attributes"]
    return sum(int(attrs.get(name, "0")) for name in attribute_names)


def message_count(mapping: QueueMapping, client) -> int:
    return _count_messages(mapping, client, ["ApproximateNumberOfMessages"])


def purgeable_message_count(mapping: QueueMapping, client) -> int:
    # PurgeQueue also deletes messages that are in flight or delayed, not just the visible ones.
    return _count_messages(
        mapping,
        client,
        [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )


def started_at(move_task: dict[str, Any] | None) -> datetime | None:
    started_ms = (move_task or {}).get("StartedTimestamp")
    if started_ms is None:
        return None
    return datetime.fromtimestamp(started_ms / 1000, UTC)


def start_redrive(
    client, mapping: QueueMapping, max_messages_per_second: int | None = None
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "SourceArn": mapping.deadletter_queue_arn,
        "DestinationArn": mapping.arn,
    }
    if max_messages_per_second is not None:
        params["MaxNumberOfMessagesPerSecond"] = max_messages_per_second
    return client.start_message_move_task(**params)


def cancel_redrive(client, task_handle: str) -> dict[str, Any]:
    return client.cancel_message_move_task(TaskHandle=task_handle)


def purge(client, mapping: QueueMapping) -> dict[str, Any]:
    try:
        return client.purge_queue(QueueUrl=dlq_url(mapping, client))
    except ClientError as err:
        error_code = err.response.get("Error", {}).get("Code")
        if error_code in {
            "PurgeQueueInProgress",
            "AWS.SimpleQueueService.PurgeQueueInProgress",
        }:
            raise PurgeInProgressError() from err
        raise


def sample_messages(client, mapping: QueueMapping) -> list[dict[str, Any]]:
    # Long polling asks every SQS host; short polling samples a few and can return nothing.
    # The visibility timeout must outlast the poll, or SQS hands the same message back to fill the batch.
    wait_seconds = 2
    response = client.receive_message(
        QueueUrl=dlq_url(mapping, client),
        MaxNumberOfMessages=10,
        VisibilityTimeout=wait_seconds + 1,
        WaitTimeSeconds=wait_seconds,
        MessageSystemAttributeNames=["SentTimestamp"],
        MessageAttributeNames=["All"],
    )
    return list(
        {
            message["MessageId"]: message
            for message in response.get("Messages", [])
            if message.get("MessageId")
        }.values()
    )
