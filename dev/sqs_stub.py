import json
import os
import time
import uuid
from dataclasses import dataclass

from botocore.exceptions import ClientError

# With the form's rate left empty, move slowly enough to watch a redrive run.
DEFAULT_MOVE_RATE = 1

# Matches AWS: a second purge of the same queue within 60 seconds fails with PurgeQueueInProgress.
PURGE_WINDOW_SECONDS = 60


@dataclass
class _Message:
    message_id: str
    body: str
    sent_timestamp_ms: int
    invisible_until: float = 0.0


class InMemorySqsStub:
    def __init__(
        self,
        queue_urls: dict[str, str],
        queue_messages: dict[str, list[_Message]],
    ):
        self._queue_urls = queue_urls
        self._queue_messages = queue_messages
        self._latest_move_task_by_source_arn: dict[str, dict] = {}
        self._move_rate_by_source_arn: dict[str, int] = {}
        self._last_purge_by_queue_name: dict[str, float] = {}
        self._now = time.time

    @classmethod
    def from_env(cls) -> "InMemorySqsStub":
        raw = os.getenv("SQS_QUEUES", "[]")
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            parsed = []

        sample_count = int(os.getenv("SQS_STUB_SAMPLE_COUNT") or "3")
        now_ms = int(time.time() * 1000)
        queue_urls: dict[str, str] = {}
        queue_messages: dict[str, list[_Message]] = {}
        for item in parsed:
            if not isinstance(item, dict):
                continue
            dlq_arn = item.get("deadletter_queue_arn")
            if not isinstance(dlq_arn, str):
                continue
            dlq_name = dlq_arn.rsplit(":", 1)[-1]
            queue_urls[dlq_name] = f"https://sqs.stub.local/000000000000/{dlq_name}"
            queue_messages[dlq_name] = [
                _Message(
                    message_id=str(uuid.uuid4()),
                    body=json.dumps(
                        {
                            "sample": index + 1,
                            "source": "sqs-stub",
                            "queue": dlq_name,
                        }
                    ),
                    sent_timestamp_ms=now_ms - (index * 1000),
                )
                for index in range(max(sample_count, 0))
            ]
        return cls(queue_urls=queue_urls, queue_messages=queue_messages)

    def get_queue_url(self, QueueName: str):
        return {
            "QueueUrl": self._queue_urls.get(
                QueueName, f"https://sqs.stub.local/{QueueName}"
            )
        }

    def get_queue_attributes(self, QueueUrl: str, AttributeNames: list[str]):
        _ = AttributeNames
        self._advance_move_tasks()
        queue_name = QueueUrl.rsplit("/", 1)[-1]
        now = self._now()
        visible_count = sum(
            1
            for message in self._queue_messages.get(queue_name, [])
            if message.invisible_until <= now
        )
        return {"Attributes": {"ApproximateNumberOfMessages": str(visible_count)}}

    def list_message_move_tasks(self, SourceArn: str, MaxResults: int = 1):
        _ = MaxResults
        self._advance_move_tasks()
        task = self._latest_move_task_by_source_arn.get(SourceArn)
        return {"Results": [task] if task else []}

    def start_message_move_task(
        self,
        SourceArn: str,
        DestinationArn: str,
        MaxNumberOfMessagesPerSecond: int | None = None,
    ):
        _ = DestinationArn
        self._advance_move_tasks()
        queue_name = SourceArn.rsplit(":", 1)[-1]
        to_move = len(self._queue_messages.setdefault(queue_name, []))
        self._move_rate_by_source_arn[SourceArn] = (
            MaxNumberOfMessagesPerSecond or DEFAULT_MOVE_RATE
        )

        task_handle = f"stub-{uuid.uuid4()}"
        self._latest_move_task_by_source_arn[SourceArn] = {
            "TaskHandle": task_handle,
            "Status": "RUNNING",
            "ApproximateNumberOfMessagesMoved": 0,
            "ApproximateNumberOfMessagesToMove": to_move,
            "StartedTimestamp": int(self._now() * 1000),
        }
        return {"TaskHandle": task_handle}

    def cancel_message_move_task(self, TaskHandle: str):
        self._advance_move_tasks()
        for task in self._latest_move_task_by_source_arn.values():
            if task["TaskHandle"] == TaskHandle and task["Status"] == "RUNNING":
                task["Status"] = "CANCELLED"
        return {}

    def purge_queue(self, QueueUrl: str):
        queue_name = QueueUrl.rsplit("/", 1)[-1]
        now = self._now()
        last_purge = self._last_purge_by_queue_name.get(queue_name)
        if last_purge is not None and (now - last_purge) < PURGE_WINDOW_SECONDS:
            raise ClientError(
                {
                    "Error": {
                        "Code": "PurgeQueueInProgress",
                        "Message": "Only one purge can run every 60 seconds.",
                    }
                },
                "PurgeQueue",
            )
        self._queue_messages[queue_name] = []
        self._last_purge_by_queue_name[queue_name] = now
        return {}

    def _advance_move_tasks(self) -> None:
        """Moves the messages each running task is due by now; nothing runs in the background."""
        for source_arn, task in self._latest_move_task_by_source_arn.items():
            if task["Status"] != "RUNNING":
                continue
            elapsed = self._now() - task["StartedTimestamp"] / 1000
            to_move = task["ApproximateNumberOfMessagesToMove"]
            rate = self._move_rate_by_source_arn[source_arn]
            due = min(to_move, int(elapsed * rate))
            newly_moved = due - task["ApproximateNumberOfMessagesMoved"]
            del self._queue_messages[source_arn.rsplit(":", 1)[-1]][:newly_moved]
            task["ApproximateNumberOfMessagesMoved"] = due
            if due == to_move:
                task["Status"] = "COMPLETED"

    def receive_message(
        self,
        QueueUrl: str,
        MaxNumberOfMessages: int = 1,
        VisibilityTimeout: int = 30,
        WaitTimeSeconds: int = 0,
        MessageSystemAttributeNames: list[str] | None = None,
        MessageAttributeNames: list[str] | None = None,
    ):
        _ = WaitTimeSeconds
        _ = MessageSystemAttributeNames
        _ = MessageAttributeNames
        self._advance_move_tasks()
        queue_name = QueueUrl.rsplit("/", 1)[-1]
        now = self._now()
        visible_messages = [
            m
            for m in self._queue_messages.get(queue_name, [])
            if m.invisible_until <= now
        ][: max(MaxNumberOfMessages, 0)]
        for message in visible_messages:
            message.invisible_until = now + VisibilityTimeout

        if not visible_messages:
            return {}
        return {
            "Messages": [
                {
                    "MessageId": message.message_id,
                    "ReceiptHandle": f"stub-receipt-{message.message_id}",
                    "Body": message.body,
                    "Attributes": {"SentTimestamp": str(message.sent_timestamp_ms)},
                }
                for message in visible_messages
            ]
        }
