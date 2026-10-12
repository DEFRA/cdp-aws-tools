import json
import sys
from dataclasses import dataclass
from functools import lru_cache
from importlib import import_module
from pathlib import Path

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

from app.common import CSRF_TOKEN
from app.sqs import queues

REGION = "eu-west-2"


@dataclass(frozen=True)
class Dlq:
    name: str
    url: str
    arn: str
    mapping: dict[str, str]


class ToolClient(TestClient):
    """Speaks the tool's forms: posts carry the CSRF token, and redirects are left unfollowed."""

    def __init__(self, app):
        super().__init__(app, follow_redirects=False)

    def post_form(self, path: str, headers: dict[str, str] | None = None, **fields):
        return self.post(
            path, data={"csrf_token": CSRF_TOKEN, **fields}, headers=headers
        )

    def redrive(self, dlq_arn: str, **fields):
        return self.post_form("/redrive", dlq_arn=dlq_arn, **fields)

    def purge(self, dlq_arn: str, confirm_name: str):
        return self.post_form("/purge", dlq_arn=dlq_arn, confirm_name=confirm_name)

    def cancel(self, dlq_arn: str, task_handle: str):
        return self.post_form("/cancel", dlq_arn=dlq_arn, task_handle=task_handle)

    def sample(self, dlq_arn: str):
        return self.get("/messages", params={"dlq_arn": dlq_arn})


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    # A custom endpoint (e.g. LocalStack) bypasses moto, so tests would hit a real, stateful SQS.
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_SQS", raising=False)
    monkeypatch.delenv("TOKEN", raising=False)
    monkeypatch.setenv("AWS_REGION", REGION)
    monkeypatch.setenv("SERVICE", "demo-service")
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("USER_ID", "user-1")
    monkeypatch.setenv("USER_NAME", "User One")
    monkeypatch.setenv("SQS_QUEUES", "[]")


@pytest.fixture(autouse=True)
def audit_log(monkeypatch, tmp_path) -> Path:
    """Each test writes its audit trail to its own file, which pytest cleans up."""
    path = tmp_path / "audit.log"
    monkeypatch.setenv("AUDIT_LOG_PATH", str(path))
    return path


@pytest.fixture(autouse=True)
def sqs():
    """A moto-backed SQS client. Autouse, so no test can reach real AWS."""
    with mock_aws():
        yield boto3.client("sqs", region_name=REGION)


@pytest.fixture
def move_tasks() -> list[dict]:
    """The move tasks the app sees for every DLQ."""
    return []


@pytest.fixture
def redrive_calls() -> list[dict]:
    """The params of each StartMessageMoveTask call the app made."""
    return []


@pytest.fixture(autouse=True)
def _fill_moto_gaps(monkeypatch, move_tasks, redrive_calls):
    """moto doesn't implement the message move task operations, so the app's client fakes them."""
    build_client = queues.get_sqs_client.__wrapped__

    @lru_cache
    def get_sqs_client():
        client = build_client()
        client.list_message_move_tasks = lambda **_: {"Results": move_tasks}  # type: ignore[method-assign]
        client.start_message_move_task = (  # type: ignore[method-assign]
            lambda **params: redrive_calls.append(params) or {"TaskHandle": "task-1"}
        )
        return client

    monkeypatch.setattr(queues, "get_sqs_client", get_sqs_client)


@pytest.fixture
def dlq(sqs, monkeypatch) -> Dlq:
    """A queue and its dead letter queue, configured as the app's only mapping."""

    def create(name: str) -> tuple[str, str]:
        url = sqs.create_queue(QueueName=name)["QueueUrl"]
        attributes = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
        return url, attributes["Attributes"]["QueueArn"]

    _, source_arn = create("orders")
    dlq_url, dlq_arn = create("orders-deadletter")
    mapping = {"name": "orders", "arn": source_arn, "deadletter_queue_arn": dlq_arn}
    monkeypatch.setenv("SQS_QUEUES", json.dumps([mapping]))
    return Dlq(name="orders-deadletter", url=dlq_url, arn=dlq_arn, mapping=mapping)


@pytest.fixture
def load_app(monkeypatch):
    """Imports the app afresh, because it reads its settings from the environment on import."""

    def load(
        *,
        show_message_content: bool = False,
        allow_purge: bool = False,
        token: str | None = None,
        module: str = "app.main",
    ):
        monkeypatch.setenv("SHOW_MESSAGE_CONTENT", str(show_message_content).lower())
        monkeypatch.setenv("ALLOW_PURGE", str(allow_purge).lower())
        if token:
            monkeypatch.setenv("TOKEN", token)
        for name in {"app.main", module}:
            monkeypatch.delitem(sys.modules, name, raising=False)
        return import_module(module)

    return load


@pytest.fixture
def make_client(load_app):
    """Builds a client for the app; takes the same settings as load_app."""

    def make(**settings) -> ToolClient:
        return ToolClient(load_app(**settings).app)

    return make
