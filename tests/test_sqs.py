import base64
import importlib
import json
from functools import lru_cache

import boto3
import pytest
from botocore.stub import Stubber
from fastapi.testclient import TestClient
from moto import mock_aws


@pytest.fixture(autouse=True)
def move_tasks(monkeypatch):
    """moto doesn't implement ListMessageMoveTasks, so the app's client answers from this list."""
    sqs_routes = importlib.import_module("app.sqs.routes")
    tasks: list[dict] = []
    build_client = sqs_routes.get_sqs_client.__wrapped__

    @lru_cache
    def get_sqs_client():
        client = build_client()
        client.list_message_move_tasks = lambda **_: {"Results": tasks}  # type: ignore[method-assign]
        return client

    monkeypatch.setattr(sqs_routes, "get_sqs_client", get_sqs_client)
    return tasks


def _create_queues(name: str) -> tuple[str, str, str]:
    """Creates a queue and its DLQ, returning (SQS_QUEUES json, dlq url, dlq arn)."""
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName=name)["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    dlq = sqs.create_queue(QueueName=f"{name}-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    mappings = json.dumps([{"name": name, "arn": source_arn, "deadletter_queue_arn": dlq_arn}])
    return mappings, dlq, dlq_arn


def _load_module(monkeypatch, show_message_content: str = "false", token: str | None = None):
    if token is None:
        monkeypatch.delenv("TOKEN", raising=False)
    else:
        monkeypatch.setenv("TOKEN", token)
    monkeypatch.setenv("SHOW_MESSAGE_CONTENT", show_message_content)
    monkeypatch.setenv("SERVICE", "demo-service")
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("USER_ID", "user-1")
    monkeypatch.setenv("USER_NAME", "User One")
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    monkeypatch.setenv("AUDIT_LOG_PATH", "/tmp/cdp-aws-tools.audit")
    if "app.main" in importlib.sys.modules:
        del importlib.sys.modules["app.main"]
    return importlib.import_module("app.main")


@mock_aws
def test_index_and_redrive(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="orders")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    dlq = sqs.create_queue(QueueName="orders-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.send_message(QueueUrl=dlq, MessageBody="hello")

    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps([{"name": "orders", "arn": source_arn, "url": source, "deadletter_queue_arn": dlq_arn}]),
    )
    module = _load_module(monkeypatch)
    client = TestClient(module.app)

    home = client.get("/")
    assert home.status_code == 200
    assert "orders-deadletter" in home.text

    sqs_routes = importlib.import_module("app.sqs.routes")
    csrf_module = importlib.import_module("app.common.csrf")
    real_client = sqs_routes.get_sqs_client()
    real_client.start_message_move_task = lambda **_: {"TaskHandle": "task-1"}  # type: ignore[attr-defined]
    monkeypatch.setattr(sqs_routes, "get_sqs_client", lambda: real_client)
    response = client.post(
        "/redrive",
        data={"dlq_arn": dlq_arn, "csrf_token": csrf_module.CSRF_TOKEN},
        follow_redirects=False,
    )
    assert response.status_code == 303


@mock_aws
def test_messages_hidden_without_flag(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="invoices")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    dlq = sqs.create_queue(QueueName="invoices-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps([{"name": "invoices", "arn": source_arn, "url": source, "deadletter_queue_arn": dlq_arn}]),
    )
    module = _load_module(monkeypatch, show_message_content="false")
    client = TestClient(module.app)

    response = client.get(f"/messages?dlq_arn={dlq_arn}")
    assert response.status_code == 404


@mock_aws
def test_messages_visible_with_flag(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="reports")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    dlq = sqs.create_queue(QueueName="reports-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.send_message(QueueUrl=dlq, MessageBody="world")
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps([{"name": "reports", "arn": source_arn, "url": source, "deadletter_queue_arn": dlq_arn}]),
    )
    module = _load_module(monkeypatch, show_message_content="true")
    client = TestClient(module.app)

    response = client.get(f"/messages?dlq_arn={dlq_arn}")
    assert response.status_code == 200
    assert len(response.json()["messages"]) == 1


@mock_aws
def test_routes_served_under_token_prefix(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="payments")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    dlq = sqs.create_queue(QueueName="payments-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.send_message(QueueUrl=dlq, MessageBody="failed payment")
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps([{"name": "payments", "arn": source_arn, "url": source, "deadletter_queue_arn": dlq_arn}]),
    )
    module = _load_module(monkeypatch, show_message_content="true", token="tok123")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")

    for path in ("/tok123", "/tok123/"):
        page = client.get(path, follow_redirects=False)
        assert page.status_code == 200
        assert 'action="/tok123/redrive"' in page.text
        assert 'href="/tok123/messages?' in page.text
        assert 'href="/tok123/static/app.css"' in page.text

    stylesheet = client.get("/tok123/static/app.css")
    assert stylesheet.status_code == 200
    assert ".app-entity-table" in stylesheet.text
    assert "node_modules" not in stylesheet.text
    assert client.get("/tok123/static/govuk/fonts/bold-b542beb274-v2.woff2").status_code == 200

    assert client.get("/").status_code == 404
    assert client.get("/health").status_code == 200
    assert client.get("/tok123/health").status_code == 200

    sqs_routes = importlib.import_module("app.sqs.routes")
    real_client = sqs_routes.get_sqs_client()
    real_client.start_message_move_task = lambda **_: {"TaskHandle": "task-1"}  # type: ignore[attr-defined]
    monkeypatch.setattr(sqs_routes, "get_sqs_client", lambda: real_client)
    response = client.post(
        "/tok123/redrive",
        data={"dlq_arn": dlq_arn, "csrf_token": csrf_module.CSRF_TOKEN},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/tok123/"


@mock_aws
def test_redrive_rate_capped_to_aws_limit(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="refunds")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    dlq = sqs.create_queue(QueueName="refunds-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps([{"name": "refunds", "arn": source_arn, "url": source, "deadletter_queue_arn": dlq_arn}]),
    )
    _load_module(monkeypatch)
    client = TestClient(importlib.import_module("app.main").app)
    csrf_module = importlib.import_module("app.common.csrf")
    sqs_routes = importlib.import_module("app.sqs.routes")

    calls = []
    real_client = sqs_routes.get_sqs_client()
    real_client.start_message_move_task = lambda **kwargs: calls.append(kwargs) or {"TaskHandle": "t"}  # type: ignore[attr-defined]
    monkeypatch.setattr(sqs_routes, "get_sqs_client", lambda: real_client)

    def post(rate: str):
        return client.post(
            "/redrive",
            data={"dlq_arn": dlq_arn, "max_messages_per_second": rate, "csrf_token": csrf_module.CSRF_TOKEN},
            follow_redirects=False,
        )

    assert post("0").status_code == 422
    assert post("501").status_code == 422
    assert calls == []

    assert post("500").status_code == 303
    assert calls[-1]["MaxNumberOfMessagesPerSecond"] == 500

    assert post("").status_code == 303
    assert "MaxNumberOfMessagesPerSecond" not in calls[-1]


@mock_aws
def test_redrive_requires_csrf_token_not_matching_host(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="claims")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    dlq = sqs.create_queue(QueueName="claims-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.send_message(QueueUrl=dlq, MessageBody="failed claim")
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps([{"name": "claims", "arn": source_arn, "url": source, "deadletter_queue_arn": dlq_arn}]),
    )
    _load_module(monkeypatch)
    client = TestClient(importlib.import_module("app.main").app)
    csrf_module = importlib.import_module("app.common.csrf")
    sqs_routes = importlib.import_module("app.sqs.routes")

    real_client = sqs_routes.get_sqs_client()
    real_client.start_message_move_task = lambda **_: {"TaskHandle": "t"}  # type: ignore[attr-defined]
    monkeypatch.setattr(sqs_routes, "get_sqs_client", lambda: real_client)

    assert f'name="csrf_token" value="{csrf_module.CSRF_TOKEN}"' in client.get("/").text

    proxied = {"origin": "http://cdp.127.0.0.1.sslip.io:8000", "sec-fetch-site": "same-origin"}
    accepted = client.post(
        "/redrive",
        data={"dlq_arn": dlq_arn, "csrf_token": csrf_module.CSRF_TOKEN},
        headers=proxied,
        follow_redirects=False,
    )
    assert accepted.status_code == 303

    assert client.post("/redrive", data={"dlq_arn": dlq_arn}, headers=proxied).status_code == 403
    cross_site = client.post(
        "/redrive",
        data={"dlq_arn": dlq_arn, "csrf_token": csrf_module.CSRF_TOKEN},
        headers={"sec-fetch-site": "cross-site"},
    )
    assert cross_site.status_code == 403


@mock_aws
def test_reject_unknown_dlq(monkeypatch):
    mappings, _, _ = _create_queues("known")
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")
    unknown_arn = "arn:aws:sqs:eu-west-2:111111111111:unknown-deadletter"

    response = client.post("/redrive", data={"dlq_arn": unknown_arn, "csrf_token": csrf_module.CSRF_TOKEN})
    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown DLQ"

    response = client.get("/messages", params={"dlq_arn": unknown_arn})
    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown DLQ"


@mock_aws
def test_non_ascii_csrf_token_is_forbidden(monkeypatch):
    mappings, _, dlq_arn = _create_queues("letters")
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch)
    client = TestClient(module.app)

    response = client.post("/redrive", data={"dlq_arn": dlq_arn, "csrf_token": "é"})
    assert response.status_code == 403


@mock_aws
def test_cancel_running_redrive(monkeypatch, move_tasks):
    mappings, dlq, dlq_arn = _create_queues("shipments")
    boto3.client("sqs", region_name="eu-west-2").send_message(QueueUrl=dlq, MessageBody="stuck")
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")
    sqs_routes = importlib.import_module("app.sqs.routes")
    move_tasks.append({"TaskHandle": "handle-1", "Status": "RUNNING"})

    page = client.get("/")
    assert "Cancel redrive" in page.text
    assert "Start redrive" not in page.text
    assert "View messages" not in page.text

    # The stubber checks the call against the real API model, which moto can't do for this operation.
    with Stubber(sqs_routes.get_sqs_client()) as stubber:
        stubber.add_response("cancel_message_move_task", {}, {"TaskHandle": "handle-1"})
        response = client.post(
            "/cancel",
            data={"dlq_arn": dlq_arn, "task_handle": "handle-1", "csrf_token": csrf_module.CSRF_TOKEN},
            follow_redirects=False,
        )
        stubber.assert_no_pending_responses()
    assert response.status_code == 303

    move_tasks[0]["Status"] = "CANCELLING"
    assert client.get("/messages", params={"dlq_arn": dlq_arn}).status_code == 409


@mock_aws
def test_messages_with_binary_attribute(monkeypatch):
    mappings, dlq, dlq_arn = _create_queues("uploads")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq,
        MessageBody="payload",
        MessageAttributes={"blob": {"DataType": "Binary", "BinaryValue": b"\xff\x00"}},
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true")
    client = TestClient(module.app)

    response = client.get("/messages", params={"dlq_arn": dlq_arn})
    assert response.status_code == 200
    attribute = response.json()["messages"][0]["MessageAttributes"]["blob"]
    assert attribute["BinaryValue"] == base64.b64encode(b"\xff\x00").decode()


@mock_aws
def test_no_queues_shows_empty_state(monkeypatch):
    monkeypatch.setenv("SQS_QUEUES", "[]")
    module = _load_module(monkeypatch, token="tok123")
    client = TestClient(module.app)

    for path in ("/tok123", "/tok123/"):
        page = client.get(path)
        assert page.status_code == 200
        assert "This service has no queues with a dead letter queue in dev" in page.text
    assert client.get("/health").status_code == 200
    assert client.get("/tok123/health").status_code == 200


@mock_aws
def test_failing_queue_does_not_hide_the_others(monkeypatch):
    mappings, dlq, _ = _create_queues("healthy")
    boto3.client("sqs", region_name="eu-west-2").send_message(QueueUrl=dlq, MessageBody="hello")
    broken = {
        "name": "broken",
        "arn": "arn:aws:sqs:eu-west-2:123456789012:broken",
        "deadletter_queue_arn": "arn:aws:sqs:eu-west-2:123456789012:broken-deadletter",
    }
    monkeypatch.setenv("SQS_QUEUES", json.dumps([*json.loads(mappings), broken]))
    module = _load_module(monkeypatch)
    client = TestClient(module.app)

    page = client.get("/")
    assert page.status_code == 200
    assert "Could not load this queue" in page.text
    assert page.text.count("Start redrive") == 1
