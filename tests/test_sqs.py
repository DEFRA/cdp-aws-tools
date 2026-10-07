import importlib
import json

import boto3
from fastapi.testclient import TestClient
from moto import mock_aws


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
    monkeypatch.setenv("SQS_QUEUES", "[]")
    _load_module(monkeypatch, show_message_content="true")
    client = TestClient(importlib.import_module("app.main").app)
    csrf_module = importlib.import_module("app.common.csrf")

    response = client.post(
        "/redrive",
        data={
            "dlq_arn": "arn:aws:sqs:eu-west-2:111111111111:unknown-deadletter",
            "csrf_token": csrf_module.CSRF_TOKEN,
        },
    )
    assert response.status_code == 404
