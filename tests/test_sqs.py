import base64
import importlib
import json
import os
import time
from functools import lru_cache

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from fastapi.testclient import TestClient
from moto import mock_aws


@pytest.fixture(autouse=True)
def _no_endpoint_override(monkeypatch):
    """A custom endpoint (e.g. LocalStack) bypasses moto, so tests would hit a real, stateful SQS."""
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_SQS", raising=False)


@pytest.fixture(autouse=True)
def _audit_log(monkeypatch, tmp_path):
    """Each test writes its audit trail to its own file, which pytest cleans up."""
    monkeypatch.setenv("AUDIT_LOG_PATH", str(tmp_path / "audit.log"))


@pytest.fixture(autouse=True)
def move_tasks(monkeypatch):
    """moto doesn't implement ListMessageMoveTasks, so the app's client answers from this list."""
    sqs_routes = importlib.import_module("app.sqs.queues")
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
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    dlq = sqs.create_queue(QueueName=f"{name}-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    mappings = json.dumps(
        [{"name": name, "arn": source_arn, "deadletter_queue_arn": dlq_arn}]
    )
    return mappings, dlq, dlq_arn


def _load_module(
    monkeypatch,
    show_message_content: str = "false",
    token: str | None = None,
    allow_purge: str = "false",
):
    if token is None:
        monkeypatch.delenv("TOKEN", raising=False)
    else:
        monkeypatch.setenv("TOKEN", token)
    monkeypatch.setenv("SHOW_MESSAGE_CONTENT", show_message_content)
    monkeypatch.setenv("ALLOW_PURGE", allow_purge)
    monkeypatch.setenv("SERVICE", "demo-service")
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("USER_ID", "user-1")
    monkeypatch.setenv("USER_NAME", "User One")
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    if "app.main" in importlib.sys.modules:
        del importlib.sys.modules["app.main"]
    return importlib.import_module("app.main")


@mock_aws
def test_index_and_redrive(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="orders")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    dlq = sqs.create_queue(QueueName="orders-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    sqs.send_message(QueueUrl=dlq, MessageBody="hello")

    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps(
            [
                {
                    "name": "orders",
                    "arn": source_arn,
                    "url": source,
                    "deadletter_queue_arn": dlq_arn,
                }
            ]
        ),
    )
    module = _load_module(monkeypatch)
    client = TestClient(module.app)

    home = client.get("/")
    assert home.status_code == 200
    assert "orders-deadletter" in home.text

    sqs_routes = importlib.import_module("app.sqs.queues")
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
    assert response.headers["location"].startswith("/?notice=")


@mock_aws
def test_messages_hidden_without_flag(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="invoices")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    dlq = sqs.create_queue(QueueName="invoices-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps(
            [
                {
                    "name": "invoices",
                    "arn": source_arn,
                    "url": source,
                    "deadletter_queue_arn": dlq_arn,
                }
            ]
        ),
    )
    module = _load_module(monkeypatch, show_message_content="false")
    client = TestClient(module.app)

    response = client.get(f"/messages?dlq_arn={dlq_arn}")
    assert response.status_code == 404


@mock_aws
def test_messages_visible_with_flag(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="reports")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    dlq = sqs.create_queue(QueueName="reports-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    sqs.send_message(QueueUrl=dlq, MessageBody="world")
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps(
            [
                {
                    "name": "reports",
                    "arn": source_arn,
                    "url": source,
                    "deadletter_queue_arn": dlq_arn,
                }
            ]
        ),
    )
    module = _load_module(monkeypatch, show_message_content="true")
    client = TestClient(module.app)

    response = client.get(f"/messages?dlq_arn={dlq_arn}")
    assert response.status_code == 200
    assert len(response.json()["messages"]) == 1


@mock_aws
def test_purge_hidden_without_flag(monkeypatch):
    mappings, dlq, dlq_arn = _create_queues("reports")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="world"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true", allow_purge="false")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")

    assert "Purge queue" not in client.get("/").text
    response = client.post(
        "/purge",
        data={
            "dlq_arn": dlq_arn,
            "confirm_name": "reports-deadletter",
            "csrf_token": csrf_module.CSRF_TOKEN,
        },
    )
    assert response.status_code == 404


@mock_aws
def test_purge_allowed_while_message_content_is_hidden(monkeypatch):
    mappings, dlq, _ = _create_queues("ledgers")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="world"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="false", allow_purge="true")

    page = TestClient(module.app).get("/").text

    assert "Purge queue" in page
    assert "Sample messages (JSON)" not in page
    assert "Message content is hidden." in page


@mock_aws
def test_purge_requires_matching_queue_name(monkeypatch):
    mappings, dlq, dlq_arn = _create_queues("orders")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="hello"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true", allow_purge="true")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")

    response = client.post(
        "/purge",
        data={
            "dlq_arn": dlq_arn,
            "confirm_name": "wrong-name",
            "csrf_token": csrf_module.CSRF_TOKEN,
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "notice=purge-name-mismatch" in response.headers["location"]
    assert (
        len(client.get("/messages", params={"dlq_arn": dlq_arn}).json()["messages"])
        == 1
    )


@mock_aws
def test_purge_queue_clears_messages_and_audits(monkeypatch):
    mappings, dlq, dlq_arn = _create_queues("payments")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="hello"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true", allow_purge="true")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")

    response = client.post(
        "/purge",
        data={
            "dlq_arn": dlq_arn,
            "confirm_name": "payments-deadletter",
            "csrf_token": csrf_module.CSRF_TOKEN,
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "notice=purge-requested" in response.headers["location"]

    page = client.get(response.headers["location"]).text
    assert "Purge requested for payments-deadletter." in page
    assert "Purge requested at " in page
    assert "Actions available again in about a minute." in page
    assert "Start redrive" not in page
    assert client.get("/messages", params={"dlq_arn": dlq_arn}).json()["messages"] == []

    audit_path = os.environ["AUDIT_LOG_PATH"]
    assert os.path.exists(audit_path)
    with open(audit_path, encoding="utf-8") as fh:
        audit = fh.read()
    assert '"action": "queue.purged"' in audit
    assert dlq_arn in audit


@mock_aws
def test_purge_audit_counts_in_flight_messages(monkeypatch):
    mappings, dlq, dlq_arn = _create_queues("invoices")
    sqs = boto3.client("sqs", region_name="eu-west-2")
    sqs.send_message(QueueUrl=dlq, MessageBody="visible")
    sqs.send_message(QueueUrl=dlq, MessageBody="in flight")
    sqs.receive_message(QueueUrl=dlq, MaxNumberOfMessages=1, VisibilityTimeout=60)
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true", allow_purge="true")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")

    client.post(
        "/purge",
        data={
            "dlq_arn": dlq_arn,
            "confirm_name": "invoices-deadletter",
            "csrf_token": csrf_module.CSRF_TOKEN,
        },
        follow_redirects=False,
    )

    with open(os.environ["AUDIT_LOG_PATH"], encoding="utf-8") as fh:
        purged = [
            entry
            for entry in map(json.loads, fh)
            if entry["event"]["action"] == "queue.purged"
        ]
    assert [entry["messages"]["count"] for entry in purged] == [2]


@mock_aws
def test_redrive_is_refused_while_purge_is_in_progress(monkeypatch):
    mappings, dlq, dlq_arn = _create_queues("refunds")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="hello"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true", allow_purge="true")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")
    sqs_queues = importlib.import_module("app.sqs.queues")
    started: list[dict] = []
    sqs_queues.get_sqs_client().start_message_move_task = (  # type: ignore[attr-defined]
        lambda **params: started.append(params) or {"TaskHandle": "task-1"}
    )

    client.post(
        "/purge",
        data={
            "dlq_arn": dlq_arn,
            "confirm_name": "refunds-deadletter",
            "csrf_token": csrf_module.CSRF_TOKEN,
        },
        follow_redirects=False,
    )
    response = client.post(
        "/redrive",
        data={"dlq_arn": dlq_arn, "csrf_token": csrf_module.CSRF_TOKEN},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert "notice=purge-in-progress" in response.headers["location"]
    assert started == []


@mock_aws
def test_purge_queue_in_progress_shows_friendly_notice(monkeypatch):
    mappings, dlq, dlq_arn = _create_queues("claims")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="hello"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true", allow_purge="true")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")
    sqs_routes = importlib.import_module("app.sqs.queues")

    with Stubber(sqs_routes.get_sqs_client()) as stubber:
        stubber.add_response(
            "get_queue_url", {"QueueUrl": dlq}, {"QueueName": "claims-deadletter"}
        )
        stubber.add_response(
            "get_queue_attributes",
            {"Attributes": {"ApproximateNumberOfMessages": "1"}},
            {
                "QueueUrl": dlq,
                "AttributeNames": [
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            },
        )
        stubber.add_response(
            "get_queue_url", {"QueueUrl": dlq}, {"QueueName": "claims-deadletter"}
        )
        stubber.add_client_error(
            "purge_queue",
            service_error_code="PurgeQueueInProgress",
            expected_params={"QueueUrl": dlq},
        )
        response = client.post(
            "/purge",
            data={
                "dlq_arn": dlq_arn,
                "confirm_name": "claims-deadletter",
                "csrf_token": csrf_module.CSRF_TOKEN,
            },
            follow_redirects=False,
        )
        stubber.assert_no_pending_responses()

    assert response.status_code == 303
    assert "notice=purge-in-progress" in response.headers["location"]
    page = client.get(response.headers["location"]).text
    assert (
        "was already requested in the last 60 seconds. Try again in a minute." in page
    )
    assert "Purge requested at " in page
    assert "Start redrive" not in page


@mock_aws
def test_purge_hidden_and_refused_while_redrive_running(monkeypatch, move_tasks):
    mappings, dlq, dlq_arn = _create_queues("shipments")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="stuck"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true", allow_purge="true")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")
    move_tasks.append({"TaskHandle": "handle-1", "Status": "RUNNING"})

    page = client.get("/").text
    assert "Purge queue" not in page
    response = client.post(
        "/purge",
        data={
            "dlq_arn": dlq_arn,
            "confirm_name": "shipments-deadletter",
            "csrf_token": csrf_module.CSRF_TOKEN,
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "notice=redrive-running" in response.headers["location"]


@mock_aws
def test_purge_requires_csrf(monkeypatch):
    mappings, dlq, dlq_arn = _create_queues("returns")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="stuck"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true", allow_purge="true")
    client = TestClient(module.app)

    response = client.post(
        "/purge", data={"dlq_arn": dlq_arn, "confirm_name": "returns-deadletter"}
    )
    assert response.status_code == 403


def test_long_queue_names_can_wrap(monkeypatch):
    monkeypatch.setenv("SQS_QUEUES", "[]")
    module = _load_module(monkeypatch)

    assert (
        module.wrappable("forms_events-deadletter")
        == "forms_<wbr>events-<wbr>deadletter"
    )
    assert module.wrappable("<b>_x") == "&lt;b&gt;_<wbr>x"


def test_stub_mode_ui_sample_and_redrive(monkeypatch):
    mappings = json.dumps(
        [
            {
                "name": "orders",
                "arn": "arn:aws:sqs:eu-west-2:000000000000:orders",
                "deadletter_queue_arn": "arn:aws:sqs:eu-west-2:000000000000:orders-deadletter",
            },
            {
                "name": "payments",
                "arn": "arn:aws:sqs:eu-west-2:000000000000:payments",
                "deadletter_queue_arn": "arn:aws:sqs:eu-west-2:000000000000:payments-deadletter",
            },
        ]
    )
    monkeypatch.setenv("SQS_STUB_SAMPLE_COUNT", "2")
    monkeypatch.setenv("SQS_QUEUES", mappings)

    _load_module(monkeypatch, show_message_content="true")
    # dev.main swaps the client factory on import, so re-import it after the fixture's patch.
    monkeypatch.delitem(importlib.sys.modules, "dev.main", raising=False)
    client = TestClient(importlib.import_module("dev.main").app)
    csrf_module = importlib.import_module("app.common.csrf")
    payments_dlq = "arn:aws:sqs:eu-west-2:000000000000:payments-deadletter"
    orders_dlq = "arn:aws:sqs:eu-west-2:000000000000:orders-deadletter"

    home = client.get("/")
    assert home.status_code == 200
    assert "orders-deadletter" in home.text
    assert "payments-deadletter" in home.text
    assert "Sample messages (JSON)" in home.text

    sample = client.get("/messages", params={"dlq_arn": orders_dlq})
    assert sample.status_code == 200
    assert len(sample.json()["messages"]) == 2
    assert json.loads(sample.json()["messages"][0]["Body"]).get("source") == "sqs-stub"

    redrive = client.post(
        "/redrive",
        data={"dlq_arn": payments_dlq, "csrf_token": csrf_module.CSRF_TOKEN},
        follow_redirects=False,
    )
    assert redrive.status_code == 303
    assert "notice=redrive-started" in redrive.headers["location"]
    stub = importlib.import_module("app.sqs.queues").get_sqs_client()
    task = stub.list_message_move_tasks(SourceArn=payments_dlq)["Results"][0]
    assert task["Status"] == "RUNNING"
    page = client.get(redrive.headers["location"]).text
    assert "Redrive started for payments-deadletter." in page
    assert "Cancel redrive" in page
    assert "Redrive started for" not in client.get("/").text
    assert client.get("/messages", params={"dlq_arn": payments_dlq}).status_code == 409

    # The stub moves one message a second by default, so two messages are done after two.
    started = task["StartedTimestamp"] / 1000
    stub._now = lambda: started + 2
    task = stub.list_message_move_tasks(SourceArn=payments_dlq)["Results"][0]
    assert task["Status"] == "COMPLETED"
    assert task["ApproximateNumberOfMessagesMoved"] == 2

    moved_messages = client.get("/messages", params={"dlq_arn": payments_dlq})
    assert moved_messages.status_code == 200
    assert moved_messages.json()["messages"] == []

    # Cancelling part-way leaves the messages not yet moved on the DLQ.
    client.post(
        "/redrive", data={"dlq_arn": orders_dlq, "csrf_token": csrf_module.CSRF_TOKEN}
    )
    task = stub.list_message_move_tasks(SourceArn=orders_dlq)["Results"][0]
    stub._now = lambda: task["StartedTimestamp"] / 1000 + 1
    cancelled = client.post(
        "/cancel",
        data={
            "dlq_arn": orders_dlq,
            "task_handle": task["TaskHandle"],
            "csrf_token": csrf_module.CSRF_TOKEN,
        },
        follow_redirects=False,
    )
    assert cancelled.status_code == 303
    task = stub.list_message_move_tasks(SourceArn=orders_dlq)["Results"][0]
    assert task["Status"] == "CANCELLED"
    assert task["ApproximateNumberOfMessagesMoved"] == 1
    # Step past the visibility timeout left by the earlier sample.
    stub._now = lambda: started + 60
    remaining = client.get("/messages", params={"dlq_arn": orders_dlq})
    assert len(remaining.json()["messages"]) == 1


@mock_aws
def test_routes_served_under_token_prefix(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="payments")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    dlq = sqs.create_queue(QueueName="payments-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    sqs.send_message(QueueUrl=dlq, MessageBody="failed payment")
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps(
            [
                {
                    "name": "payments",
                    "arn": source_arn,
                    "url": source,
                    "deadletter_queue_arn": dlq_arn,
                }
            ]
        ),
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
    assert (
        client.get("/tok123/static/govuk/fonts/bold-b542beb274-v2.woff2").status_code
        == 200
    )

    assert client.get("/").status_code == 404
    assert client.get("/health").status_code == 200
    assert client.get("/tok123/health").status_code == 200

    sqs_routes = importlib.import_module("app.sqs.queues")
    real_client = sqs_routes.get_sqs_client()
    real_client.start_message_move_task = lambda **_: {"TaskHandle": "task-1"}  # type: ignore[attr-defined]
    monkeypatch.setattr(sqs_routes, "get_sqs_client", lambda: real_client)
    response = client.post(
        "/tok123/redrive",
        data={"dlq_arn": dlq_arn, "csrf_token": csrf_module.CSRF_TOKEN},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("/tok123/?notice=redrive-started")


@mock_aws
def test_redrive_rate_capped_to_aws_limit(monkeypatch):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    source = sqs.create_queue(QueueName="refunds")["QueueUrl"]
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    dlq = sqs.create_queue(QueueName="refunds-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps(
            [
                {
                    "name": "refunds",
                    "arn": source_arn,
                    "url": source,
                    "deadletter_queue_arn": dlq_arn,
                }
            ]
        ),
    )
    _load_module(monkeypatch)
    client = TestClient(importlib.import_module("app.main").app)
    csrf_module = importlib.import_module("app.common.csrf")
    sqs_routes = importlib.import_module("app.sqs.queues")

    calls = []
    real_client = sqs_routes.get_sqs_client()
    real_client.start_message_move_task = lambda **kwargs: (
        calls.append(kwargs) or {"TaskHandle": "t"}
    )  # type: ignore[attr-defined]
    monkeypatch.setattr(sqs_routes, "get_sqs_client", lambda: real_client)

    def post(rate: str):
        return client.post(
            "/redrive",
            data={
                "dlq_arn": dlq_arn,
                "max_messages_per_second": rate,
                "csrf_token": csrf_module.CSRF_TOKEN,
            },
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
    source_arn = sqs.get_queue_attributes(QueueUrl=source, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    dlq = sqs.create_queue(QueueName="claims-deadletter")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    sqs.send_message(QueueUrl=dlq, MessageBody="failed claim")
    monkeypatch.setenv(
        "SQS_QUEUES",
        json.dumps(
            [
                {
                    "name": "claims",
                    "arn": source_arn,
                    "url": source,
                    "deadletter_queue_arn": dlq_arn,
                }
            ]
        ),
    )
    _load_module(monkeypatch)
    client = TestClient(importlib.import_module("app.main").app)
    csrf_module = importlib.import_module("app.common.csrf")
    sqs_routes = importlib.import_module("app.sqs.queues")

    real_client = sqs_routes.get_sqs_client()
    real_client.start_message_move_task = lambda **_: {"TaskHandle": "t"}  # type: ignore[attr-defined]
    monkeypatch.setattr(sqs_routes, "get_sqs_client", lambda: real_client)

    assert f'name="csrf_token" value="{csrf_module.CSRF_TOKEN}"' in client.get("/").text

    proxied = {
        "origin": "http://cdp.127.0.0.1.sslip.io:8000",
        "sec-fetch-site": "same-origin",
    }
    accepted = client.post(
        "/redrive",
        data={"dlq_arn": dlq_arn, "csrf_token": csrf_module.CSRF_TOKEN},
        headers=proxied,
        follow_redirects=False,
    )
    assert accepted.status_code == 303

    assert (
        client.post("/redrive", data={"dlq_arn": dlq_arn}, headers=proxied).status_code
        == 403
    )
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

    response = client.post(
        "/redrive", data={"dlq_arn": unknown_arn, "csrf_token": csrf_module.CSRF_TOKEN}
    )
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
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="stuck"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true")
    client = TestClient(module.app)
    csrf_module = importlib.import_module("app.common.csrf")
    sqs_routes = importlib.import_module("app.sqs.queues")
    move_tasks.append({"TaskHandle": "handle-1", "Status": "RUNNING"})

    page = client.get("/")
    assert "Cancel redrive" in page.text
    assert "Start redrive" not in page.text
    assert "Sample messages" not in page.text

    # The stubber checks the call against the real API model, which moto can't do for this operation.
    with Stubber(sqs_routes.get_sqs_client()) as stubber:
        stubber.add_response("cancel_message_move_task", {}, {"TaskHandle": "handle-1"})
        response = client.post(
            "/cancel",
            data={
                "dlq_arn": dlq_arn,
                "task_handle": "handle-1",
                "csrf_token": csrf_module.CSRF_TOKEN,
            },
            follow_redirects=False,
        )
        stubber.assert_no_pending_responses()
    assert response.status_code == 303
    assert response.headers["location"].startswith("/?notice=")

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
def test_running_redrive_asks_user_to_refresh(monkeypatch, move_tasks):
    mappings, dlq, _ = _create_queues("returns")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="stuck"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch)
    client = TestClient(module.app)
    refresh_hint = "Refresh the page to see progress."

    idle = client.get("/").text
    assert refresh_hint not in idle
    assert 'data-confirm="Move ' in idle

    move_tasks.append({"TaskHandle": "handle-1", "Status": "RUNNING"})
    assert refresh_hint in client.get("/").text

    move_tasks[0]["Status"] = "COMPLETED"
    assert refresh_hint not in client.get("/").text


@mock_aws
def test_count_lag_hint_only_shortly_after_redrive(monkeypatch, move_tasks):
    mappings, dlq, _ = _create_queues("returns")
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="stuck"
    )
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch)
    client = TestClient(module.app)
    lag_hint = "May not have caught up yet. Refresh in a minute."
    now_ms = int(time.time() * 1000)

    move_tasks.append(
        {"TaskHandle": "h", "Status": "COMPLETED", "StartedTimestamp": now_ms - 60_000}
    )
    recent = client.get("/").text
    assert lag_hint in recent
    assert "Started " in recent

    move_tasks[0]["StartedTimestamp"] = now_ms - 3 * 24 * 60 * 60 * 1000
    days_later = client.get("/").text
    assert lag_hint not in days_later
    assert "Started " in days_later

    move_tasks[0]["StartedTimestamp"] = 1_760_000_000_000
    assert "Started 9 Oct 2025 08:53 UTC" in client.get("/").text


@mock_aws
def test_aws_error_returns_502(monkeypatch):
    mappings, _, dlq_arn = _create_queues("returns")
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true")
    client = TestClient(module.app)

    def throttled(**_):
        raise ClientError(
            {"Error": {"Code": "ThrottlingException"}}, "ListMessageMoveTasks"
        )

    sqs_routes = importlib.import_module("app.sqs.queues")
    sqs_routes.get_sqs_client().list_message_move_tasks = throttled

    assert client.get("/messages", params={"dlq_arn": dlq_arn}).status_code == 502


@mock_aws
def test_messages_deduplicated_by_message_id(monkeypatch):
    mappings, dlq, dlq_arn = _create_queues("refunds")
    monkeypatch.setenv("SQS_QUEUES", mappings)
    module = _load_module(monkeypatch, show_message_content="true")
    client = TestClient(module.app)
    sqs_routes = importlib.import_module("app.sqs.queues")
    message = {"MessageId": "msg-1", "ReceiptHandle": "rh", "Body": "once"}

    with Stubber(sqs_routes.get_sqs_client()) as stubber:
        stubber.add_response(
            "get_queue_url", {"QueueUrl": dlq}, {"QueueName": "refunds-deadletter"}
        )
        stubber.add_response(
            "receive_message",
            {"Messages": [{**message, "ReceiptHandle": f"rh-{i}"} for i in range(3)]},
            {
                "QueueUrl": dlq,
                "MaxNumberOfMessages": 10,
                "VisibilityTimeout": 3,
                "WaitTimeSeconds": 2,
                "MessageSystemAttributeNames": ["SentTimestamp"],
                "MessageAttributeNames": ["All"],
            },
        )
        response = client.get("/messages", params={"dlq_arn": dlq_arn})

    assert response.status_code == 200
    assert [m["MessageId"] for m in response.json()["messages"]] == ["msg-1"]


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
    boto3.client("sqs", region_name="eu-west-2").send_message(
        QueueUrl=dlq, MessageBody="hello"
    )
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
