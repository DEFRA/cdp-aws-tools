import base64
import json
import time
from urllib.parse import parse_qs, urlsplit

import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from app.common import CSRF_TOKEN
from app.sqs import queues, routes


def notice_of(response) -> str:
    """The notice code a form post redirected to."""
    assert response.status_code == 303
    return parse_qs(urlsplit(response.headers["location"]).query)["notice"][0]


def audited(audit_log, action: str) -> list[dict]:
    with audit_log.open(encoding="utf-8") as fh:
        return [e for e in map(json.loads, fh) if e["event"]["action"] == action]


def advance_sample_clock(monkeypatch, seconds: float) -> None:
    real_monotonic = routes.monotonic
    monkeypatch.setattr(routes, "monotonic", lambda: real_monotonic() + seconds)


def test_index_and_redrive(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="hello")
    client = make_client()

    home = client.get("/")
    assert home.status_code == 200
    assert dlq.name in home.text

    response = client.redrive(dlq.arn)
    assert notice_of(response) == "redrive-started"
    assert response.headers["location"].startswith("/?notice=")


def test_messages_hidden_without_flag(dlq, make_client):
    client = make_client(show_message_content=False)

    assert client.sample(dlq.arn).status_code == 404


def test_messages_visible_with_flag(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="world")
    client = make_client(show_message_content=True)

    response = client.sample(dlq.arn)
    assert response.status_code == 200
    assert len(response.json()["messages"]) == 1


def test_messages_output_indented(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="hello")
    client = make_client(show_message_content=True)

    response = client.sample(dlq.arn)
    assert response.status_code == 200
    assert response.text.startswith('{\n  "messages": [')


@pytest.mark.parametrize(
    "body",
    [
        "plain text",
        '{"amount": 10.50, "big": 19.999999999999999999, "id": 1E3}',
        '{"s": "NEW", "s": "PAID"}',
        '{"x": "\\ud800"}',
        '{"a": NaN}',
    ],
)
def test_messages_body_is_original_text(sqs, dlq, make_client, body):
    sqs.send_message(QueueUrl=dlq.url, MessageBody=body)
    client = make_client(show_message_content=True)

    assert client.sample(dlq.arn).json()["messages"][0]["Body"] == body


def test_purge_hidden_without_flag(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="world")
    client = make_client(show_message_content=True, allow_purge=False)

    assert "Purge queue" not in client.get("/").text
    assert client.purge(dlq.arn, dlq.name).status_code == 404


def test_purge_allowed_while_message_content_is_hidden(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="world")
    client = make_client(show_message_content=False, allow_purge=True)

    page = client.get("/").text

    assert "Purge queue" in page
    assert "Sample messages (JSON)" not in page
    assert "Message content is hidden." in page


def test_purge_requires_matching_queue_name(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="hello")
    client = make_client(show_message_content=True, allow_purge=True)

    response = client.purge(dlq.arn, "wrong-name")

    assert notice_of(response) == "purge-name-mismatch"
    assert len(client.sample(dlq.arn).json()["messages"]) == 1


def test_purge_queue_clears_messages_and_audits(sqs, dlq, make_client, audit_log):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="hello")
    client = make_client(show_message_content=True, allow_purge=True)

    response = client.purge(dlq.arn, dlq.name)
    assert notice_of(response) == "purge-requested"

    page = client.get(response.headers["location"]).text
    assert f"Purge requested for {dlq.name}." in page
    assert "Purge requested at " in page
    assert "Actions available again in about a minute." in page
    assert "Start redrive" not in page
    assert client.sample(dlq.arn).json()["messages"] == []

    purged = audited(audit_log, "queue.purged")
    assert [entry["sqs"]["source_dlq_arn"] for entry in purged] == [dlq.arn]


def test_purge_audit_counts_in_flight_messages(sqs, dlq, make_client, audit_log):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="visible")
    sqs.send_message(QueueUrl=dlq.url, MessageBody="in flight")
    sqs.receive_message(QueueUrl=dlq.url, MaxNumberOfMessages=1, VisibilityTimeout=60)
    client = make_client(show_message_content=True, allow_purge=True)

    client.purge(dlq.arn, dlq.name)

    purged = audited(audit_log, "queue.purged")
    assert [entry["messages"]["count"] for entry in purged] == [2]


def test_redrive_is_refused_while_purge_is_in_progress(
    sqs, dlq, make_client, redrive_calls
):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="hello")
    client = make_client(show_message_content=True, allow_purge=True)

    client.purge(dlq.arn, dlq.name)
    response = client.redrive(dlq.arn)

    assert notice_of(response) == "purge-in-progress"
    assert redrive_calls == []


def test_purge_queue_in_progress_shows_friendly_notice(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="hello")
    client = make_client(show_message_content=True, allow_purge=True)
    queue_url = ("get_queue_url", {"QueueUrl": dlq.url}, {"QueueName": dlq.name})

    with Stubber(queues.get_sqs_client()) as stubber:
        stubber.add_response(*queue_url)
        stubber.add_response(
            "get_queue_attributes",
            {"Attributes": {"ApproximateNumberOfMessages": "1"}},
            {
                "QueueUrl": dlq.url,
                "AttributeNames": [
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed",
                ],
            },
        )
        stubber.add_response(*queue_url)
        stubber.add_client_error(
            "purge_queue",
            service_error_code="PurgeQueueInProgress",
            expected_params={"QueueUrl": dlq.url},
        )
        response = client.purge(dlq.arn, dlq.name)
        stubber.assert_no_pending_responses()

    assert notice_of(response) == "purge-in-progress"
    page = client.get(response.headers["location"]).text
    assert (
        "was already requested in the last 60 seconds. Try again in a minute." in page
    )
    assert "Purge requested at " in page
    assert "Start redrive" not in page


def test_purge_hidden_and_refused_while_redrive_running(
    sqs, dlq, make_client, move_tasks
):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="stuck")
    client = make_client(show_message_content=True, allow_purge=True)
    move_tasks.append({"TaskHandle": "handle-1", "Status": "RUNNING"})

    assert "Purge queue" not in client.get("/").text
    assert notice_of(client.purge(dlq.arn, dlq.name)) == "redrive-running"


def test_purge_requires_csrf(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="stuck")
    client = make_client(show_message_content=True, allow_purge=True)

    response = client.post(
        "/purge", data={"dlq_arn": dlq.arn, "confirm_name": dlq.name}
    )
    assert response.status_code == 403


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("forms_events-deadletter", "forms_<wbr>events-<wbr>deadletter"),
        ("<b>_x", "&lt;b&gt;_<wbr>x"),
    ],
)
def test_long_queue_names_can_wrap(load_app, name, expected):
    assert load_app().wrappable(name) == expected


def test_stub_mode_ui_sample_and_redrive(monkeypatch, make_client):
    orders_dlq = "arn:aws:sqs:eu-west-2:000000000000:orders-deadletter"
    payments_dlq = "arn:aws:sqs:eu-west-2:000000000000:payments-deadletter"
    mappings = [
        {
            "name": name,
            "arn": f"arn:aws:sqs:eu-west-2:000000000000:{name}",
            "deadletter_queue_arn": dlq_arn,
        }
        for name, dlq_arn in (("orders", orders_dlq), ("payments", payments_dlq))
    ]
    monkeypatch.setenv("SQS_STUB_SAMPLE_COUNT", "2")
    monkeypatch.setenv("SQS_QUEUES", json.dumps(mappings))

    # dev.main swaps the client factory on import, replacing the moto-backed one.
    client = make_client(show_message_content=True, module="dev.main")

    home = client.get("/")
    assert home.status_code == 200
    assert "orders-deadletter" in home.text
    assert "payments-deadletter" in home.text
    assert "Sample messages (JSON)" in home.text

    sample = client.sample(orders_dlq)
    assert sample.status_code == 200
    assert len(sample.json()["messages"]) == 2
    assert json.loads(sample.json()["messages"][0]["Body"]).get("source") == "sqs-stub"

    redrive = client.redrive(payments_dlq)
    assert notice_of(redrive) == "redrive-started"
    stub = queues.get_sqs_client()
    task = stub.list_message_move_tasks(SourceArn=payments_dlq)["Results"][0]
    assert task["Status"] == "RUNNING"
    page = client.get(redrive.headers["location"]).text
    assert "Redrive started for payments-deadletter." in page
    assert "Cancel redrive" in page
    assert "Redrive started for" not in client.get("/").text
    assert client.sample(payments_dlq).status_code == 409

    # The stub moves one message a second by default, so two messages are done after two.
    started = task["StartedTimestamp"] / 1000
    stub._now = lambda: started + 2
    task = stub.list_message_move_tasks(SourceArn=payments_dlq)["Results"][0]
    assert task["Status"] == "COMPLETED"
    assert task["ApproximateNumberOfMessagesMoved"] == 2

    moved_messages = client.sample(payments_dlq)
    assert moved_messages.status_code == 200
    assert moved_messages.json()["messages"] == []

    # Step past the hide left by the earlier sample, which blocks redrive until it ends.
    advance_sample_clock(monkeypatch, 60)
    stub._now = lambda: started + 60

    # Cancelling part-way leaves the messages not yet moved on the DLQ.
    client.redrive(orders_dlq)
    task = stub.list_message_move_tasks(SourceArn=orders_dlq)["Results"][0]
    stub._now = lambda: task["StartedTimestamp"] / 1000 + 1
    cancelled = client.cancel(orders_dlq, task["TaskHandle"])
    assert cancelled.status_code == 303
    task = stub.list_message_move_tasks(SourceArn=orders_dlq)["Results"][0]
    assert task["Status"] == "CANCELLED"
    assert task["ApproximateNumberOfMessagesMoved"] == 1
    assert len(client.sample(orders_dlq).json()["messages"]) == 1


def test_routes_served_under_token_prefix(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="failed payment")
    client = make_client(show_message_content=True, token="tok123")

    for path in ("/tok123", "/tok123/"):
        page = client.get(path)
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

    response = client.post_form("/tok123/redrive", dlq_arn=dlq.arn)
    assert response.status_code == 303
    assert response.headers["location"].startswith("/tok123/?notice=redrive-started")


@pytest.mark.parametrize("rate", ["0", "501"])
def test_redrive_rate_outside_aws_limit_is_rejected(
    dlq, make_client, redrive_calls, rate
):
    response = make_client().redrive(dlq.arn, max_messages_per_second=rate)

    assert response.status_code == 422
    assert redrive_calls == []


@pytest.mark.parametrize(
    ("rate", "expected"),
    [("500", {"MaxNumberOfMessagesPerSecond": 500}), ("", {})],
    ids=["aws-limit", "unset"],
)
def test_redrive_rate_passed_to_aws_only_when_set(
    dlq, make_client, redrive_calls, rate, expected
):
    response = make_client().redrive(dlq.arn, max_messages_per_second=rate)

    assert response.status_code == 303
    assert redrive_calls == [
        {"SourceArn": dlq.arn, "DestinationArn": dlq.mapping["arn"], **expected}
    ]


def test_redrive_refused_immediately_after_sampling_same_dlq(
    sqs, dlq, make_client, redrive_calls
):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="failed claim")
    client = make_client(show_message_content=True)

    sample = client.sample(dlq.arn)
    assert sample.status_code == 200
    assert sample.json()["messages"]

    redrive = client.redrive(dlq.arn)
    assert notice_of(redrive) == "messages-hidden"
    assert redrive_calls == []
    assert (
        f"Some messages in {dlq.name} are hidden, for example by a recent sample. "
        "Try the redrive again in a moment."
    ) in client.get(redrive.headers["location"]).text


def test_redrive_refused_while_sample_poll_is_running(
    monkeypatch, sqs, dlq, make_client
):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="failed claim")
    client = make_client(show_message_content=True)
    redrives_during_poll = []
    real_sample_messages = queues.sample_messages

    def sample_with_concurrent_redrive(sqs_client, mapping):
        redrives_during_poll.append(client.redrive(dlq.arn))
        return real_sample_messages(sqs_client, mapping)

    monkeypatch.setattr(queues, "sample_messages", sample_with_concurrent_redrive)

    assert client.sample(dlq.arn).status_code == 200
    assert [notice_of(r) for r in redrives_during_poll] == ["messages-hidden"]


def test_redrive_refused_while_dlq_has_messages_hidden_elsewhere(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="failed claim")
    # Another consumer, e.g. the AWS console, holds the message.
    sqs.receive_message(QueueUrl=dlq.url, VisibilityTimeout=30)
    client = make_client()

    assert notice_of(client.redrive(dlq.arn)) == "messages-hidden"


def test_redrive_allowed_after_sampling_window(
    monkeypatch, sqs, dlq, make_client, redrive_calls
):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="failed claim")
    client = make_client(show_message_content=True)

    sample = client.sample(dlq.arn)
    assert sample.status_code == 200
    assert sample.json()["messages"]

    advance_sample_clock(monkeypatch, queues.SAMPLE_HIDDEN_SECONDS + 1)
    # moto keeps the message hidden in real time, so only the tool's own window is under test here.
    monkeypatch.setattr(queues, "hidden_message_count", lambda *_: 0)

    assert notice_of(client.redrive(dlq.arn)) == "redrive-started"
    assert len(redrive_calls) == 1


def test_redrive_requires_csrf_token_not_matching_host(sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="failed claim")
    client = make_client()

    assert f'name="csrf_token" value="{CSRF_TOKEN}"' in client.get("/").text

    proxied = {
        "origin": "http://cdp.127.0.0.1.sslip.io:8000",
        "sec-fetch-site": "same-origin",
    }
    accepted = client.post_form("/redrive", headers=proxied, dlq_arn=dlq.arn)
    assert accepted.status_code == 303

    missing_token = client.post("/redrive", data={"dlq_arn": dlq.arn}, headers=proxied)
    assert missing_token.status_code == 403

    cross_site = client.post_form(
        "/redrive", headers={"sec-fetch-site": "cross-site"}, dlq_arn=dlq.arn
    )
    assert cross_site.status_code == 403


def test_reject_unknown_dlq(dlq, make_client):
    client = make_client(show_message_content=True)
    unknown_arn = "arn:aws:sqs:eu-west-2:111111111111:unknown-deadletter"

    for response in (client.redrive(unknown_arn), client.sample(unknown_arn)):
        assert response.status_code == 404
        assert response.json()["detail"] == "Unknown DLQ"


def test_non_ascii_csrf_token_is_forbidden(dlq, make_client):
    response = make_client().redrive(dlq.arn, csrf_token="é")

    assert response.status_code == 403


def test_cancel_running_redrive(sqs, dlq, make_client, move_tasks):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="stuck")
    client = make_client(show_message_content=True)
    move_tasks.append({"TaskHandle": "handle-1", "Status": "RUNNING"})

    page = client.get("/")
    assert "Cancel redrive" in page.text
    assert "Start redrive" not in page.text
    assert "Sample messages" not in page.text

    # The stubber checks the call against the real API model, which moto can't do for this operation.
    with Stubber(queues.get_sqs_client()) as stubber:
        stubber.add_response("cancel_message_move_task", {}, {"TaskHandle": "handle-1"})
        response = client.cancel(dlq.arn, "handle-1")
        stubber.assert_no_pending_responses()
    assert notice_of(response) == "redrive-cancelled"

    move_tasks[0]["Status"] = "CANCELLING"
    assert client.sample(dlq.arn).status_code == 409


def test_messages_with_binary_attribute(sqs, dlq, make_client):
    sqs.send_message(
        QueueUrl=dlq.url,
        MessageBody="payload",
        MessageAttributes={"blob": {"DataType": "Binary", "BinaryValue": b"\xff\x00"}},
    )
    client = make_client(show_message_content=True)

    response = client.sample(dlq.arn)
    assert response.status_code == 200
    attribute = response.json()["messages"][0]["MessageAttributes"]["blob"]
    assert attribute["BinaryValue"] == base64.b64encode(b"\xff\x00").decode()


def test_running_redrive_asks_user_to_refresh(sqs, dlq, make_client, move_tasks):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="stuck")
    client = make_client()
    refresh_hint = "Refresh the page to see progress."

    idle = client.get("/").text
    assert refresh_hint not in idle
    assert 'data-confirm="Move ' in idle

    move_tasks.append({"TaskHandle": "handle-1", "Status": "RUNNING"})
    assert refresh_hint in client.get("/").text

    move_tasks[0]["Status"] = "COMPLETED"
    assert refresh_hint not in client.get("/").text


def test_count_lag_hint_only_shortly_after_redrive(sqs, dlq, make_client, move_tasks):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="stuck")
    client = make_client()
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


def test_aws_error_returns_502(dlq, make_client):
    client = make_client(show_message_content=True)

    def throttled(**_):
        raise ClientError(
            {"Error": {"Code": "ThrottlingException"}}, "ListMessageMoveTasks"
        )

    queues.get_sqs_client().list_message_move_tasks = throttled

    assert client.sample(dlq.arn).status_code == 502


def test_messages_deduplicated_by_message_id(dlq, make_client):
    client = make_client(show_message_content=True)
    message = {"MessageId": "msg-1", "Body": "once"}

    with Stubber(queues.get_sqs_client()) as stubber:
        stubber.add_response(
            "get_queue_url", {"QueueUrl": dlq.url}, {"QueueName": dlq.name}
        )
        stubber.add_response(
            "receive_message",
            {"Messages": [{**message, "ReceiptHandle": f"rh-{i}"} for i in range(3)]},
            {
                "QueueUrl": dlq.url,
                "MaxNumberOfMessages": 10,
                "VisibilityTimeout": 3,
                "WaitTimeSeconds": 2,
                "MessageSystemAttributeNames": ["SentTimestamp"],
                "MessageAttributeNames": ["All"],
            },
        )
        response = client.sample(dlq.arn)

    assert response.status_code == 200
    assert [m["MessageId"] for m in response.json()["messages"]] == ["msg-1"]


def test_no_queues_shows_empty_state(make_client):
    client = make_client(token="tok123")

    for path in ("/tok123", "/tok123/"):
        page = client.get(path)
        assert page.status_code == 200
        assert "This service has no queues with a dead letter queue in dev" in page.text
    assert client.get("/health").status_code == 200
    assert client.get("/tok123/health").status_code == 200


def test_failing_queue_does_not_hide_the_others(monkeypatch, sqs, dlq, make_client):
    sqs.send_message(QueueUrl=dlq.url, MessageBody="hello")
    broken = {
        "name": "broken",
        "arn": "arn:aws:sqs:eu-west-2:123456789012:broken",
        "deadletter_queue_arn": "arn:aws:sqs:eu-west-2:123456789012:broken-deadletter",
    }
    monkeypatch.setenv("SQS_QUEUES", json.dumps([dlq.mapping, broken]))
    client = make_client()

    page = client.get("/")
    assert page.status_code == 200
    assert "Could not load this queue" in page.text
    assert page.text.count("Start redrive") == 1
