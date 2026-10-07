import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import ecs_logging

logger = logging.getLogger("cdp-aws-tools")
handler = logging.StreamHandler()
handler.setFormatter(ecs_logging.StdlibFormatter())
logger.addHandler(handler)
logger.setLevel("INFO")
logger.propagate = False


def configure_log_level(level: str) -> None:
    logger.setLevel(level.upper())


def write_audit(
    app_context: dict[str, Any],
    event: str,
    outcome: str,
    extra: dict[str, Any] | None = None,
    tool: str = "sqs_tool",
) -> None:
    payload: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event": {"action": event, "outcome": outcome},
        "user": {"id": app_context["user_id"], "name": app_context["user_name"]},
        "service": {"name": app_context["service"]},
        "labels": {"environment": app_context["environment"], "tool": tool},
    }
    if extra:
        payload.update(extra)
    logger.info("audit_event %s", json.dumps(payload))

    audit_path = Path(app_context["audit_path"])
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload))
        fh.write("\n")
