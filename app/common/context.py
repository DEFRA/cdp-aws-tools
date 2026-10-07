import os
from pathlib import Path


def _to_bool(value: str | None) -> bool:
    return (value or "").strip().lower() == "true"


def load_app_context() -> dict[str, object]:
    return {
        "show_message_content": _to_bool(os.getenv("SHOW_MESSAGE_CONTENT")),
        "service": os.getenv("SERVICE", ""),
        "environment": os.getenv("ENVIRONMENT", ""),
        "user_id": os.getenv("USER_ID", ""),
        "user_name": os.getenv("USER_NAME", ""),
        "audit_path": Path(os.getenv("AUDIT_LOG_PATH", "/var/log/webshell/sqs.audit")),
    }
