from urllib.parse import urlencode

from fastapi.responses import RedirectResponse

NOTICES = {
    "redrive-started": ("success", "Redrive started for {queue}."),
    "redrive-cancelled": ("success", "Redrive cancelled for {queue}."),
    "purge-requested": (
        "success",
        "Purge requested for {queue}. Messages can take up to a minute to disappear.",
    ),
    "purge-name-mismatch": (
        "error",
        "Queue name did not match {queue}. No messages were purged.",
    ),
    "redrive-running": ("error", "Cannot purge {queue} while a redrive is running."),
    "purge-in-progress": (
        "error",
        "A purge of {queue} was already requested in the last 60 seconds. Try again in a minute.",
    ),
}


def banner_for(code: str | None, queue: str | None) -> dict[str, str] | None:
    notice = NOTICES.get(code or "")
    if notice is None or queue is None:
        return None
    banner_type, text = notice
    return {"type": banner_type, "text": text.format(queue=queue)}


def redirect_with_notice(base_path: str, dlq_arn: str, notice: str) -> RedirectResponse:
    query = urlencode({"notice": notice, "dlq_arn": dlq_arn})
    return RedirectResponse(f"{base_path}/?{query}", status_code=303)
