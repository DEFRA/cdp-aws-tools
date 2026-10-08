import secrets

from fastapi import HTTPException, Request, status


# webshell-proxy rewrites Host to the task address, so Origin can't be compared with Host.
# Forms carry this per-process token instead.
CSRF_TOKEN = secrets.token_urlsafe(32)


def csrf_or_403(request: Request, csrf_token: str) -> None:
    if request.headers.get("sec-fetch-site") == "cross-site":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cross-site request")
    # Compared as bytes: compare_digest raises TypeError on non-ASCII str.
    if not secrets.compare_digest(csrf_token.encode(), CSRF_TOKEN.encode()):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid CSRF token")
