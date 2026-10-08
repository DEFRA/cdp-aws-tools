import os
from pathlib import Path

from botocore.exceptions import ClientError
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.common import configure_log_level, load_app_context, write_audit
from app.sqs import create_router

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
configure_log_level(os.getenv("LOG_LEVEL", "INFO"))
app_context = load_app_context()

# webshell-proxy forwards /{TOKEN}/... unchanged, so routes live under that prefix.
BASE_PATH = f"/{os.getenv('TOKEN', '').strip('/')}".rstrip("/")

app = FastAPI(title="cdp-aws-tools")
router, index, health = create_router(
    app_context=app_context,
    base_path=BASE_PATH,
    templates=templates,
)
app.include_router(router, prefix=BASE_PATH)

app.mount(
    f"{BASE_PATH}/static",
    StaticFiles(directory=str(Path(__file__).parent / "static")),
    name="static",
)
if BASE_PATH:
    # The portal iframe opens /{TOKEN} without a trailing slash.
    app.add_api_route(BASE_PATH, index, methods=["GET"], include_in_schema=False)
    app.add_api_route("/health", health, methods=["GET"], include_in_schema=False)


@app.exception_handler(ClientError)
def handle_client_error(_: Request, exc: ClientError):
    write_audit(
        app_context=app_context,
        event="aws.error",
        outcome="failure",
        extra={"error": {"message": str(exc)}},
    )
    return JSONResponse(
        status_code=status.HTTP_502_BAD_GATEWAY,
        content={"message": "AWS request failed", "detail": str(exc)},
    )
