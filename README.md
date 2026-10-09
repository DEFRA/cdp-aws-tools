# cdp-aws-tools

Browser UI for SQS operations launched through the CDP webshell flow.

## Features

- Lists service DLQs with queue depth and redrive status.
- Starts and cancels native SQS redrives.
- Optionally shows message content when `SHOW_MESSAGE_CONTENT=true`.
- Logs user actions as JSON for audit.

## Runtime contract

The container is launched by `cdp-ecs-webshell-lambda` and expects:

- `PORT` (default `8085`)
- `TOKEN` (all routes are served under `/$TOKEN`, because webshell-proxy forwards the path unchanged)
- `SQS_QUEUES` (JSON array of queue mappings)
- `SHOW_MESSAGE_CONTENT` (`true` or `false`)
- `USER_ID`, `USER_NAME`, `SERVICE`, `ENVIRONMENT`
- `AUDIT_UPLOAD_URL` (optional; used by entrypoint upload step)

## Local run (zero-dependency stub)

```bash
uv sync
export TOKEN=demo
export PORT=8085
export SERVICE=demo-service
export ENVIRONMENT=dev
export USER_ID=local
export USER_NAME="Local User"
export SHOW_MESSAGE_CONTENT=true
export SQS_STUB_SAMPLE_COUNT=3
export SQS_QUEUES='[{"name":"orders","arn":"arn:aws:sqs:eu-west-2:000000000000:orders","deadletter_queue_arn":"arn:aws:sqs:eu-west-2:000000000000:orders-deadletter"}]'
uv run uvicorn dev.main:app --host 0.0.0.0 --port "$PORT"
# open http://localhost:8085/demo/
```

`dev.main:app` is the same app with an in-memory fake SQS client swapped in, so no AWS credentials,
Floci, or LocalStack are needed. `dev/` is not copied into the Docker image.

## Local run (real API target, optional)

If you want to hit a real SQS-compatible endpoint (for example LocalStack), run `app.main:app` instead and set
`AWS_ENDPOINT_URL` or `AWS_ENDPOINT_URL_SQS`.

Run the tests with `uv run pytest`. Dependencies are pinned in `uv.lock`; after changing them in `pyproject.toml`,
run `uv lock` and commit the result.

## Styles

The UI uses cdp-portal-frontend's own Sass (GOV.UK Frontend settings, CDP colours, and the `entity-table`, `tag`,
`button`, `info`, `loader` and `page-heading` components), so it looks like the portal it is shown in.
`app/static/app.css` and `app/static/govuk/fonts` are generated and committed. `app/static/VERSION` records the
govuk-frontend version and portal commit they were built from.

Rebuild after the portal's styles change (needs a checkout of cdp-portal-frontend with `npm install` done):

```bash
PORTAL_DIR=../cdp-portal-frontend ./scripts/build-styles.sh
```

Tool-specific styles go in `styles/app.scss`.
