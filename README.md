# cdp-aws-tools

Browser UI for SQS operations launched through the CDP webshell flow.

## Features

- Lists service DLQs with queue depth and oldest-message age.
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

## Local run

```bash
uv venv
uv pip install -e '.[dev]'
export TOKEN=demo
export PORT=8085
export SQS_QUEUES='[]'
uv run uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
# open http://localhost:8085/demo/
```

## Styles

The UI uses cdp-portal-frontend's own Sass (GOV.UK Frontend settings, CDP colours, and the `entity-table`, `tag`,
`button`, `info` and `page-heading` components), so it looks like the portal it is shown in.
`app/static/app.css` and `app/static/govuk/fonts` are generated and committed. `app/static/VERSION` records the
govuk-frontend version and portal commit they were built from.

Rebuild after the portal's styles change (needs a checkout of cdp-portal-frontend with `npm install` done):

```bash
PORTAL_DIR=../cdp-portal-frontend ./scripts/build-styles.sh
```

Tool-specific styles go in `styles/app.scss`.
